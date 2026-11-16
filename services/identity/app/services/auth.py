"""Authentication business logic: register, login, refresh rotation, logout.

Transaction policy
------------------
Every mutating method owns exactly one transaction (`async with
session.begin()`). Repositories only `flush()`, never `commit()`.

Security writes (login audit rows, refresh-chain revocation) must survive the
error response that follows them. That is why the corresponding domain
exception is raised **after** the transaction block exits: raising inside the
block would roll the audit row back.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core import security
from app.core.config import settings
from app.core.security import InvalidTokenError
from app.models.login_attempt import LoginAttempt
from app.models.refresh_token import RefreshToken
from app.models.user import User
from app.repositories.login_attempt import LoginAttemptRepository
from app.repositories.refresh_token import RefreshTokenRepository
from app.repositories.user import UserRepository
from app.services.exceptions import (
    InvalidAccessTokenError,
    InvalidCredentialsError,
    RefreshTokenReusedError,
    RefreshTokenRevokedError,
    UserAlreadyExistsError,
    UserInactiveError,
)


@dataclass(frozen=True, slots=True)
class IssuedTokens:
    """A freshly minted access/refresh pair."""

    access_token: str
    refresh_token: str


def normalize_email(email: str) -> str:
    """Canonical form of an email address.

    Emails are unique per address but case-insensitive in practice, so the DB
    stores and compares only the normalized value. Doing this in the service
    (not in the endpoint) keeps a single source of truth.
    """
    return email.strip().lower()


@lru_cache(maxsize=1)
def _dummy_password_hash() -> str:
    """Hash of a throwaway password, used to equalize login timing.

    Without it, an unknown email returns in ~1 ms while a known one burns ~100 ms
    in Argon2, which is a reliable account-enumeration oracle.
    """
    return security.hash_password(uuid.uuid4().hex)


class AuthService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.user_repo = UserRepository(session)
        self.refresh_repo = RefreshTokenRepository(session)
        self.login_attempt_repo = LoginAttemptRepository(session)

    async def register(self, email: str, password: str) -> User:
        """Create a user. Returns the ORM object; no tokens are issued here."""
        email = normalize_email(email)
        # Hash outside the transaction: Argon2id is ~100 ms of CPU, and a DB
        # transaction must not stay open for that.
        password_hash = security.hash_password(password)

        try:
            async with self.session.begin():
                if await self.user_repo.get_by_email(email) is not None:
                    raise UserAlreadyExistsError(email)

                user = User(email=email, password_hash=password_hash)
                await self.user_repo.create(user)
        except IntegrityError as exc:
            # Two concurrent registrations can both pass the SELECT; the unique
            # index is the real arbiter, so map its violation to the same error.
            raise UserAlreadyExistsError(email) from exc

        return user

    async def login(
        self,
        email: str,
        password: str,
        ip: str | None = None,
    ) -> IssuedTokens:
        """Verify credentials, record the attempt, and issue a token pair."""
        email = normalize_email(email)

        async with self.session.begin():
            user = await self.user_repo.get_by_email(email)

            if user is None:
                # Same CPU cost as a real check, so timing does not leak
                # whether the email exists.
                security.verify_password(password, _dummy_password_hash())
                password_ok = False
            else:
                password_ok = security.verify_password(password, user.password_hash)

            await self.login_attempt_repo.record(
                LoginAttempt(
                    user_id=user.id if user is not None else None,
                    email=email,
                    success=password_ok,
                    ip=ip,
                )
            )

            issued: IssuedTokens | None = None
            if password_ok and user is not None and user.is_active:
                issued = await self._issue_tokens(user)

        # Raised after commit so the audit row is persisted even for failures.
        if user is None or not password_ok:
            raise InvalidCredentialsError(email)
        if not user.is_active:
            raise UserInactiveError(str(user.id))
        if issued is None:  # defensive: unreachable given the checks above
            raise InvalidCredentialsError(email)
        return issued

    async def _issue_tokens(
        self,
        user: User,
        *,
        jti: uuid.UUID | None = None,
    ) -> IssuedTokens:
        """Mint a token pair and persist the refresh-token row.

        The `jti` is generated here and written to the DB *before* the token is
        returned, so a token that reached a client always has a matching row.
        """
        token_jti = jti or uuid.uuid4()
        access_token = security.create_access_token(user.id, user.role)
        refresh_token = security.create_refresh_token(user.id, token_jti)

        await self.refresh_repo.create(
            RefreshToken(
                jti=token_jti,
                user_id=user.id,
                expires_at=datetime.now(UTC)
                + timedelta(seconds=settings.refresh_token_ttl_seconds),
            )
        )
        return IssuedTokens(
            access_token=access_token,
            refresh_token=refresh_token,
        )

    async def refresh(self, refresh_token: str) -> IssuedTokens:
        """Rotate a refresh token.

        Success: the presented token is revoked with `replaced_by_jti` pointing
        at the new one, and a brand-new pair is returned.

        Reuse: a token that was already rotated shows up again. We cannot tell a
        retrying client from a thief, so the whole chain is revoked and the call
        fails — the legitimate user just has to log in again.
        """
        jti, _user_id = self._refresh_claims(refresh_token)

        reused = False
        issued: IssuedTokens | None = None

        async with self.session.begin():
            # Row lock: two parallel refreshes with the same token must not both
            # rotate it. The second one waits here and then sees `revoked_at`.
            stored = await self.refresh_repo.get_by_jti_for_update(jti)

            if stored is None:
                raise RefreshTokenRevokedError("unknown refresh token")

            if stored.revoked_at is not None:
                if stored.replaced_by_jti is not None:
                    await self.refresh_repo.revoke_all_for_user(stored.user_id)
                    reused = True
                else:
                    raise RefreshTokenRevokedError("refresh token revoked")
            elif stored.expires_at <= datetime.now(UTC):
                raise RefreshTokenRevokedError("refresh token expired")
            else:
                user = await self.user_repo.get_by_id(stored.user_id)
                if user is None:
                    raise RefreshTokenRevokedError("user no longer exists")
                if not user.is_active:
                    raise UserInactiveError(str(user.id))

                new_jti = uuid.uuid4()
                await self.refresh_repo.revoke(jti, replaced_by_jti=new_jti)
                issued = await self._issue_tokens(user, jti=new_jti)

        if reused:
            # Committed on purpose: revoking the chain must outlive the 401.
            raise RefreshTokenReusedError(str(jti))
        if issued is None:  # defensive: unreachable given the branches above
            raise RefreshTokenRevokedError("refresh token rejected")
        return issued

    async def logout(self, refresh_token: str) -> None:
        """Revoke exactly the presented refresh token.

        Idempotent by design: an unusable token means the session is already
        over, so the caller still gets 204 and no information about validity.
        """
        try:
            jti, _user_id = self._refresh_claims(refresh_token)
        except RefreshTokenRevokedError:
            return

        async with self.session.begin():
            await self.refresh_repo.revoke(jti)

    async def get_current_user(self, access_token: str) -> User:
        """Resolve the user behind an access token.

        The role is re-read from the DB rather than trusted from the claim, so a
        demoted or deactivated account loses access within one token lifetime.
        """
        try:
            payload = security.decode_token(access_token, "access")
            user_id = uuid.UUID(payload["sub"])
        except (InvalidTokenError, KeyError, ValueError) as exc:
            raise InvalidAccessTokenError("invalid access token") from exc

        user = await self.user_repo.get_by_id(user_id)
        if user is None:
            raise InvalidAccessTokenError("user no longer exists")
        if not user.is_active:
            raise UserInactiveError(str(user.id))
        return user

    @staticmethod
    def _refresh_claims(refresh_token: str) -> tuple[uuid.UUID, uuid.UUID]:
        """Decode a refresh token into `(jti, sub)`.

        Signature, `exp` and `type` are already validated by `decode_token`;
        anything unparsable is reported as "revoked" so the API returns one
        indistinguishable 401.
        """
        try:
            payload = security.decode_token(refresh_token, "refresh")
            return uuid.UUID(payload["jti"]), uuid.UUID(payload["sub"])
        except (InvalidTokenError, KeyError, ValueError) as exc:
            raise RefreshTokenRevokedError("invalid refresh token") from exc
