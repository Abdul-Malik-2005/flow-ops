"""In-memory fakes for the Identity service tests.

`AuthService` is only meaningfully testable if two things are real:

* transaction boundaries — the service raises *after* the `async with
  session.begin()` block on purpose, because a login audit row or a revoked
  refresh chain must survive the error response that follows it;
* "flush applies column defaults" — a freshly constructed `User` has no `id`,
  `role` or `created_at` until an INSERT happens.

`AsyncMock` would hide both, so this module implements a miniature in-memory
database instead:

* `FakeAsyncSession` — `begin()` snapshots the rows, then commits or rolls back;
  `add()` + `flush()` apply the defaults an INSERT would apply.
* `Fake*Repository` — the same interface as the real repositories, no SQL.
* `install_fakes()` — swaps the repository classes inside the *service* modules,
  so services constructed by the API layer use the fakes. Production code is
  not modified.

Only the calls the services actually make are implemented: anything else raises
`NotImplementedError` instead of silently returning `None`.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self

import pytest
from sqlalchemy.exc import IntegrityError

from app.core import security
from app.models.login_attempt import LoginAttempt
from app.models.refresh_token import RefreshToken
from app.models.user import User, UserRole
from app.services import auth as auth_service_module
from app.services import user as user_service_module

# One shared password for every fixture user: Argon2id costs ~100 ms per hash,
# so the hash is memoized per password (see `password_hash_for`).
TEST_PASSWORD = "correct-horse-battery-staple"

_password_hashes: dict[str, str] = {}


def utcnow() -> datetime:
    return datetime.now(UTC)


def password_hash_for(password: str) -> str:
    """Argon2id hash of `password`, memoized for the duration of the run."""
    if password not in _password_hashes:
        _password_hashes[password] = security.hash_password(password)
    return _password_hashes[password]


async def create_user(
    session: FakeAsyncSession,
    *,
    email: str = "user@example.com",
    password: str = TEST_PASSWORD,
    role: UserRole = UserRole.USER,
    is_active: bool = True,
) -> User:
    """Insert a user the way a real registration would (via `add` + `flush`)."""
    user = User(
        email=email.lower(),
        password_hash=password_hash_for(password),
        role=role,
        is_active=is_active,
    )
    session.add(user)
    await session.flush()
    return user


def _capture(rows: Sequence[Any]) -> list[tuple[Any, dict[str, Any]]]:
    """Snapshot each row's mapped attributes (a shallow copy is enough)."""
    return [(row, dict(row.__dict__)) for row in rows]


def _restore(rows: list[tuple[Any, dict[str, Any]]]) -> list[Any]:
    """Put the snapshot values back into the original row objects."""
    for row, attributes in rows:
        row.__dict__.clear()
        row.__dict__.update(attributes)
    return [row for row, _ in rows]


class _Snapshot:
    """State captured when a transaction starts, restored on rollback."""

    __slots__ = ("login_attempts", "pending", "refresh_tokens", "users")

    def __init__(self, session: FakeAsyncSession) -> None:
        self.users = _capture(session.users)
        self.refresh_tokens = _capture(session.refresh_tokens)
        self.login_attempts = _capture(session.login_attempts)
        self.pending = list(session.pending)


class FakeTransaction:
    """Mirrors `AsyncSessionTransaction`: commit on clean exit, rollback on error."""

    def __init__(self, session: FakeAsyncSession) -> None:
        self.session = session
        self.snapshot: _Snapshot | None = None

    async def __aenter__(self) -> Self:
        self.session.begun += 1
        self.snapshot = _Snapshot(self.session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if exc_type is None:
            self.session.committed += 1
        else:
            self.session.rolled_back += 1
            assert self.snapshot is not None
            self.session.rollback_to(self.snapshot)
        self.snapshot = None
        # Never swallow the exception: the service relies on it propagating.
        return False


class FakeAsyncSession:
    """In-memory stand-in for `AsyncSession`, supporting what the services use."""

    def __init__(self) -> None:
        self.users: list[User] = []
        self.refresh_tokens: list[RefreshToken] = []
        self.login_attempts: list[LoginAttempt] = []
        self.pending: list[Any] = []
        self.begun = 0
        self.committed = 0
        self.rolled_back = 0

    # --- transactions -----------------------------------------------------

    def begin(self) -> FakeTransaction:
        """Same call shape as `AsyncSession.begin()`: an async context manager."""
        return FakeTransaction(self)

    def rollback_to(self, snapshot: _Snapshot) -> None:
        self.users = _restore(snapshot.users)
        self.refresh_tokens = _restore(snapshot.refresh_tokens)
        self.login_attempts = _restore(snapshot.login_attempts)
        self.pending = list(snapshot.pending)

    # --- persistence ------------------------------------------------------

    def add(self, instance: Any) -> None:
        self.pending.append(instance)

    async def flush(self) -> None:
        for instance in self.pending:
            self._insert(instance)
        self.pending.clear()

    def _insert(self, instance: Any) -> None:
        """Apply the column defaults a real INSERT would apply."""
        if isinstance(instance, User):
            instance.id = instance.id or uuid.uuid4()
            instance.role = instance.role or UserRole.USER
            instance.is_active = (
                True if instance.is_active is None else instance.is_active
            )
            instance.created_at = instance.created_at or utcnow()
            instance.updated_at = instance.updated_at or instance.created_at
            self.users.append(instance)
        elif isinstance(instance, RefreshToken):
            instance.created_at = instance.created_at or utcnow()
            self.refresh_tokens.append(instance)
        elif isinstance(instance, LoginAttempt):
            instance.id = instance.id or uuid.uuid4()
            instance.created_at = instance.created_at or utcnow()
            self.login_attempts.append(instance)
        else:
            raise NotImplementedError(
                f"FakeAsyncSession cannot persist {type(instance).__name__}"
            )

    # --- helpers for assertions -------------------------------------------

    def refresh_tokens_by_jti(self) -> dict[uuid.UUID, RefreshToken]:
        return {row.jti: row for row in self.refresh_tokens}


class FakeUserRepository:
    """`UserRepository` against the in-memory store."""

    def __init__(self, session: FakeAsyncSession) -> None:
        self.session = session

    async def get_by_email(self, email: str) -> User | None:
        return next((user for user in self.session.users if user.email == email), None)

    async def get_by_id(self, user_id: uuid.UUID) -> User | None:
        return next((user for user in self.session.users if user.id == user_id), None)

    async def create(self, user: User) -> User:
        self.session.add(user)
        await self.session.flush()
        return user

    async def list_all(self, *, limit: int = 50, offset: int = 0) -> list[User]:
        ordered = sorted(
            self.session.users,
            key=lambda user: (user.created_at, user.id),
        )
        return ordered[offset : offset + limit]


class DuplicateOnInsertUserRepository(FakeUserRepository):
    """Loses the "email is free" race: the INSERT fails on the unique index."""

    async def create(self, user: User) -> User:
        self.session.add(user)
        raise IntegrityError(
            "INSERT INTO users (email, password_hash) VALUES ($1, $2)",
            {"email": user.email},
            Exception("duplicate key value violates unique constraint"),
        )


class FakeRefreshTokenRepository:
    """`RefreshTokenRepository` against the in-memory store."""

    def __init__(self, session: FakeAsyncSession) -> None:
        self.session = session

    async def get_by_jti(self, jti: uuid.UUID) -> RefreshToken | None:
        return self._find(jti)

    async def get_by_jti_for_update(self, jti: uuid.UUID) -> RefreshToken | None:
        # The real query takes a row lock; single-threaded tests need no locking.
        return self._find(jti)

    async def create(self, refresh_token: RefreshToken) -> RefreshToken:
        self.session.add(refresh_token)
        await self.session.flush()
        return refresh_token

    async def revoke(
        self,
        jti: uuid.UUID,
        replaced_by_jti: uuid.UUID | None = None,
    ) -> None:
        row = self._find(jti)
        if row is not None and row.revoked_at is None:
            row.revoked_at = utcnow()
            row.replaced_by_jti = replaced_by_jti

    async def revoke_all_for_user(self, user_id: uuid.UUID) -> None:
        for row in self.session.refresh_tokens:
            if row.user_id == user_id and row.revoked_at is None:
                row.revoked_at = utcnow()

    def _find(self, jti: uuid.UUID) -> RefreshToken | None:
        return next(
            (row for row in self.session.refresh_tokens if row.jti == jti),
            None,
        )


class FakeLoginAttemptRepository:
    """`LoginAttemptRepository` against the in-memory store (write-only)."""

    def __init__(self, session: FakeAsyncSession) -> None:
        self.session = session

    async def record(self, attempt: LoginAttempt) -> None:
        self.session.add(attempt)
        await self.session.flush()


def install_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    user_repository: type[FakeUserRepository] = FakeUserRepository,
) -> None:
    """Point the service modules' repositories at the fakes.

    The service layer imports the repository classes into its own namespace and
    calls `UserRepository(session)` in `__init__`, so patching those module
    globals covers both `AuthService` and `UserService`: no production change
    and no dependency-injection refactor.
    """
    monkeypatch.setattr(auth_service_module, "UserRepository", user_repository)
    monkeypatch.setattr(
        auth_service_module,
        "RefreshTokenRepository",
        FakeRefreshTokenRepository,
    )
    monkeypatch.setattr(
        auth_service_module,
        "LoginAttemptRepository",
        FakeLoginAttemptRepository,
    )
    monkeypatch.setattr(user_service_module, "UserRepository", user_repository)
