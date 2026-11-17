"""Unit tests for `app.services.auth`, running on the in-memory fakes.

The service owns every business rule of the auth flow, so these tests assert
behaviour rather than SQL: which rows a call leaves behind, which transaction it
committed, and which error it raises *after* the commit.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest

from app.core import security
from app.models.refresh_token import RefreshToken
from app.models.user import UserRole
from app.services.auth import AuthService, normalize_email
from app.services.exceptions import (
    InvalidAccessTokenError,
    InvalidCredentialsError,
    RefreshTokenReusedError,
    RefreshTokenRevokedError,
    UserAlreadyExistsError,
    UserInactiveError,
)
from tests import fakes
from tests.fakes import TEST_PASSWORD, FakeAsyncSession, create_user, utcnow


def _refresh_jti(token: str) -> uuid.UUID:
    """`jti` claim of a refresh token, as a UUID."""
    return uuid.UUID(security.decode_token(token, "refresh")["jti"])


# ---------------------------------------------------------------------------
# normalize_email
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("User@Example.COM", "user@example.com"),
        ("  user@example.com  ", "user@example.com"),
        ("already-normalized@example.com", "already-normalized@example.com"),
    ],
)
def test_normalize_email(raw: str, expected: str) -> None:
    assert normalize_email(raw) == expected


# ---------------------------------------------------------------------------
# register
# ---------------------------------------------------------------------------


async def test_register_creates_normalized_user(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await auth_service.register("  Mixed.Case@Example.COM ", "s3cret-pass")

    assert user.email == "mixed.case@example.com"
    assert user.id is not None  # assigned by flush, like a real INSERT
    assert user.role is UserRole.USER
    assert user.is_active is True
    assert user.created_at is not None
    assert security.verify_password("s3cret-pass", user.password_hash)
    assert fake_session.users == [user]
    assert fake_session.committed == 1
    assert fake_session.rolled_back == 0


async def test_register_rejects_existing_email_case_insensitively(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    await create_user(fake_session, email="taken@example.com")

    with pytest.raises(UserAlreadyExistsError):
        await auth_service.register("TAKEN@example.com", "s3cret-pass")

    assert len(fake_session.users) == 1
    assert fake_session.rolled_back == 1


async def test_register_maps_integrity_error_to_conflict(
    fake_session: FakeAsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    jwt_keys: None,
) -> None:
    """Two concurrent registrations both pass the SELECT; the index decides."""
    fakes.install_fakes(
        monkeypatch,
        user_repository=fakes.DuplicateOnInsertUserRepository,
    )
    service = AuthService(fake_session)

    with pytest.raises(UserAlreadyExistsError):
        await service.register("race@example.com", "s3cret-pass")

    assert fake_session.users == []
    assert fake_session.rolled_back == 1


# ---------------------------------------------------------------------------
# login
# ---------------------------------------------------------------------------


async def test_login_issues_token_pair_and_audits_success(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session, email="login@example.com")

    tokens = await auth_service.login(
        " Login@Example.com ",
        TEST_PASSWORD,
        ip="10.0.0.1",
    )

    access = security.decode_token(tokens.access_token, "access")
    refresh = security.decode_token(tokens.refresh_token, "refresh")

    assert access["sub"] == str(user.id)
    assert access["role"] == UserRole.USER.value
    assert refresh["sub"] == str(user.id)
    assert "role" not in refresh

    rows = fake_session.refresh_tokens_by_jti()
    assert set(rows) == {uuid.UUID(refresh["jti"])}
    assert rows[uuid.UUID(refresh["jti"])].user_id == user.id
    assert rows[uuid.UUID(refresh["jti"])].revoked_at is None

    attempt = fake_session.login_attempts[-1]
    assert attempt.email == "login@example.com"
    assert attempt.success is True
    assert attempt.ip == "10.0.0.1"
    assert attempt.user_id == user.id


async def test_login_wrong_password_is_rejected_but_audited(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session, email="login@example.com")

    with pytest.raises(InvalidCredentialsError):
        await auth_service.login(user.email, "not-the-password")

    assert fake_session.refresh_tokens == []
    assert len(fake_session.login_attempts) == 1

    attempt = fake_session.login_attempts[0]
    assert attempt.user_id == user.id
    assert attempt.success is False
    # The audit row is committed before the 401 is raised.
    assert fake_session.committed == 1
    assert fake_session.rolled_back == 0


async def test_login_unknown_email_verifies_dummy_hash(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unknown email must cost the same Argon2 work as a known one."""
    calls: list[tuple[str, str]] = []
    real_verify = security.verify_password

    def spy(password: str, password_hash: str) -> bool:
        calls.append((password, password_hash))
        return real_verify(password, password_hash)

    monkeypatch.setattr(security, "verify_password", spy)

    with pytest.raises(InvalidCredentialsError):
        await auth_service.login("ghost@example.com", TEST_PASSWORD)

    assert len(calls) == 1
    assert calls[0][0] == TEST_PASSWORD
    assert calls[0][1].startswith("$argon2id$")  # a real hash was burned

    attempt = fake_session.login_attempts[-1]
    assert attempt.user_id is None
    assert attempt.success is False


async def test_login_inactive_user_forbidden_but_audited(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(
        fake_session,
        email="blocked@example.com",
        is_active=False,
    )

    with pytest.raises(UserInactiveError):
        await auth_service.login(user.email, TEST_PASSWORD)

    assert fake_session.refresh_tokens == []
    assert fake_session.login_attempts[-1].success is True


# ---------------------------------------------------------------------------
# refresh
# ---------------------------------------------------------------------------


async def test_refresh_rotates_token_and_links_replacement(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session)
    tokens = await auth_service.login(user.email, TEST_PASSWORD)
    old_jti = _refresh_jti(tokens.refresh_token)

    rotated = await auth_service.refresh(tokens.refresh_token)

    new_jti = _refresh_jti(rotated.refresh_token)
    rows = fake_session.refresh_tokens_by_jti()

    assert rows[old_jti].revoked_at is not None
    assert rows[old_jti].replaced_by_jti == new_jti
    assert rows[new_jti].revoked_at is None
    assert rotated.access_token != tokens.access_token
    assert rotated.refresh_token != tokens.refresh_token


async def test_refresh_rejects_unknown_token(
    auth_service: AuthService,
    user_id: uuid.UUID,
) -> None:
    """A well-signed token with no matching row was never issued by us."""
    token = security.create_refresh_token(user_id, uuid.uuid4())

    with pytest.raises(RefreshTokenRevokedError):
        await auth_service.refresh(token)


async def test_refresh_rejects_token_revoked_by_logout(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session)
    tokens = await auth_service.login(user.email, TEST_PASSWORD)
    await auth_service.logout(tokens.refresh_token)

    with pytest.raises(RefreshTokenRevokedError):
        await auth_service.refresh(tokens.refresh_token)


async def test_refresh_rejects_expired_row(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    """The JWT itself is still valid; only the DB row expired."""
    user = await create_user(fake_session)
    jti = uuid.uuid4()
    fake_session.add(
        RefreshToken(
            jti=jti,
            user_id=user.id,
            expires_at=utcnow() - timedelta(minutes=1),
        )
    )
    await fake_session.flush()

    with pytest.raises(RefreshTokenRevokedError):
        await auth_service.refresh(security.create_refresh_token(user.id, jti))

    assert fake_session.refresh_tokens[0].revoked_at is None


async def test_refresh_reuse_revokes_whole_chain(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session)
    first = await auth_service.login(user.email, TEST_PASSWORD)
    second = await auth_service.refresh(first.refresh_token)

    with pytest.raises(RefreshTokenReusedError):
        await auth_service.refresh(first.refresh_token)

    rows = fake_session.refresh_tokens_by_jti()
    assert rows[_refresh_jti(first.refresh_token)].replaced_by_jti == (
        _refresh_jti(second.refresh_token)
    )
    # Both the rotated token and its successor are dead.
    assert all(row.revoked_at is not None for row in rows.values())
    # login + rotation + chain revocation, each committed on purpose.
    assert fake_session.committed == 3
    assert fake_session.rolled_back == 0

    with pytest.raises(RefreshTokenRevokedError):
        await auth_service.refresh(second.refresh_token)


async def test_refresh_of_inactive_user_rolls_back(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session)
    tokens = await auth_service.login(user.email, TEST_PASSWORD)
    user.is_active = False

    with pytest.raises(UserInactiveError):
        await auth_service.refresh(tokens.refresh_token)

    # Nothing was written: reactivating the account keeps the session alive.
    assert fake_session.refresh_tokens[0].revoked_at is None
    assert fake_session.rolled_back == 1


async def test_refresh_rejects_token_of_deleted_user(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session)
    tokens = await auth_service.login(user.email, TEST_PASSWORD)
    fake_session.users.clear()

    with pytest.raises(RefreshTokenRevokedError):
        await auth_service.refresh(tokens.refresh_token)


async def test_refresh_rejects_garbage_and_access_tokens(
    auth_service: AuthService,
    user_id: uuid.UUID,
) -> None:
    with pytest.raises(RefreshTokenRevokedError):
        await auth_service.refresh("not.a.jwt")

    access = security.create_access_token(user_id, UserRole.USER)
    with pytest.raises(RefreshTokenRevokedError):
        await auth_service.refresh(access)


# ---------------------------------------------------------------------------
# logout
# ---------------------------------------------------------------------------


async def test_logout_revokes_only_presented_token(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session)
    first = await auth_service.login(user.email, TEST_PASSWORD)
    second = await auth_service.login(user.email, TEST_PASSWORD)

    await auth_service.logout(first.refresh_token)

    rows = fake_session.refresh_tokens_by_jti()
    assert rows[_refresh_jti(first.refresh_token)].revoked_at is not None
    assert rows[_refresh_jti(second.refresh_token)].revoked_at is None


async def test_logout_is_idempotent(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session)
    tokens = await auth_service.login(user.email, TEST_PASSWORD)

    await auth_service.logout(tokens.refresh_token)
    revoked_at = fake_session.refresh_tokens[0].revoked_at
    assert revoked_at is not None

    await auth_service.logout(tokens.refresh_token)  # must not raise

    assert fake_session.refresh_tokens[0].revoked_at == revoked_at


async def test_logout_ignores_unusable_token(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    begun_before = fake_session.begun

    await auth_service.logout("not.a.jwt")

    # Nothing to revoke means no transaction at all.
    assert fake_session.begun == begun_before


# ---------------------------------------------------------------------------
# get_current_user
# ---------------------------------------------------------------------------


async def test_get_current_user_returns_db_row(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session, role=UserRole.ADMIN)
    token = security.create_access_token(user.id, user.role)

    assert await auth_service.get_current_user(token) is user


@pytest.mark.parametrize("token", ["", "not.a.jwt"])
async def test_get_current_user_rejects_invalid_token(
    auth_service: AuthService,
    token: str,
) -> None:
    with pytest.raises(InvalidAccessTokenError):
        await auth_service.get_current_user(token)


async def test_get_current_user_rejects_refresh_token(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session)
    tokens = await auth_service.login(user.email, TEST_PASSWORD)

    with pytest.raises(InvalidAccessTokenError):
        await auth_service.get_current_user(tokens.refresh_token)


async def test_get_current_user_after_deletion(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session)
    token = security.create_access_token(user.id, user.role)
    fake_session.users.clear()

    with pytest.raises(InvalidAccessTokenError):
        await auth_service.get_current_user(token)


async def test_get_current_user_rejects_inactive_user(
    auth_service: AuthService,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session, is_active=False)
    token = security.create_access_token(user.id, user.role)

    with pytest.raises(UserInactiveError):
        await auth_service.get_current_user(token)
