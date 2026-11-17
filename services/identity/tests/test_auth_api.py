"""API tests for `/auth/*` and `/users` over `httpx.ASGITransport`.

Real app, real routing, real Pydantic validation, real service layer — only the
database and the repository SQL are faked. Assertions therefore cover the HTTP
contract: status codes, headers, error bodies, and the absence of leaked
internals.
"""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient

from app.core import security
from app.models.user import User, UserRole
from tests.fakes import TEST_PASSWORD, FakeAsyncSession, create_user

REGISTER = "/api/v1/auth/register"
LOGIN = "/api/v1/auth/login"
REFRESH = "/api/v1/auth/refresh"
LOGOUT = "/api/v1/auth/logout"
ME = "/api/v1/auth/me"
USERS = "/api/v1/users"


def _bearer(user: User) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {security.create_access_token(user.id, user.role)}",
    }


async def _issue_tokens(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
    *,
    email: str = "api@example.com",
) -> dict[str, str]:
    """Create a user and log in, returning the token pair."""
    await create_user(fake_session, email=email)
    response = await api_client.post(
        LOGIN,
        json={"email": email, "password": TEST_PASSWORD},
    )
    assert response.status_code == 200
    return response.json()


# ---------------------------------------------------------------------------
# POST /auth/register
# ---------------------------------------------------------------------------


async def test_register_returns_created_user(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    response = await api_client.post(
        REGISTER,
        json={"email": "New.User@Example.com", "password": "s3cret-pass"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["email"] == "new.user@example.com"
    assert body["role"] == "user"
    assert body["is_active"] is True
    assert uuid.UUID(body["id"])
    assert "password_hash" not in body
    assert len(fake_session.users) == 1
    assert fake_session.users[0].password_hash != "s3cret-pass"


async def test_register_duplicate_email_conflict(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    await create_user(fake_session, email="taken@example.com")

    response = await api_client.post(
        REGISTER,
        json={"email": "Taken@Example.com", "password": "s3cret-pass"},
    )

    assert response.status_code == 409
    assert response.json() == {"detail": "Email already registered"}
    assert len(fake_session.users) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {"email": "short@example.com", "password": "short"},  # below min length
        {"email": "long@example.com", "password": "x" * 129},  # above max length
        {"email": "not-an-email", "password": "s3cret-pass"},
        {"password": "s3cret-pass"},
    ],
)
async def test_register_rejects_invalid_payload(
    api_client: AsyncClient,
    payload: dict[str, str],
) -> None:
    response = await api_client.post(REGISTER, json=payload)

    assert response.status_code == 422


# ---------------------------------------------------------------------------
# POST /auth/login
# ---------------------------------------------------------------------------


async def test_login_returns_token_pair(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session, email="api@example.com")

    response = await api_client.post(
        LOGIN,
        json={"email": "API@Example.com", "password": TEST_PASSWORD},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["token_type"] == "bearer"
    assert security.decode_token(body["access_token"], "access")["sub"] == str(user.id)
    assert security.decode_token(body["refresh_token"], "refresh")["sub"] == str(
        user.id
    )
    assert fake_session.login_attempts[-1].success is True


async def test_login_failures_are_indistinguishable(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    await create_user(fake_session, email="known@example.com")

    wrong_password = await api_client.post(
        LOGIN,
        json={"email": "known@example.com", "password": "wrong-password"},
    )
    unknown_email = await api_client.post(
        LOGIN,
        json={"email": "ghost@example.com", "password": TEST_PASSWORD},
    )

    assert wrong_password.status_code == 401
    assert unknown_email.status_code == 401
    assert wrong_password.json() == unknown_email.json()
    assert wrong_password.json() == {"detail": "Invalid email or password"}
    assert wrong_password.headers["www-authenticate"] == "Bearer"

    # Both attempts are audited, and neither issued a token.
    assert [attempt.success for attempt in fake_session.login_attempts] == [
        False,
        False,
    ]
    assert fake_session.refresh_tokens == []


async def test_login_short_password_is_401_not_422(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    """A legacy short password must read as bad credentials, not bad format."""
    await create_user(fake_session, email="known@example.com")

    response = await api_client.post(
        LOGIN,
        json={"email": "known@example.com", "password": "short"},
    )

    assert response.status_code == 401


async def test_login_inactive_user_forbidden(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    await create_user(fake_session, email="blocked@example.com", is_active=False)

    response = await api_client.post(
        LOGIN,
        json={"email": "blocked@example.com", "password": TEST_PASSWORD},
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "User is inactive"}


# ---------------------------------------------------------------------------
# POST /auth/refresh
# ---------------------------------------------------------------------------


async def test_refresh_rotates_and_detects_reuse(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    tokens = await _issue_tokens(api_client, fake_session)

    rotated = await api_client.post(
        REFRESH,
        json={"refresh_token": tokens["refresh_token"]},
    )
    assert rotated.status_code == 200
    new_pair = rotated.json()
    assert new_pair["refresh_token"] != tokens["refresh_token"]

    reuse = await api_client.post(
        REFRESH,
        json={"refresh_token": tokens["refresh_token"]},
    )
    assert reuse.status_code == 401
    assert reuse.json() == {"detail": "Refresh token reuse detected, session revoked"}

    # The successor died together with the chain.
    after_reuse = await api_client.post(
        REFRESH,
        json={"refresh_token": new_pair["refresh_token"]},
    )
    assert after_reuse.status_code == 401
    assert after_reuse.json() == {"detail": "Refresh token is invalid or expired"}
    assert all(row.revoked_at is not None for row in fake_session.refresh_tokens)


@pytest.mark.parametrize(
    "refresh_token",
    ["not.a.jwt", "eyJhbGciOiJSUzI1NiJ9.e30.signature"],
)
async def test_refresh_rejects_unusable_tokens(
    api_client: AsyncClient,
    refresh_token: str,
) -> None:
    response = await api_client.post(REFRESH, json={"refresh_token": refresh_token})

    assert response.status_code == 401
    assert response.json() == {"detail": "Refresh token is invalid or expired"}


async def test_refresh_rejects_access_token(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session)
    access = security.create_access_token(user.id, user.role)

    response = await api_client.post(REFRESH, json={"refresh_token": access})

    assert response.status_code == 401


async def test_refresh_of_inactive_user_forbidden(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    tokens = await _issue_tokens(api_client, fake_session)
    fake_session.users[0].is_active = False

    response = await api_client.post(
        REFRESH,
        json={"refresh_token": tokens["refresh_token"]},
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "User is inactive"}
    # Rejected before rotation: the token is still usable if the account returns.
    assert fake_session.refresh_tokens[0].revoked_at is None


async def test_refresh_requires_token_field(api_client: AsyncClient) -> None:
    response = await api_client.post(REFRESH, json={})

    assert response.status_code == 422


# ---------------------------------------------------------------------------
# POST /auth/logout
# ---------------------------------------------------------------------------


async def test_logout_returns_204_and_is_idempotent(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    tokens = await _issue_tokens(api_client, fake_session)

    first = await api_client.post(
        LOGOUT,
        json={"refresh_token": tokens["refresh_token"]},
    )
    assert first.status_code == 204
    assert first.content == b""
    assert fake_session.refresh_tokens[0].revoked_at is not None

    second = await api_client.post(
        LOGOUT,
        json={"refresh_token": tokens["refresh_token"]},
    )
    assert second.status_code == 204


async def test_logout_with_unusable_token_is_still_204(
    api_client: AsyncClient,
) -> None:
    response = await api_client.post(LOGOUT, json={"refresh_token": "not.a.jwt"})

    assert response.status_code == 204


# ---------------------------------------------------------------------------
# GET /auth/me
# ---------------------------------------------------------------------------


async def test_me_without_token(api_client: AsyncClient) -> None:
    response = await api_client.get(ME)

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.json() == {"detail": "Not authenticated"}


async def test_me_with_garbage_token(api_client: AsyncClient) -> None:
    response = await api_client.get(ME, headers={"Authorization": "Bearer not.a.jwt"})

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or expired access token"}


async def test_me_returns_current_user(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session, email="me@example.com", role=UserRole.ADMIN)

    response = await api_client.get(ME, headers=_bearer(user))

    assert response.status_code == 200
    body = response.json()
    assert body["email"] == "me@example.com"
    assert body["role"] == "admin"
    assert "password_hash" not in body


async def test_me_rejects_inactive_user(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session, is_active=False)

    response = await api_client.get(ME, headers=_bearer(user))

    assert response.status_code == 403
    assert response.json() == {"detail": "User is inactive"}


# ---------------------------------------------------------------------------
# GET /users
# ---------------------------------------------------------------------------


async def test_users_requires_authentication(api_client: AsyncClient) -> None:
    response = await api_client.get(USERS)

    assert response.status_code == 401


async def test_users_forbidden_for_regular_user(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    user = await create_user(fake_session, role=UserRole.USER)

    response = await api_client.get(USERS, headers=_bearer(user))

    assert response.status_code == 403
    assert response.json() == {"detail": "Admin access required"}


async def test_users_lists_users_for_admin(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    admin = await create_user(
        fake_session,
        email="admin@example.com",
        role=UserRole.ADMIN,
    )
    await create_user(fake_session, email="member@example.com")

    response = await api_client.get(USERS, headers=_bearer(admin))

    assert response.status_code == 200
    emails = {row["email"] for row in response.json()}
    assert emails == {"admin@example.com", "member@example.com"}
    assert all("password_hash" not in row for row in response.json())


async def test_users_pagination_is_bounded(
    api_client: AsyncClient,
    fake_session: FakeAsyncSession,
) -> None:
    admin = await create_user(fake_session, role=UserRole.ADMIN)
    await create_user(fake_session, email="member@example.com")

    page = await api_client.get(f"{USERS}?limit=1&offset=1", headers=_bearer(admin))
    assert page.status_code == 200
    assert len(page.json()) == 1

    for query in ("limit=0", "limit=201", "offset=-1"):
        response = await api_client.get(f"{USERS}?{query}", headers=_bearer(admin))
        assert response.status_code == 422, query

    assert "detail" in response.json()
