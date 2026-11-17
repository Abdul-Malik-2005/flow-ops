"""End-to-end auth flows against a real PostgreSQL database.

These tests exist to catch what in-memory fakes cannot:

* SQL that does not match the models (types, enum values, timestamptz);
* transaction boundaries that behave differently on a real driver;
* PostgreSQL constraints such as the unique email index;
* the service reading role/status from the database, not from the JWT claim.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.login_attempt import LoginAttempt
from app.models.refresh_token import RefreshToken
from app.models.user import User, UserRole
from tests.integration.helpers import (
    LOGOUT,
    ME,
    REFRESH,
    REGISTER,
    TEST_PASSWORD,
    USERS,
    bearer,
    create_user,
    login,
    refresh_jti,
)

pytestmark = pytest.mark.integration


async def test_register_persists_normalized_user(
    api_client,
    db_session: AsyncSession,
) -> None:
    response = await api_client.post(
        REGISTER,
        json={"email": "User@Example.COM", "password": TEST_PASSWORD},
    )

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["email"] == "user@example.com"
    assert body["role"] == "user"
    assert body["is_active"] is True

    user = await db_session.scalar(select(User).where(User.email == "user@example.com"))
    assert user is not None
    assert str(user.id) == body["id"]
    assert user.password_hash.startswith("$argon2id$")
    assert TEST_PASSWORD not in user.password_hash


async def test_login_persists_live_refresh_row_and_audit(
    api_client,
    db_session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory)

    tokens = await login(api_client, user.email)

    jti = refresh_jti(tokens["refresh_token"])
    stored = await db_session.get(RefreshToken, jti)
    assert stored is not None
    assert stored.user_id == user.id
    assert stored.revoked_at is None
    assert stored.expires_at > datetime.now(UTC)

    attempt = await db_session.scalar(
        select(LoginAttempt).where(LoginAttempt.user_id == user.id)
    )
    assert attempt is not None
    assert attempt.success is True
    assert attempt.email == user.email
    assert attempt.ip == "127.0.0.1"


async def test_me_reads_active_flag_from_database(
    api_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory)
    tokens = await login(api_client, user.email)
    headers = bearer(tokens["access_token"])

    assert (await api_client.get(ME, headers=headers)).status_code == 200

    async with session_factory() as session, session.begin():
        await session.execute(
            update(User).where(User.id == user.id).values(is_active=False)
        )

    # The access token is still cryptographically valid, but the account is not.
    response = await api_client.get(ME, headers=headers)
    assert response.status_code == 403


async def test_users_endpoint_uses_role_from_database(
    api_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory)
    tokens = await login(api_client, user.email)
    headers = bearer(tokens["access_token"])

    assert (await api_client.get(USERS, headers=headers)).status_code == 403

    async with session_factory() as session, session.begin():
        await session.execute(
            update(User).where(User.id == user.id).values(role=UserRole.ADMIN)
        )

    response = await api_client.get(USERS, headers=headers)
    assert response.status_code == 200
    assert [row["email"] for row in response.json()] == [user.email]


async def test_refresh_rotates_token_and_links_previous_one(
    api_client,
    db_session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory)
    tokens = await login(api_client, user.email)

    response = await api_client.post(
        REFRESH, json={"refresh_token": tokens["refresh_token"]}
    )

    assert response.status_code == 200, response.text
    rotated = response.json()
    old_jti = refresh_jti(tokens["refresh_token"])
    new_jti = refresh_jti(rotated["refresh_token"])

    previous = await db_session.get(RefreshToken, old_jti)
    assert previous is not None
    assert previous.revoked_at is not None
    assert previous.replaced_by_jti == new_jti

    fresh = await db_session.get(RefreshToken, new_jti)
    assert fresh is not None
    assert fresh.revoked_at is None

    me = await api_client.get(ME, headers=bearer(rotated["access_token"]))
    assert me.status_code == 200


async def test_refresh_reuse_revokes_the_whole_chain(
    api_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory)
    tokens = await login(api_client, user.email)
    rotated = await api_client.post(
        REFRESH, json={"refresh_token": tokens["refresh_token"]}
    )
    assert rotated.status_code == 200, rotated.text

    reused = await api_client.post(
        REFRESH, json={"refresh_token": tokens["refresh_token"]}
    )
    assert reused.status_code == 401
    assert "reuse" in reused.json()["detail"].lower()

    # The revocation is committed before the 401 is raised, so even the token
    # the attacker's victim legitimately holds is now dead.
    winner_jti = refresh_jti(rotated.json()["refresh_token"])
    async with session_factory() as session:
        winner = await session.get(RefreshToken, winner_jti)
        active = await session.scalars(
            select(RefreshToken.jti).where(
                RefreshToken.user_id == user.id,
                RefreshToken.revoked_at.is_(None),
            )
        )

    assert winner is not None
    assert winner.revoked_at is not None
    assert active.all() == []

    denied = await api_client.post(
        REFRESH, json={"refresh_token": rotated.json()["refresh_token"]}
    )
    assert denied.status_code == 401


async def test_logout_is_idempotent(
    api_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory)
    tokens = await login(api_client, user.email)
    jti = refresh_jti(tokens["refresh_token"])

    first = await api_client.post(
        LOGOUT, json={"refresh_token": tokens["refresh_token"]}
    )
    assert first.status_code == 204

    async with session_factory() as session:
        revoked_at = (await session.get(RefreshToken, jti)).revoked_at
    assert revoked_at is not None

    second = await api_client.post(
        LOGOUT, json={"refresh_token": tokens["refresh_token"]}
    )
    assert second.status_code == 204

    async with session_factory() as session:
        stored = await session.get(RefreshToken, jti)
    # `revoke()` keeps the first timestamp: replaying logout reveals nothing.
    assert stored.revoked_at == revoked_at

    rejected = await api_client.post(
        REFRESH, json={"refresh_token": tokens["refresh_token"]}
    )
    assert rejected.status_code == 401


async def test_refresh_for_inactive_user_rolls_back_rotation(
    api_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory)
    tokens = await login(api_client, user.email)
    jti = refresh_jti(tokens["refresh_token"])

    async with session_factory() as session, session.begin():
        await session.execute(
            update(User).where(User.id == user.id).values(is_active=False)
        )

    response = await api_client.post(
        REFRESH, json={"refresh_token": tokens["refresh_token"]}
    )
    assert response.status_code == 403

    # The 403 is raised inside the transaction, so the rotation is undone:
    # the token is neither revoked nor replaced, and no new row exists.
    async with session_factory() as session:
        stored = await session.get(RefreshToken, jti)
        total = await session.scalar(
            select(func.count())
            .select_from(RefreshToken)
            .where(RefreshToken.user_id == user.id)
        )

    assert stored.revoked_at is None
    assert stored.replaced_by_jti is None
    assert total == 1


async def test_expired_refresh_token_is_rejected_without_revoking(
    api_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory)
    tokens = await login(api_client, user.email)
    jti = refresh_jti(tokens["refresh_token"])

    async with session_factory() as session, session.begin():
        await session.execute(
            update(RefreshToken)
            .where(RefreshToken.jti == jti)
            .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )

    response = await api_client.post(
        REFRESH, json={"refresh_token": tokens["refresh_token"]}
    )
    assert response.status_code == 401

    async with session_factory() as session:
        stored = await session.get(RefreshToken, jti)
    # Expiry is a rejection, not a reuse: the row keeps its history intact.
    assert stored.revoked_at is None
