"""Login auditing against real PostgreSQL.

The audit row is written inside the login transaction and the domain error is
raised *after* the commit. On a fake session "committed" is not observable; here
every assertion opens a fresh session, so a row that was rolled back is simply
absent.
"""

from __future__ import annotations

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.login_attempt import LoginAttempt
from app.models.refresh_token import RefreshToken
from tests.integration.helpers import (
    LOGIN,
    TEST_PASSWORD,
    create_user,
    password_hash,
)

pytestmark = pytest.mark.integration


async def _attempts(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[LoginAttempt]:
    """Read audit rows from a brand-new session, bypassing any test cache."""
    async with session_factory() as session:
        return list(
            await session.scalars(
                select(LoginAttempt).order_by(LoginAttempt.created_at)
            )
        )


async def test_wrong_password_is_audited_and_committed(
    api_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory)

    response = await api_client.post(
        LOGIN,
        json={"email": "User@Example.COM", "password": "not-the-password"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid email or password"

    attempts = await _attempts(session_factory)
    assert len(attempts) == 1
    attempt = attempts[0]
    assert attempt.email == "user@example.com"
    assert attempt.user_id == user.id
    assert attempt.success is False
    assert attempt.ip == "127.0.0.1"

    async with session_factory() as session:
        tokens = await session.scalar(select(func.count()).select_from(RefreshToken))
    assert tokens == 0


async def test_unknown_email_is_audited_without_user_id(
    api_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    response = await api_client.post(
        LOGIN,
        json={"email": "ghost@example.com", "password": TEST_PASSWORD},
    )

    assert response.status_code == 401

    attempts = await _attempts(session_factory)
    assert len(attempts) == 1
    assert attempts[0].email == "ghost@example.com"
    assert attempts[0].user_id is None
    assert attempts[0].success is False


async def test_inactive_user_login_is_audited_and_returns_403(
    api_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory, is_active=False)

    response = await api_client.post(
        LOGIN,
        json={"email": user.email, "password": TEST_PASSWORD},
    )

    assert response.status_code == 403

    attempts = await _attempts(session_factory)
    assert len(attempts) == 1
    # `success` records that the password was correct; authorisation is a
    # separate decision, so the row must not be flipped to False here.
    assert attempts[0].success is True
    assert attempts[0].user_id == user.id

    async with session_factory() as session:
        tokens = await session.scalar(select(func.count()).select_from(RefreshToken))
    assert tokens == 0


async def test_password_hash_from_helper_verifies(
    api_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Guard for the memoized hash: a stale cache would break every test above."""
    user = await create_user(session_factory)

    assert user.password_hash == password_hash()

    response = await api_client.post(
        LOGIN,
        json={"email": user.email, "password": TEST_PASSWORD},
    )
    assert response.status_code == 200
