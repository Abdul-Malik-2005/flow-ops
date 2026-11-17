"""Race condition: two refreshes with the same token at the same time.

`AuthService.refresh` takes a row lock (`SELECT ... FOR UPDATE`) before deciding
whether a token is still valid. This test gives the two requests genuinely
independent database connections — a shared engine could serialise them in the
pool and hide the bug this test is about.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextvars import ContextVar

import pytest
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.db.session import get_session
from app.main import app
from app.models.refresh_token import RefreshToken
from tests.integration.helpers import REFRESH, create_user, login

pytestmark = pytest.mark.integration


async def test_parallel_refresh_rotates_exactly_once(
    api_client: AsyncClient,
    database_url: str,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user = await create_user(session_factory)
    tokens = await login(api_client, user.email)

    engines = [create_async_engine(database_url, poolclass=NullPool) for _ in range(2)]
    factories = [
        async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
        for engine in engines
    ]

    # One endpoint, two "request handlers": the ContextVar lets each task pick
    # its own connection without touching global state between tasks.
    choice: ContextVar[async_sessionmaker[AsyncSession] | None] = ContextVar(
        "session_factory_choice", default=None
    )

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        factory = choice.get()
        assert factory is not None, "task did not select a session factory"
        async with factory() as session:
            yield session

    async def refresh(factory: async_sessionmaker[AsyncSession]) -> Response:
        choice.set(factory)
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            return await client.post(
                REFRESH, json={"refresh_token": tokens["refresh_token"]}
            )

    previous_overrides = app.dependency_overrides.copy()
    app.dependency_overrides[get_session] = override_get_session
    try:
        responses = await asyncio.gather(*(refresh(factory) for factory in factories))
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(previous_overrides)
        for engine in engines:
            await engine.dispose()

    assert sorted(response.status_code for response in responses) == [200, 401]

    rejected = next(response for response in responses if response.status_code == 401)
    assert "reuse" in rejected.json()["detail"].lower()

    # The loser saw a rotated token, so it revoked the whole chain — including
    # the token the winner just received. Only the two rotation rows exist.
    async with session_factory() as session:
        total = await session.scalar(
            select(func.count())
            .select_from(RefreshToken)
            .where(RefreshToken.user_id == user.id)
        )
        active = await session.scalar(
            select(func.count())
            .select_from(RefreshToken)
            .where(
                RefreshToken.user_id == user.id,
                RefreshToken.revoked_at.is_(None),
            )
        )

    assert total == 2
    assert active == 0

    winner_token = next(
        response for response in responses if response.status_code == 200
    ).json()["refresh_token"]
    denied = await api_client.post(REFRESH, json={"refresh_token": winner_token})
    assert denied.status_code == 401
