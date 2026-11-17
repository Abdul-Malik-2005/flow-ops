"""Smoke tests proving the integration fixtures talk to a real database."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

pytestmark = pytest.mark.integration


async def test_connection_is_alive(test_engine: AsyncEngine) -> None:
    async with test_engine.connect() as connection:
        result = await connection.execute(text("SELECT 1"))

    assert result.scalar_one() == 1


async def test_alembic_created_the_identity_schema(db_session: AsyncSession) -> None:
    tables = await db_session.execute(
        text(
            "SELECT to_regclass('public.users'),"
            " to_regclass('public.refresh_tokens'),"
            " to_regclass('public.login_attempts'),"
            " to_regclass('public.alembic_version')"
        )
    )

    assert all(table is not None for table in tables.one())


async def test_tables_are_truncated_between_tests(db_session: AsyncSession) -> None:
    count = await db_session.scalar(text("SELECT count(*) FROM users"))

    assert count == 0
