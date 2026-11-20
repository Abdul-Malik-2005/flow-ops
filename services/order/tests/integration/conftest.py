"""Integration fixtures: real PostgreSQL, real SQL, real migrations.

The unit suite in `tests/` runs on fakes. This package runs the persistence and
saga code against the database it ships with, guarded by the same two rules as
the Identity service:

* destructive statements are allowed only when the database name ends with
  ``_test_db``, checked both in the URL and on the server via
  ``current_database()``;
* the schema is built by Alembic, never by ``Base.metadata.create_all``, so the
  tests exercise the DDL that production runs.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Iterator
from typing import NoReturn

import pytest
from dotenv import dotenv_values
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.core.config import settings
from tests.integration.helpers import (
    SERVICE_ROOT,
    TEST_DATABASE_SUFFIX,
    run_alembic,
)

_TRUNCATE = text(
    "TRUNCATE TABLE order_event_projections, outbox_events, "
    "order_status_history, order_items, orders "
    "RESTART IDENTITY CASCADE"
)


def _stop(message: str) -> NoReturn:
    """Fail in CI, skip on a developer machine without a test database.

    A silent skip in CI would make this suite a no-op, so
    `REQUIRE_INTEGRATION_TESTS` turns it into a hard failure there.
    """
    if os.getenv("REQUIRE_INTEGRATION_TESTS") == "1":
        pytest.fail(message)
    pytest.skip(message)


def _env_file_database_name() -> str | None:
    """Database name configured in `.env`, ignoring the process environment.

    `tests/conftest.py` sets a placeholder `DATABASE_URL` before importing
    `app.*`, so `settings.database_url` cannot be used for this check. CI has no
    `.env`; there the suffix guard is the only protection needed.
    """
    env_file = SERVICE_ROOT / ".env"
    if not env_file.exists():
        return None

    url = dotenv_values(env_file).get("DATABASE_URL")
    return (make_url(url).database or "") if url else None


def _require_test_database_url() -> str:
    """Return a URL that is safe to destroy, or stop the run."""
    url = settings.test_database_url
    if not url:
        _stop(
            "TEST_DATABASE_URL is not set. Integration tests need a real "
            "PostgreSQL database (see .env.example)."
        )

    database = make_url(url).database or ""
    if not database.endswith(TEST_DATABASE_SUFFIX):
        pytest.fail(
            f"Refusing to run: {database!r} does not end with "
            f"{TEST_DATABASE_SUFFIX!r}. Integration tests truncate tables."
        )

    main_database = _env_file_database_name()
    if main_database is not None and database == main_database:
        pytest.fail(
            "Refusing to run: TEST_DATABASE_URL points at the application "
            "database. Use a dedicated *_test_db database."
        )

    return url


async def _reset_public_schema(database_url: str) -> None:
    """Drop and recreate `public` so a stale schema cannot mask a migration."""
    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            current = (
                await connection.execute(text("SELECT current_database()"))
            ).scalar_one()
            # The URL check is not enough: a typo in the host or a redirect could
            # still point the drop at the wrong database.
            if current != make_url(database_url).database:
                pytest.fail(
                    f"Refusing to reset the schema of {current!r}: expected "
                    f"{make_url(database_url).database!r}."
                )

            await connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
            await connection.execute(text("CREATE SCHEMA public"))
            await connection.commit()
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def database_url() -> str:
    return _require_test_database_url()


@pytest.fixture(scope="session")
def migrated_database(database_url: str) -> Iterator[str]:
    """Bring the test database to `head` exactly once per pytest session."""
    asyncio.run(_reset_public_schema(database_url))
    run_alembic(database_url, "upgrade", "head")
    yield database_url


@pytest.fixture()
async def test_engine(migrated_database: str) -> AsyncIterator[AsyncEngine]:
    """Engine with a fresh backend per checkout.

    `NullPool` keeps the tests honest about row-level behaviour: every session
    gets its own PostgreSQL connection instead of borrowing a pooled one.
    """
    engine = create_async_engine(migrated_database, poolclass=NullPool)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
async def clean_tables(test_engine: AsyncEngine) -> AsyncIterator[None]:
    """Start every integration test from an empty schema.

    `TRUNCATE` instead of a transactional rollback: the saga commits its own
    transactions, so an outer transaction cannot be rolled back around a test.
    """
    async with test_engine.begin() as connection:
        await connection.execute(_TRUNCATE)
    yield


@pytest.fixture()
def session_factory(
    test_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


@pytest.fixture()
async def db_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """A session for assertions that does not share transactions with the app."""
    async with session_factory() as session:
        yield session
