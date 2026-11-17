"""Integration fixtures: a real PostgreSQL instance, real SQL, real migrations.

The unit/API suite in `tests/` runs against in-memory fakes. This package runs
the same application against the database it ships with, so two rules keep it
safe:

* destructive statements are allowed only when the connected database name ends
  with ``_test_db`` — checked both in the URL *and* on the server through
  ``current_database()``;
* the schema is built once per session by Alembic, never by
  ``Base.metadata.create_all``, so the tests exercise the DDL that production
  actually runs.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Iterator
from typing import NoReturn

import pytest
from dotenv import dotenv_values
from httpx import ASGITransport, AsyncClient
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.db.session import get_session
from app.main import app
from tests.integration.helpers import (
    SERVICE_ROOT,
    TEST_DATABASE_SUFFIX,
    run_alembic,
)

_TRUNCATE = text(
    "TRUNCATE TABLE users, refresh_tokens, login_attempts RESTART IDENTITY CASCADE"
)


def _stop(message: str) -> NoReturn:
    """Fail in CI, skip on a developer machine without a test database.

    A silent skip in CI would make the integration suite a no-op, so the
    `REQUIRE_INTEGRATION_TESTS` switch turns it into a hard failure there.
    """
    if os.getenv("REQUIRE_INTEGRATION_TESTS") == "1":
        pytest.fail(message)
    pytest.skip(message)


def _env_file_database_name() -> str | None:
    """Database name configured in `.env`, ignoring the process environment.

    `tests/conftest.py` sets a placeholder `DATABASE_URL` (already pointing at a
    `*_test_db`) before importing `app.*`, so `settings.database_url` cannot be
    used for this check. CI has no `.env`; there the suffix guard is the only
    protection needed.
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
            "PostgreSQL database; see services/identity/README.md."
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
    """Drop and recreate `public` in the test database.

    Runs in its own short-lived event loop and engine: a session-scoped async
    fixture would bind asyncpg connections to a loop that `pytest-asyncio`
    (function-scoped loops) cannot reuse.
    """
    expected = make_url(database_url).database or ""
    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            current = (
                await connection.execute(text("SELECT current_database()"))
            ).scalar_one()
            if current != expected or not str(current).endswith(TEST_DATABASE_SUFFIX):
                pytest.fail(
                    f"Refusing to reset the schema of {current!r}: expected a "
                    f"{TEST_DATABASE_SUFFIX} database."
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
    """Engine with a fresh connection per checkout.

    `NullPool` keeps concurrent tests honest: every session gets its own
    PostgreSQL backend, which is what row-level locking and `SELECT ... FOR
    UPDATE` are about.
    """
    engine = create_async_engine(migrated_database, poolclass=NullPool)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
async def clean_tables(test_engine: AsyncEngine) -> AsyncIterator[None]:
    """Start every integration test from an empty schema.

    `TRUNCATE` is used instead of a transactional rollback because the service
    layer opens and commits its own transactions (`async with session.begin()`),
    so an outer transaction cannot be rolled back around a test.
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
    """A session for assertions that does not share transactions with the API."""
    async with session_factory() as session:
        yield session


@pytest.fixture()
async def api_client(
    session_factory: async_sessionmaker[AsyncSession],
    jwt_keys: None,
) -> AsyncIterator[AsyncClient]:
    """`AsyncClient` hitting the real app with one real session per request.

    A session per request mirrors production (`get_session`), which matters
    because services manage their own `session.begin()` blocks.
    """

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    previous_overrides = app.dependency_overrides.copy()
    app.dependency_overrides[get_session] = override_get_session
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            yield client
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(previous_overrides)
