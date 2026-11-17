"""Helpers for integration tests: build rows directly in PostgreSQL.

Creating a user through `/auth/register` costs one Argon2id hash (~100 ms).
Most scenarios do not care *how* the user appeared, only that the row exists, so
they insert it directly with a memoized hash. Only the endpoints under test go
through the API.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core import security
from app.models.refresh_token import RefreshToken
from app.models.user import User, UserRole

SERVICE_ROOT = Path(__file__).resolve().parents[2]
TEST_DATABASE_SUFFIX = "_test_db"
TEST_PASSWORD = "correct-horse-battery-staple"
REGISTER = "/api/v1/auth/register"
LOGIN = "/api/v1/auth/login"
REFRESH = "/api/v1/auth/refresh"
LOGOUT = "/api/v1/auth/logout"
ME = "/api/v1/auth/me"
USERS = "/api/v1/users"

_hashes: dict[str, str] = {}


def password_hash(password: str = TEST_PASSWORD) -> str:
    """Argon2id hash of `password`, memoized for the whole test run."""
    if password not in _hashes:
        _hashes[password] = security.hash_password(password)
    return _hashes[password]


async def create_user(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    email: str = "user@example.com",
    password: str = TEST_PASSWORD,
    role: UserRole = UserRole.USER,
    is_active: bool = True,
) -> User:
    """Insert a user row and return the detached ORM object."""
    async with session_factory() as session, session.begin():
        user = User(
            email=email.strip().lower(),
            password_hash=password_hash(password),
            role=role,
            is_active=is_active,
        )
        session.add(user)
    return user


async def create_refresh_row(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: uuid.UUID,
    jti: uuid.UUID | None = None,
    expires_at: datetime | None = None,
) -> RefreshToken:
    """Insert a refresh-token row without going through `/auth/login`."""
    async with session_factory() as session, session.begin():
        row = RefreshToken(
            jti=jti or uuid.uuid4(),
            user_id=user_id,
            expires_at=expires_at or datetime.now(UTC) + timedelta(seconds=3600),
        )
        session.add(row)
    return row


async def login(
    client: AsyncClient,
    email: str,
    password: str = TEST_PASSWORD,
) -> dict[str, str]:
    """Log in and return the token pair, asserting the happy path."""
    response = await client.post(LOGIN, json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    return response.json()


def bearer(access_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}


def refresh_jti(refresh_token: str) -> uuid.UUID:
    """`jti` claim of a refresh token, as a UUID."""
    return uuid.UUID(security.decode_token(refresh_token, "refresh")["jti"])


def run_alembic(database_url: str, *args: str) -> None:
    """Run Alembic against `database_url` in a subprocess.

    `migrations/env.py` calls `asyncio.run()`, which cannot run inside a live
    pytest event loop. A subprocess also mirrors how the container applies
    migrations on start-up.
    """
    env = {**os.environ, "DATABASE_URL": database_url}
    completed = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=SERVICE_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"alembic {' '.join(args)} failed with exit code "
            f"{completed.returncode}\nstdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
