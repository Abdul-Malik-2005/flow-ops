"""Shared fixtures for order tests.

Settings are constructed at import time by `app.core.config`, so the required
environment variables have to exist before any `app.*` import. These values
only satisfy pydantic validation; tests that need a real database override
them explicitly.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://test:test@localhost:5432/order_test_db",
)
os.environ.setdefault("DEBUG", "false")
os.environ.setdefault("ENVIRONMENT", "testing")

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.core.security import load_public_key
from app.db.session import get_session
from app.main import app


class FakeSession:
    """Minimal stand-in for `AsyncSession` used by the readiness probe.

    It records executed statements so a test can prove that `/health` never
    touches the database and that `/ready` issues exactly one `SELECT 1`.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.error: Exception | None = None

    async def execute(self, statement: object) -> None:
        if self.error is not None:
            raise self.error
        self.statements.append(str(statement))


@pytest.fixture()
def fake_session() -> FakeSession:
    return FakeSession()


@pytest.fixture()
async def api_client(fake_session: FakeSession) -> AsyncIterator[AsyncClient]:
    """`AsyncClient` talking to the real ASGI app: no network, no database."""

    async def override_get_session() -> AsyncIterator[FakeSession]:
        yield fake_session

    app.dependency_overrides[get_session] = override_get_session
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            yield client
    finally:
        app.dependency_overrides.pop(get_session, None)


@pytest.fixture()
def sqlalchemy_error() -> SQLAlchemyError:
    return SQLAlchemyError("database is down")


# ---------------------------------------------------------------------------
# Authentication: throwaway RSA keys and token factory
# ---------------------------------------------------------------------------

ClaimsMutatorType = Callable[[dict[str, object]], dict[str, object]]
TokenFactory = Callable[..., str]


@pytest.fixture(scope="session")
def user_id() -> uuid.UUID:
    """Stable subject (`sub`) shared by every token built in tests."""
    return uuid.UUID("3f1c9d1e-0b7a-4c56-9a1f-7c2b0f5a8d11")


@pytest.fixture(scope="session")
def rsa_keys() -> tuple[bytes, bytes]:
    """A throwaway RSA key pair: (private PEM, public PEM).

    Generated per test session, so no real key material ever enters the repo.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private_pem, public_pem


@pytest.fixture()
def public_key_file(
    rsa_keys: tuple[bytes, bytes],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """Point the service at a generated public key and reset the key cache."""
    _, public_pem = rsa_keys
    path = tmp_path / "jwt_public.pem"
    path.write_bytes(public_pem)
    monkeypatch.setattr(settings, "jwt_public_key_path", str(path))
    load_public_key.cache_clear()
    yield path
    load_public_key.cache_clear()


@pytest.fixture()
def make_token(
    rsa_keys: tuple[bytes, bytes],
    user_id: uuid.UUID,
) -> TokenFactory:
    """Build a signed access token, with optional claim/header overrides."""
    private_pem, _ = rsa_keys

    def _make_token(
        *,
        claims: ClaimsMutatorType | None = None,
        kid: str | None = settings.jwt_kid,
    ) -> str:
        now = datetime.now(UTC)
        payload: dict[str, object] = {
            "sub": str(user_id),
            "role": "user",
            "type": "access",
            "iat": now,
            "exp": now + timedelta(minutes=15),
            "jti": str(uuid.uuid4()),
        }
        if claims is not None:
            payload = claims(payload)
        headers = {"kid": kid} if kid is not None else {}
        return jwt.encode(payload, private_pem, algorithm="RS256", headers=headers)

    return _make_token
