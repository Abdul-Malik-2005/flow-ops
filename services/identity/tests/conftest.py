"""Shared fixtures for identity tests."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

# Ensure settings can be constructed before any `app.*` import.
# These values are used ONLY to satisfy pydantic validation; tests that
# need real DB / keys override or ignore them via fixtures.
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://test:test@localhost:5432/identity_test_db",
)
os.environ.setdefault("DEBUG", "false")
os.environ.setdefault("ENVIRONMENT", "testing")
# JWT fields have defaults in Settings; key paths are overridden per-test
# by the `jwt_keys` fixture (monkeypatch). No need to set them here.

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.core import security
from app.core.config import settings
from app.models.user import UserRole


def _generate_keypair(directory: Path) -> tuple[Path, Path]:
    """Generate an RSA 2048 keypair, write PEM files, return their paths."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    private_path = directory / "jwt_private.pem"
    public_path = directory / "jwt_public.pem"

    private_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    public_path.write_bytes(
        private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    return private_path, public_path


@pytest.fixture()
def jwt_keys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Point `settings` at a fresh keypair and clear the security cache."""
    private_path, public_path = _generate_keypair(tmp_path)

    monkeypatch.setattr(settings, "jwt_private_key_path", str(private_path))
    monkeypatch.setattr(settings, "jwt_public_key_path", str(public_path))

    security._private_key.cache_clear()
    security._public_key.cache_clear()

    yield

    security._private_key.cache_clear()
    security._public_key.cache_clear()


@pytest.fixture()
def user_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture()
def refresh_jti() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture()
def access_token(jwt_keys: None, user_id: uuid.UUID) -> str:
    return security.create_access_token(user_id, UserRole.ADMIN)


@pytest.fixture()
def refresh_token(jwt_keys: None, user_id: uuid.UUID, refresh_jti: uuid.UUID) -> str:
    return security.create_refresh_token(user_id, refresh_jti)
