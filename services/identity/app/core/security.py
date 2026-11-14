"""Cryptography: password hashing (Argon2id) and JWT signing/verification (RS256)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import (
    RSAPrivateKey,
    RSAPublicKey,
)

from app.core.config import settings
from app.models.user import UserRole

# ---------------------------------------------------------------------------
# Password hashing (Argon2id)
# ---------------------------------------------------------------------------

# OWASP-recommended parameters for Argon2id (as of 2024+).
# Explicit values — not relying on library defaults that may change.
_hasher = PasswordHasher(
    time_cost=3,
    memory_cost=65536,  # 64 MiB
    parallelism=4,
    hash_len=32,
    salt_len=16,
    type=__import__("argon2").Type.ID,
)


def hash_password(password: str) -> str:
    """Hash a plaintext password with Argon2id."""
    return _hasher.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a plaintext password against an Argon2id hash.

    Returns False on mismatch or malformed hash — never raises.
    """
    try:
        return _hasher.verify(password_hash, password)
    except VerifyMismatchError, InvalidHashError:
        return False


# ---------------------------------------------------------------------------
# RS256 keys (cached per process)
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _private_key() -> RSAPrivateKey:
    path = Path(settings.jwt_private_key_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(
            f"JWT private key not found at {path}. "
            "Run `poetry run python scripts/gen_keys.py` or set JWT_PRIVATE_KEY_PATH."
        )
    data = path.read_bytes()
    key = serialization.load_pem_private_key(data, password=None)
    if not isinstance(key, RSAPrivateKey):
        raise TypeError(f"Expected RSA private key, got {type(key).__name__}")
    return key


@lru_cache(maxsize=1)
def _public_key() -> RSAPublicKey:
    path = Path(settings.jwt_public_key_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(
            f"JWT public key not found at {path}. "
            "Run `poetry run python scripts/gen_keys.py` or set JWT_PUBLIC_KEY_PATH."
        )
    data = path.read_bytes()
    key = serialization.load_pem_public_key(data)
    if not isinstance(key, RSAPublicKey):
        raise TypeError(f"Expected RSA public key, got {type(key).__name__}")
    return key


# ---------------------------------------------------------------------------
# JWT
# ---------------------------------------------------------------------------

TokenType = Literal["access", "refresh"]


def _encode(payload: dict[str, Any]) -> str:
    return jwt.encode(
        payload,
        _private_key(),
        algorithm=settings.jwt_algorithm,
        headers={"kid": settings.jwt_kid},
    )


def _now() -> datetime:
    return datetime.now(UTC)


def create_access_token(sub: uuid.UUID, role: UserRole) -> str:
    """Issue an access token.

    Claims: sub, role, type=access, exp, iat, jti.  kid is in the JWT header.
    """
    now = _now()
    payload = {
        "sub": str(sub),
        "role": role.value,
        "type": "access",
        "iat": now,
        "exp": now + timedelta(seconds=settings.access_token_ttl_seconds),
        "jti": str(uuid.uuid4()),
    }
    return _encode(payload)


def create_refresh_token(sub: uuid.UUID, jti: uuid.UUID) -> str:
    """Issue a refresh token.

    Claims: sub, type=refresh, exp, iat, jti.  No role — refresh must reload
    the role from the DB on use.  kid is in the JWT header.
    """
    now = _now()
    payload = {
        "sub": str(sub),
        "type": "refresh",
        "iat": now,
        "exp": now + timedelta(seconds=settings.refresh_token_ttl_seconds),
        "jti": str(jti),
    }
    return _encode(payload)


class InvalidTokenError(Exception):
    """Raised when a JWT is invalid, expired, or has the wrong type."""


def decode_token(token: str, expected_type: TokenType) -> dict[str, Any]:
    """Decode and validate a JWT.  Raises InvalidTokenError on any problem.

    Checks: signature (RS256), expiration, and the `type` claim.
    Does NOT check `jti` against the DB — that is the service layer's job.
    """
    try:
        payload = jwt.decode(
            token,
            _public_key(),
            algorithms=[settings.jwt_algorithm],
            options={"require": ["exp", "iat", "sub", "type", "jti"]},
        )
    except jwt.InvalidTokenError as exc:
        raise InvalidTokenError(str(exc)) from exc

    if payload.get("type") != expected_type:
        raise InvalidTokenError(
            f"expected token type {expected_type!r}, got {payload.get('type')!r}"
        )
    return payload
