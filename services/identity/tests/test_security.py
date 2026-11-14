"""Unit tests for app.core.security: Argon2id and RS256 JWT."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from app.core import security
from app.core.config import settings
from app.core.security import (
    InvalidTokenError,
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from app.models.user import UserRole

# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------


def test_hash_and_verify_roundtrip() -> None:
    h = hash_password("s3cret")
    assert verify_password("s3cret", h) is True


def test_verify_rejects_wrong_password() -> None:
    h = hash_password("s3cret")
    assert verify_password("wrong", h) is False


def test_verify_rejects_malformed_hash() -> None:
    assert verify_password("s3cret", "not-a-hash") is False


def test_hash_is_salted() -> None:
    """Same password hashed twice produces different hashes (random salt)."""
    h1 = hash_password("same")
    h2 = hash_password("same")
    assert h1 != h2
    assert verify_password("same", h1) is True
    assert verify_password("same", h2) is True


# ---------------------------------------------------------------------------
# Access token
# ---------------------------------------------------------------------------


def test_access_token_claims(
    jwt_keys: None,
    user_id: uuid.UUID,
) -> None:
    token = create_access_token(user_id, UserRole.ADMIN)
    payload = decode_token(token, "access")

    assert payload["sub"] == str(user_id)
    assert payload["role"] == "admin"
    assert payload["type"] == "access"
    assert "jti" in payload
    assert "iat" in payload
    assert "exp" in payload
    assert payload["exp"] > payload["iat"]


def test_access_token_has_kid_header(
    jwt_keys: None,
    user_id: uuid.UUID,
) -> None:
    token = create_access_token(user_id, UserRole.USER)
    header = jwt.get_unverified_header(token)
    assert header["kid"] == settings.jwt_kid
    assert header["alg"] == settings.jwt_algorithm


# ---------------------------------------------------------------------------
# Refresh token
# ---------------------------------------------------------------------------


def test_refresh_token_claims(
    jwt_keys: None,
    user_id: uuid.UUID,
    refresh_jti: uuid.UUID,
) -> None:
    token = create_refresh_token(user_id, refresh_jti)
    payload = decode_token(token, "refresh")

    assert payload["sub"] == str(user_id)
    assert payload["type"] == "refresh"
    assert payload["jti"] == str(refresh_jti)
    assert "role" not in payload


# ---------------------------------------------------------------------------
# Type checking
# ---------------------------------------------------------------------------


def test_refresh_token_rejected_as_access(access_token: str) -> None:
    with pytest.raises(InvalidTokenError):
        decode_token(access_token, "refresh")


def test_access_token_rejected_as_refresh(refresh_token: str) -> None:
    with pytest.raises(InvalidTokenError):
        decode_token(refresh_token, "access")


# ---------------------------------------------------------------------------
# Signature / algorithm / garbage
# ---------------------------------------------------------------------------


def test_token_signed_with_other_key_is_rejected(
    jwt_keys: None,
    user_id: uuid.UUID,
) -> None:
    """A token signed by an unrelated RSA key must fail verification."""
    attacker_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    forged = jwt.encode(
        {
            "sub": str(user_id),
            "role": "admin",
            "type": "access",
            "iat": now,
            "exp": now + timedelta(minutes=5),
            "jti": str(uuid.uuid4()),
        },
        attacker_key,
        algorithm="RS256",
        headers={"kid": settings.jwt_kid},
    )
    with pytest.raises(InvalidTokenError):
        decode_token(forged, "access")


def test_garbage_token_is_rejected(jwt_keys: None) -> None:
    with pytest.raises(InvalidTokenError):
        decode_token("not.a.jwt", "access")


def test_expired_token_is_rejected(
    jwt_keys: None,
    user_id: uuid.UUID,
) -> None:
    now = datetime.now(UTC)
    expired = jwt.encode(
        {
            "sub": str(user_id),
            "role": "user",
            "type": "access",
            "iat": now - timedelta(hours=1),
            "exp": now - timedelta(minutes=1),
            "jti": str(uuid.uuid4()),
        },
        security._private_key(),
        algorithm=settings.jwt_algorithm,
        headers={"kid": settings.jwt_kid},
    )
    with pytest.raises(InvalidTokenError):
        decode_token(expired, "access")


# ---------------------------------------------------------------------------
# Missing required claims
# ---------------------------------------------------------------------------


def test_missing_required_claim_is_rejected(
    jwt_keys: None,
    user_id: uuid.UUID,
) -> None:
    now = datetime.now(UTC)
    no_jti = jwt.encode(
        {
            "sub": str(user_id),
            "role": "user",
            "type": "access",
            "iat": now,
            "exp": now + timedelta(minutes=5),
            # jti deliberately missing
        },
        security._private_key(),
        algorithm=settings.jwt_algorithm,
        headers={"kid": settings.jwt_kid},
    )
    with pytest.raises(InvalidTokenError):
        decode_token(no_jti, "access")
