"""Unit tests for local access-token verification.

No database, no network: the key pair comes from the `rsa_keys` fixture in
`conftest.py` and is written to a temp directory by `public_key_file`.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from app.core.config import settings
from app.core.security import (
    InvalidTokenError,
    Principal,
    decode_access_token,
    load_public_key,
)


def test_valid_token_returns_principal(
    public_key_file: Path,
    make_token,
    user_id,
) -> None:
    principal = decode_access_token(make_token())

    assert principal == Principal(user_id=user_id, role="user")


def test_expired_token_is_rejected(public_key_file: Path, make_token) -> None:
    past = datetime.now(UTC) - timedelta(minutes=1)

    token = make_token(
        claims=lambda p: {**p, "iat": past - timedelta(minutes=15), "exp": past}
    )

    with pytest.raises(InvalidTokenError):
        decode_access_token(token)


def test_unexpected_kid_is_rejected(public_key_file: Path, make_token) -> None:
    with pytest.raises(InvalidTokenError, match="kid"):
        decode_access_token(make_token(kid="identity-2"))


def test_token_without_kid_is_rejected(public_key_file: Path, make_token) -> None:
    with pytest.raises(InvalidTokenError, match="kid"):
        decode_access_token(make_token(kid=None))


def test_refresh_token_is_not_accepted(public_key_file: Path, make_token) -> None:
    token = make_token(claims=lambda p: {**p, "type": "refresh"})

    with pytest.raises(InvalidTokenError, match="access token"):
        decode_access_token(token)


def test_token_signed_by_another_key_is_rejected(
    public_key_file: Path,
    user_id,
) -> None:
    foreign_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": str(user_id),
            "type": "access",
            "iat": now,
            "exp": now + timedelta(minutes=15),
        },
        foreign_key,
        algorithm="RS256",
        headers={"kid": settings.jwt_kid},
    )

    with pytest.raises(InvalidTokenError):
        decode_access_token(token)


def test_non_uuid_subject_is_rejected(public_key_file: Path, make_token) -> None:
    token = make_token(claims=lambda p: {**p, "sub": "not-a-uuid"})

    with pytest.raises(InvalidTokenError, match="subject"):
        decode_access_token(token)


def test_missing_public_key_file_fails_loudly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "jwt_public_key_path", str(tmp_path / "absent.pem"))
    load_public_key.cache_clear()
    try:
        # `decode_access_token` would fail on the malformed token first, so the
        # key loader is exercised directly: that is what `lifespan` calls.
        with pytest.raises(FileNotFoundError, match="JWT public key"):
            load_public_key()
    finally:
        load_public_key.cache_clear()
