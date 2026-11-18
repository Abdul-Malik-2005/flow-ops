"""Tests for the opaque pagination token."""

from __future__ import annotations

import base64
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from app.api.pagination import InvalidCursorError, decode_cursor, encode_cursor
from app.repositories.order import OrderCursor

MOMENT = datetime(2026, 11, 18, 12, 34, 56, 789012, tzinfo=UTC)
ORDER_ID = uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa")


def test_round_trip_preserves_the_position_exactly() -> None:
    cursor = OrderCursor(created_at=MOMENT, order_id=ORDER_ID)

    assert decode_cursor(encode_cursor(cursor)) == cursor


def test_round_trip_keeps_non_utc_offsets_equivalent() -> None:
    local = MOMENT.astimezone(UTC)
    cursor = OrderCursor(created_at=local, order_id=ORDER_ID)

    assert decode_cursor(encode_cursor(cursor)).created_at == MOMENT


def test_token_is_url_safe_and_unpadded() -> None:
    token = encode_cursor(OrderCursor(created_at=MOMENT, order_id=ORDER_ID))

    assert "=" not in token
    assert "+" not in token
    assert "/" not in token


def _encode_raw(raw: str) -> str:
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


@pytest.mark.parametrize(
    "token",
    [
        "not-a-cursor",
        _encode_raw("only-a-timestamp"),
        _encode_raw(f"{MOMENT.isoformat()}|not-a-uuid"),
        _encode_raw("2026-11-18T12:34:56+00:00"),  # naive: no id, no separator
    ],
)
def test_unreadable_tokens_are_rejected(token: str) -> None:
    with pytest.raises(InvalidCursorError):
        decode_cursor(token)


def test_naive_timestamps_are_rejected() -> None:
    """A naive timestamp would silently shift the window by the server offset."""
    naive = datetime(2026, 11, 18, 12, 0)

    with pytest.raises(InvalidCursorError):
        decode_cursor(_encode_raw(f"{naive.isoformat()}|{ORDER_ID}"))


def test_a_cursor_from_the_future_is_still_valid() -> None:
    """Emitting an empty page is harmless; rejecting would break clock skew."""
    future = datetime.now(UTC) + timedelta(days=1)

    decoded = decode_cursor(
        encode_cursor(OrderCursor(created_at=future, order_id=ORDER_ID))
    )

    assert decoded.created_at == future
