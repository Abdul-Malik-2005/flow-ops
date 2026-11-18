"""Opaque pagination tokens for the list endpoint.

A cursor is `base64url("<iso8601>|<uuid>")`. It is deliberately *not* signed: it
carries no secret, only a position in the caller's own feed, and the ownership
filter is applied server-side anyway. Base64 keeps clients from parsing and
depending on the internal tuple.
"""

from __future__ import annotations

import base64
import binascii
import uuid
from datetime import datetime

from app.repositories.order import OrderCursor


class InvalidCursorError(ValueError):
    """The token is not a cursor this service issued."""


def encode_cursor(cursor: OrderCursor) -> str:
    raw = f"{cursor.created_at.isoformat()}|{cursor.order_id}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def decode_cursor(value: str) -> OrderCursor:
    padded = value + "=" * (-len(value) % 4)

    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        created_at_raw, _, order_id_raw = raw.partition("|")
        created_at = datetime.fromisoformat(created_at_raw)
        order_id = uuid.UUID(order_id_raw)
    except (binascii.Error, UnicodeError, ValueError) as exc:
        raise InvalidCursorError("cursor is not a valid pagination token") from exc

    # A naive timestamp would be interpreted in the server's timezone and
    # silently shift the window, so it is rejected instead.
    if created_at.tzinfo is None:
        raise InvalidCursorError("cursor timestamp must be timezone-aware")

    return OrderCursor(created_at=created_at, order_id=order_id)
