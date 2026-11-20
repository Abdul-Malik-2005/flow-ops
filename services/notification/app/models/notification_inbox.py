"""The inbox: every fact Notification was told, recorded exactly once.

This is the transactional inbox pattern, not a read-side projection of a
database it can query. Order publishes at-least-once, so the same event can
arrive twice; `event_id` is the primary key and the deduplication key, and the
insert is `ON CONFLICT (event_id) DO NOTHING`.

Keeping the row is what turns at-least-once delivery into exactly-once handling:
a later sender can read the inbox, see what has not been delivered yet, and mark
it — instead of re-notifying the customer every time the broker redelivers. The
inbox is append-only from this process's point of view and never feeds back into
the broker: a consumer that published would make `order.reserved` loop forever.

There is no foreign key to Order's tables: they live in another database that
this service cannot read, and the fact travels in the `payload` snapshot.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, Integer, String, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class NotificationInbox(Base):
    __tablename__ = "notification_inbox"

    __table_args__ = (
        # Reading one order's facts in the order they happened, for a future
        # sender that walks the inbox per aggregate.
        Index(
            "ix_notification_inbox_aggregate_id_occurred_at",
            "aggregate_id",
            "occurred_at",
        ),
        # The sender's feed: only unsent rows, so the index stays tiny even
        # though the table keeps the full delivery history forever.
        Index(
            "ix_notification_inbox_unsent",
            "occurred_at",
            "event_id",
            postgresql_where=text("sent_at IS NULL"),
        ),
    )

    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
    )

    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
    )

    # `varchar`, not a native enum: a new event type is a code change, not an
    # `ALTER TYPE` migration (the contract is versioned, the table is not).
    event_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    # Nullable even though committed events always carry it: the contract allows
    # `null` for a row that was never committed, and a consumer must not reject
    # a message the transport cannot fix.
    occurred_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # When Notification wrote the row, not when the fact happened. Useful to
    # spot a replay or a backlog without trusting the producer's clock.
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # --- Delivery state (the sender's half of the row) ------------------
    #
    # `sent_at IS NULL` means "recorded but not delivered yet". It is set only
    # after the sender returned, so a crash in between re-sends the notification:
    # at-least-once, and `event_id` is the idempotency key the provider uses to
    # absorb a replay.
    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Lease of the current send attempt. The sender never holds a transaction
    # across the provider call, so a second sender must be able to tell "someone
    # is working on this row" from "this row was abandoned".
    locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Includes failed attempts, because the claim increments it *before* the send
    # call. Once it reaches the configured maximum the row leaves the feed and
    # surfaces as an error log instead of being retried forever — it is never
    # deleted, so nothing is lost.
    attempts: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        default=0,
        server_default=text("0"),
    )

    def __repr__(self) -> str:
        return (
            f"<NotificationInbox event_id={self.event_id} "
            f"type={self.event_type} aggregate={self.aggregate_id}>"
        )
