"""Consumer-side journal of delivered order events: every fact, recorded once.

This table is the read side of the outbox. The producer only guarantees that the
broker has the message; this journal is where a consumer records that it has
processed it. Keeping the row lets an operator see what the service actually
consumed, independently of the outbox it was published from.

`event_id` is the primary key on purpose: it is the AMQP `message_id` and the
`outbox_events.id` the message was built from. Delivery is at-least-once, so the
same event can arrive twice; `INSERT ... ON CONFLICT (event_id) DO NOTHING`
makes the replay a no-op instead of a duplicate row or a read-modify-write race.

The journal is append-only and **never** feeds back into `outbox_events`: a
consumer inside the Order service must not publish events of its own, otherwise
`order.reserved` would loop through the broker forever.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class OrderEventProjection(Base):
    __tablename__ = "order_event_projections"

    __table_args__ = (
        # Reading one order's journal in the order the facts happened.
        Index(
            "ix_order_event_projections_aggregate_id_occurred_at",
            "aggregate_id",
            "occurred_at",
        ),
    )

    # No FK to `outbox_events`: the journal must stay readable if an operator
    # cleans the outbox, and a foreign key would add lock contention for a
    # guarantee we do not need.
    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
    )

    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
    )

    # `varchar`, not a native enum: a new event type must not cost an
    # `ALTER TYPE` migration (same reasoning as `outbox_events.event_type`).
    event_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    # Nullable even though committed rows always carry it: the contract allows
    # `null` for a row that was never committed, and a consumer must not reject
    # a message the transport cannot fix.
    occurred_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # When the consumer wrote the row, not when the fact happened. Useful to
    # spot a replay or a backlog without trusting the producer's clock.
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<OrderEventProjection event_id={self.event_id} "
            f"type={self.event_type} aggregate={self.aggregate_id}>"
        )
