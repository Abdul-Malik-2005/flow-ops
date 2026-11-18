"""Append-only audit trail of order status transitions.

This table answers "what happened", not "who was notified": message delivery
belongs to the outbox (step 6+). Keeping the two concerns apart means the audit
history will not have to be rewritten when RabbitMQ appears.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.order import OrderStatus, order_status_type


class OrderStatusHistory(Base):
    __tablename__ = "order_status_history"

    __table_args__ = (
        # A transition to the same status is not a transition: it would hide
        # real bugs (e.g. a retry that re-applies `reserved`) in a noisy trail.
        CheckConstraint(
            "from_status IS NULL OR from_status <> to_status",
            name="status_changed",
        ),
        # Chronological trail of a single order; also the future outbox source.
        # `None` as the first argument is required: SQLAlchemy treats a leading
        # string positional argument as the index *name*, not as a column.
        Index(None, "order_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("orders.id", ondelete="CASCADE"),
        nullable=False,
    )

    # NULL marks the creation of the order: there was no previous state.
    from_status: Mapped[OrderStatus | None] = mapped_column(
        order_status_type(),
        nullable=True,
    )
    to_status: Mapped[OrderStatus] = mapped_column(
        order_status_type(),
        nullable=False,
    )

    # Free-form on purpose: reasons grow (`insufficient_stock`, `payment_failed`,
    # `expired`, ...) and a native enum would need a migration per new value.
    # The allowed set is validated by the domain layer.
    reason: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<OrderStatusHistory order_id={self.order_id} "
            f"{self.from_status} -> {self.to_status}>"
        )
