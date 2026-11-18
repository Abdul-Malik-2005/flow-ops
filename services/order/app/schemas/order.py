"""Request and response schemas for the order API.

Two rules shape this module:

* `user_id` never appears in a request. Ownership comes from the verified token,
  so an unknown field such as `user_id` is rejected (`extra="forbid"`) instead of
  being silently dropped — a loud 422 is better than a security audit later.
* The response never exposes `idempotency_key`, `request_hash` or `user_id`:
  a dedicated model makes accidental leaks impossible.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.domain.order import CANCELLATION_REASONS, OrderLine, OrderReason
from app.models.order import Order, OrderStatus
from app.models.order_item import OrderItem

# A single order with thousands of positions is a denial-of-service waiting to
# happen: the saga would fan the request out to Inventory in one call and hold a
# row lock for the whole round trip.
MAX_ORDER_LINES = 100


class OrderItemCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: UUID
    warehouse_id: UUID
    quantity: int = Field(gt=0)

    def to_line(self) -> OrderLine:
        """Convert to the domain type so rules never depend on Pydantic."""
        return OrderLine(
            product_id=self.product_id,
            warehouse_id=self.warehouse_id,
            quantity=self.quantity,
        )


class OrderCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[OrderItemCreate] = Field(min_length=1, max_length=MAX_ORDER_LINES)


class CancelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: OrderReason = OrderReason.USER_REQUESTED

    @field_validator("reason")
    @classmethod
    def _reason_must_be_a_cancellation_reason(cls, reason: OrderReason) -> OrderReason:
        # Failure reasons (insufficient_stock, ...) are internal: a client must
        # not be able to write them into the audit trail.
        if reason not in CANCELLATION_REASONS:
            raise ValueError(f"{reason} is not a cancellation reason")
        return reason


class OrderItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    product_id: UUID
    warehouse_id: UUID
    quantity: int
    unit_price: Decimal | None
    line_total: Decimal | None


class OrderResponse(BaseModel):
    """Public representation of an order.

    `items` is passed explicitly: `Order` has no ORM relationship on purpose
    (async lazy loading is a `MissingGreenlet` trap), so the repository loads
    children and hands them over.
    """

    id: UUID
    status: OrderStatus
    currency: str
    total_amount: Decimal | None
    created_at: datetime
    updated_at: datetime
    items: list[OrderItemResponse]

    @classmethod
    def from_order(cls, order: Order, items: Sequence[OrderItem]) -> Self:
        return cls(
            id=order.id,
            status=order.status,
            currency=order.currency,
            total_amount=order.total_amount,
            created_at=order.created_at,
            updated_at=order.updated_at,
            items=[OrderItemResponse.model_validate(item) for item in items],
        )


class OrderListResponse(BaseModel):
    items: list[OrderResponse]
    next_cursor: str | None = None
