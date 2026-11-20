"""Order domain events: which facts the service publishes, and in what shape.

Two boundaries are respected here:

* **Only terminal facts.** `order.reserved`, `order.failed`, `order.cancelled` —
  no `order.created`, no intermediate states. A `pending` order is not news: the
  world learns about an order when it either got its stock or will never get it.
  `order.failed` carries the reason, so a consumer can tell a stock rejection
  from "Inventory was unreachable until the retry budget ran out".
* **No floats, no ORM leaks.** The payload is a JSON-ready snapshot: ids and
  timestamps as strings, money as a *string* produced from `Decimal`. A JSON
  number would be parsed as a binary float on the other side, and `0.1 + 0.2`
  is not a price. Consumers also cannot join against this database, so the
  snapshot carries the positions and the total instead of a reference.

The event is built from `OrderRecord` — the same read model the saga already
returns from `transition()`/`cancel()` — so emitting costs no extra query and
happens inside the same transaction as the status change.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final

from app.domain.order import OrderReason, ensure_reason
from app.models.order import OrderStatus
from app.models.outbox_event import OutboxEvent

if TYPE_CHECKING:
    from app.models.order_item import OrderItem
    from app.repositories.order import OrderRecord


#: Version of the wire envelope. A compatible (additive) change bumps it, and an
#: old consumer must tolerate the bump; a breaking change is a new event type
#: (`order.reserved.v2`), not a bump of this field (docs/architecture.md §5).
SCHEMA_VERSION: Final = 1


class OrderEventType(StrEnum):
    """Values of `outbox_events.event_type`.

    A `varchar` in the database on purpose: a new event type is a code change,
    not a migration. These names are the public contract, so they are versioned
    with the payload, not with the table.
    """

    RESERVED = "order.reserved"
    FAILED = "order.failed"
    CANCELLED = "order.cancelled"


@dataclass(frozen=True, slots=True)
class OrderEvent:
    """A fact ready to be written to the outbox.

    `event_id` is generated here, not by the database: the same id is the
    `event_id` a consumer deduplicates by, and it has to exist before the row is
    inserted (delivery is at-least-once, so duplicates are expected).
    """

    event_id: uuid.UUID
    event_type: OrderEventType
    aggregate_id: uuid.UUID
    payload: dict[str, Any]

    def to_outbox_event(self) -> OutboxEvent:
        """Turn the fact into a row for the caller's transaction.

        `occurred_at` is deliberately left to the database: `now()` is the
        transaction timestamp, so the event is stamped with the moment the
        status change committed, not with whenever the relay picked it up.
        """
        return OutboxEvent(
            id=self.event_id,
            aggregate_id=self.aggregate_id,
            event_type=self.event_type.value,
            payload=self.payload,
        )


def order_reserved(*, record: OrderRecord) -> OrderEvent:
    """`pending -> reserved`: stock is held, the order can move on."""
    order = record.order
    payload = {
        **_order_snapshot(record),
        "inventory_reservation_id": (
            str(order.inventory_reservation_id)
            if order.inventory_reservation_id is not None
            else None
        ),
    }
    return _build(OrderEventType.RESERVED, order.id, payload)


def order_failed(*, record: OrderRecord, reason: OrderReason) -> OrderEvent:
    """`pending -> failed`: terminal, with the reason a consumer can act on."""
    # Reuses the domain rule instead of re-listing reasons: a payload that says
    # "failed because the user cancelled" must be impossible, not just unlikely.
    ensure_reason(OrderStatus.FAILED, reason)
    payload = {**_order_snapshot(record), "reason": reason.value}
    return _build(OrderEventType.FAILED, record.order.id, payload)


def order_cancelled(*, record: OrderRecord, reason: OrderReason) -> OrderEvent:
    """`pending -> cancelled`: the customer (for now) is the only canceller."""
    ensure_reason(OrderStatus.CANCELLED, reason)
    payload = {**_order_snapshot(record), "reason": reason.value}
    return _build(OrderEventType.CANCELLED, record.order.id, payload)


def _build(
    event_type: OrderEventType,
    aggregate_id: uuid.UUID,
    payload: dict[str, Any],
) -> OrderEvent:
    return OrderEvent(
        event_id=uuid.uuid4(),
        event_type=event_type,
        aggregate_id=aggregate_id,
        payload=payload,
    )


def _order_snapshot(record: OrderRecord) -> dict[str, Any]:
    """Fields every order event carries, so consumers parse one shape.

    `total_amount` is `None` until Catalog provides pricing: keeping `null`
    instead of `0.00` preserves the difference between "free" and "unknown".
    """
    order = record.order
    return {
        "order_id": str(order.id),
        "user_id": str(order.user_id),
        "status": order.status.value,
        "currency": order.currency,
        "total_amount": _money(order.total_amount),
        "items": _items_snapshot(record.items),
    }


def _items_snapshot(items: Iterable[OrderItem]) -> list[dict[str, Any]]:
    """Positions in a stable order, so the payload is reproducible.

    Sorted by ids rather than trusting the query order: two events for the same
    order must serialize identically, or a diff-based consumer would see noise.
    """
    ordered = sorted(
        items,
        key=lambda item: (str(item.product_id), str(item.warehouse_id)),
    )
    return [
        {
            "product_id": str(item.product_id),
            "warehouse_id": str(item.warehouse_id),
            "quantity": item.quantity,
        }
        for item in ordered
    ]


def _money(value: Decimal | None) -> str | None:
    """Serialize money as a string with the scale the database stores.

    `format(value, "f")` keeps `Decimal("1990.00")` as `"1990.00"`; `str()` would
    do the same, but `format` makes the intent explicit and never switches to
    scientific notation for large amounts.
    """
    return None if value is None else format(value, "f")
