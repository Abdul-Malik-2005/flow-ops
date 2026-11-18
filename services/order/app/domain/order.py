"""Pure domain rules shared by the API layer, the saga and the reconciler.

Nothing here imports FastAPI, SQLAlchemy or httpx, so the same rules can be
reused by an out-of-band worker later. The persisted status enum still lives in
`app.models.order`: it is part of the database contract and duplicating it here
would let the two definitions drift.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from app.domain.exceptions import (
    InvalidOrderError,
    InvalidReasonError,
    InvalidTransitionError,
)
from app.models.order import OrderStatus

_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")


class OrderReason(StrEnum):
    """Allowed values of `order_status_history.reason`.

    The column is a `varchar`, not an enum, so adding a reason costs no
    migration; this set is what keeps the audit trail meaningful. Cancellation
    and failure reasons are disjoint on purpose: a cancelled order must not look
    like a stock rejection.
    """

    USER_REQUESTED = "user_requested"

    INSUFFICIENT_STOCK = "insufficient_stock"
    INVENTORY_ITEM_NOT_FOUND = "inventory_item_not_found"
    INVENTORY_REJECTED = "inventory_rejected"
    #: The reconciler retried the reservation until the attempt budget ran out
    #: and Inventory was still unreachable. Terminal by design: an order that
    #: nobody will ever retry must not look like a fresh `pending` one.
    RESERVATION_UNAVAILABLE = "reservation_unavailable"


CANCELLATION_REASONS: frozenset[OrderReason] = frozenset({OrderReason.USER_REQUESTED})

FAILURE_REASONS: frozenset[OrderReason] = frozenset(
    {
        OrderReason.INSUFFICIENT_STOCK,
        OrderReason.INVENTORY_ITEM_NOT_FOUND,
        OrderReason.INVENTORY_REJECTED,
        OrderReason.RESERVATION_UNAVAILABLE,
    }
)

# Deliberately explicit instead of "everything except terminal states": a saga
# that can move an order anywhere is how `cancelled -> reserved` bugs appear.
# `reserved` has no outgoing edges yet: Inventory cannot release or confirm, so
# paying/fulfilling an order is a later block and must extend this map.
ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING: frozenset(
        {OrderStatus.RESERVED, OrderStatus.CANCELLED, OrderStatus.FAILED}
    ),
    OrderStatus.RESERVED: frozenset(),
    OrderStatus.PAID: frozenset(),
    OrderStatus.FULFILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.FAILED: frozenset(),
}


@dataclass(frozen=True, slots=True)
class OrderLine:
    """A request line as the domain sees it: no prices, no ORM, no Pydantic."""

    product_id: UUID
    warehouse_id: UUID
    quantity: int


def normalize_lines(lines: Iterable[OrderLine]) -> tuple[OrderLine, ...]:
    """Merge duplicate positions and return a deterministic order.

    Inventory aggregates identical positions on its side; Order must match it or
    a body with two identical positions would violate
    `UNIQUE(order_id, product_id, warehouse_id)` and surface as a 500.
    """
    totals: dict[tuple[UUID, UUID], int] = {}

    for line in lines:
        if line.quantity <= 0:
            raise InvalidOrderError(
                f"quantity must be positive, got {line.quantity} "
                f"for product {line.product_id}"
            )
        key = (line.product_id, line.warehouse_id)
        totals[key] = totals.get(key, 0) + line.quantity

    if not totals:
        raise InvalidOrderError("an order must contain at least one line")

    ordered = sorted(
        totals.items(),
        key=lambda item: str(item[0][0]) + str(item[0][1]),
    )
    return tuple(
        OrderLine(product_id=product_id, warehouse_id=warehouse_id, quantity=quantity)
        for (product_id, warehouse_id), quantity in ordered
    )


def canonical_request_payload(
    lines: Iterable[OrderLine],
    *,
    currency: str,
) -> dict[str, object]:
    """Canonical form of a create request, independent of input ordering.

    Prices are absent by design: they are not client-controlled, so they must
    not influence whether two requests are "the same".
    """
    if not _CURRENCY_PATTERN.fullmatch(currency):
        raise InvalidOrderError(f"currency must be an ISO-4217 code, got {currency!r}")

    return {
        "currency": currency,
        "items": [
            {
                "product_id": str(line.product_id),
                "warehouse_id": str(line.warehouse_id),
                "quantity": line.quantity,
            }
            for line in normalize_lines(lines)
        ],
    }


def canonical_request_hash(lines: Iterable[OrderLine], *, currency: str) -> str:
    """sha256 of the canonical payload — the idempotency fingerprint.

    A retry with the same `Idempotency-Key` but a different body has a different
    hash, which is what turns a silent "returning the old order" into an honest
    409.
    """
    payload = canonical_request_payload(lines, currency=currency)
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def can_transition(current: OrderStatus, target: OrderStatus) -> bool:
    """Whether `current -> target` is a legal saga step."""
    return target in ALLOWED_TRANSITIONS.get(current, frozenset())


def ensure_reason(target: OrderStatus, reason: OrderReason | None) -> None:
    """Reject a trail entry whose reason contradicts its destination status."""
    if target is OrderStatus.CANCELLED:
        if reason not in CANCELLATION_REASONS:
            raise InvalidReasonError(
                "cancellation requires one of "
                f"{sorted(reason.value for reason in CANCELLATION_REASONS)}, "
                f"got {reason}"
            )
        return

    if target is OrderStatus.FAILED:
        if reason not in FAILURE_REASONS:
            raise InvalidReasonError(
                f"failure requires one of "
                f"{sorted(reason.value for reason in FAILURE_REASONS)}, got {reason}"
            )
        return

    if reason is not None:
        raise InvalidReasonError(f"transition to '{target}' must not carry a reason")


def ensure_transition(
    current: OrderStatus,
    target: OrderStatus,
    reason: OrderReason | None = None,
) -> OrderStatus:
    """Validate a saga step and return `target` for convenient assignment."""
    if not can_transition(current, target):
        raise InvalidTransitionError(current, target)

    ensure_reason(target, reason)
    return target
