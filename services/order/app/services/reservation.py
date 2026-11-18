"""What one Inventory answer means for the order aggregate.

The saga and the reconciler ask Inventory the same question, so they must also
read the answer the same way: only a *terminal* answer may move a `pending`
order, and every terminal answer has exactly one reason. Keeping the mapping in
one place is what stops a retried reservation from ending in a different state
than the original attempt would have.

The caller keeps both the transaction boundary and the noise level: this module
never commits, and it never decides whether a lost race is an error log or an
HTTP error.
"""

from __future__ import annotations

import uuid

from app.clients.inventory import ReservationOutcome, ReservationResult
from app.domain.order import OrderReason, ensure_transition
from app.models.order import OrderStatus
from app.repositories.order import OrderRecord, OrderRepositoryPort

#: Terminal answers -> the reason written to the status history.
FAILURE_REASONS: dict[ReservationOutcome, OrderReason] = {
    ReservationOutcome.REJECTED: OrderReason.INSUFFICIENT_STOCK,
    ReservationOutcome.ITEM_NOT_FOUND: OrderReason.INVENTORY_ITEM_NOT_FOUND,
    ReservationOutcome.INVALID_REQUEST: OrderReason.INVENTORY_REJECTED,
}


def failure_reason(outcome: ReservationOutcome) -> OrderReason:
    """Reason for a terminal rejection.

    `KeyError` for `RESERVED` and `UNAVAILABLE` is intentional: neither of them
    is a failure, and inventing a default reason would hide the caller's bug.
    """
    return FAILURE_REASONS[outcome]


async def apply_terminal_outcome(
    *,
    repository: OrderRepositoryPort,
    order_id: uuid.UUID,
    reservation: ReservationResult,
) -> OrderRecord | None:
    """Shape TX2 for a *known* Inventory answer.

    Returns `None` when the conditional `UPDATE` found the order no longer
    `pending` — a cancel won the race, and the caller must roll back instead of
    resurrecting a terminal order.

    `UNAVAILABLE` raises `KeyError` through `failure_reason`: "we do not know"
    is never a reason to write a status.
    """
    if reservation.outcome is ReservationOutcome.RESERVED:
        target: OrderStatus = OrderStatus.RESERVED
        reason: OrderReason | None = None
        reservation_id = reservation.reservation_id
    else:
        target = OrderStatus.FAILED
        reason = failure_reason(reservation.outcome)
        # A rejected reservation has no id worth keeping.
        reservation_id = None

    # The domain table is checked here as well as in the repository, so an
    # illegal saga step fails in a unit test instead of only in the database.
    ensure_transition(OrderStatus.PENDING, target, reason)

    return await repository.transition(
        order_id=order_id,
        from_status=OrderStatus.PENDING,
        to_status=target,
        reason=reason,
        inventory_reservation_id=reservation_id,
        clear_lease=True,
    )
