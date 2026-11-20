"""What one Inventory answer means for the order aggregate.

The saga and the reconciler ask Inventory the same question, so they must also
read the answer the same way: only a *terminal* answer may move a `pending`
order, and every terminal answer has exactly one reason. Keeping the mapping in
one place is what stops a retried reservation from ending in a different state
than the original attempt would have.

The caller keeps both the transaction boundary and the noise level: this module
never commits, and it never decides whether a lost race is an error log or an
HTTP error.

That is why the outbox write lives here too. `enqueue` only stages a row, so the
event becomes visible exactly when the caller commits the status change: a
retried reservation cannot publish a fact the first attempt would not have, and
a rollback drops the status change and its event together.
"""

from __future__ import annotations

import uuid

from app.clients.inventory import ReservationOutcome, ReservationResult
from app.domain.events import order_failed, order_reserved
from app.domain.order import OrderReason, ensure_transition
from app.models.order import OrderStatus
from app.repositories.order import OrderRecord, OrderRepositoryPort
from app.repositories.outbox import OutboxRepositoryPort

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


async def fail_exhausted_reservation(
    *,
    repository: OrderRepositoryPort,
    outbox: OutboxRepositoryPort,
    order_id: uuid.UUID,
) -> OrderRecord | None:
    """Terminalise an order whose retry budget ran out.

    Called when the claim spent more than the allowed number of attempts and
    Inventory is still unreachable. `pending` would be a lie here: nobody will
    ever retry this order again, so it must stop looking fresh in the feed.

    Returns `None` when a cancel won the race (same contract as
    `apply_terminal_outcome`), in which case nothing is published either.
    """
    reason = OrderReason.RESERVATION_UNAVAILABLE
    ensure_transition(OrderStatus.PENDING, OrderStatus.FAILED, reason)

    record = await repository.transition(
        order_id=order_id,
        from_status=OrderStatus.PENDING,
        to_status=OrderStatus.FAILED,
        reason=reason,
        clear_lease=True,
    )
    if record is None:
        return None

    await outbox.enqueue(
        event=order_failed(record=record, reason=reason).to_outbox_event(),
    )
    return record


async def apply_terminal_outcome(
    *,
    repository: OrderRepositoryPort,
    outbox: OutboxRepositoryPort,
    order_id: uuid.UUID,
    reservation: ReservationResult,
) -> OrderRecord | None:
    """Shape TX2 for a *known* Inventory answer.

    Returns `None` when the conditional `UPDATE` found the order no longer
    `pending` — a cancel won the race, and the caller must roll back instead of
    resurrecting a terminal order. No event is staged in that case: there was no
    transition, so there is no fact to publish.

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

    record = await repository.transition(
        order_id=order_id,
        from_status=OrderStatus.PENDING,
        to_status=target,
        reason=reason,
        inventory_reservation_id=reservation_id,
        clear_lease=True,
    )
    if record is None:
        return None

    # Staged in the caller's transaction: `enqueue` does not commit, and the
    # event is built from the record the compare-and-set returned, so its
    # payload describes the state that is about to be committed.
    if target is OrderStatus.RESERVED:
        event = order_reserved(record=record)
    else:
        # `reason` is set above for every non-reserved outcome.
        event = order_failed(record=record, reason=reason)
    await outbox.enqueue(event=event.to_outbox_event())
    return record
