"""The write side of the outbox: a fact exists only if its transaction committed.

These tests treat the outbox table as the consumer would: every assertion reads
committed rows in a *separate* session, so a staged-but-rolled-back event is
invisible by construction. What is proved here is the coupling, not the payload
shape (`tests/test_events.py`) and not the claim/mark SQL
(`tests/integration/test_outbox_repository.py`):

* a terminal status change and its event appear together, exactly once;
* a lost compare-and-set, an idempotent replay and an unknown answer publish
  nothing, because nothing happened to the order;
* the reconciler publishes the same fact the saga would have.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Sequence

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.inventory import ReservationOutcome, ReservationResult
from app.domain.events import OrderEventType, order_reserved
from app.domain.order import OrderLine, OrderReason
from app.models.order import OrderStatus
from app.repositories.order import SqlOrderRepository
from app.repositories.outbox import SqlOutboxRepository
from app.services.exceptions import (
    InsufficientStockError,
    OrderNotCancellableError,
)
from app.services.order import OrderService
from app.services.pricing import NullPricing
from app.services.reconciler import Reconciler
from tests.integration.helpers import (
    PRODUCT_A,
    WAREHOUSE,
    age_lease,
    insert_order,
    outbox_rows,
)

STALE_AFTER = 30.0
MAX_ATTEMPTS = 3
ALERT_AFTER = 3600.0
RESERVATION_ID = uuid.UUID("cccccccc-3333-4333-8333-cccccccccccc")

LINES = (OrderLine(PRODUCT_A, WAREHOUSE, 2),)

Probe = Callable[[uuid.UUID], Awaitable[None]]


class FakeInventory:
    """Canned answer plus a hook that runs while the \"HTTP call\" is in flight."""

    def __init__(
        self,
        result: ReservationResult | None = None,
        *,
        before_return: Probe | None = None,
    ) -> None:
        self.result = result or ReservationResult(
            ReservationOutcome.RESERVED,
            reservation_id=RESERVATION_ID,
        )
        self.before_return = before_return

    async def reserve(
        self,
        *,
        order_id: uuid.UUID,
        lines: Sequence[OrderLine],
    ) -> ReservationResult:
        if self.before_return is not None:
            await self.before_return(order_id)
        return self.result


def build_service(session: AsyncSession, inventory: FakeInventory) -> OrderService:
    """The production wiring, minus the HTTP client."""
    return OrderService(
        session=session,
        repository=SqlOrderRepository(session),
        outbox=SqlOutboxRepository(session),
        inventory=inventory,
        pricing=NullPricing(),
        currency="RUB",
        stale_after_seconds=STALE_AFTER,
    )


def build_reconciler(
    session_factory: async_sessionmaker[AsyncSession],
    inventory: FakeInventory,
) -> Reconciler:
    """The worker's own defaults, so the test proves the shipped wiring."""
    return Reconciler(
        session_factory=session_factory,
        inventory=inventory,
        batch_size=10,
        stale_after_seconds=STALE_AFTER,
        max_attempts=MAX_ATTEMPTS,
        alert_after_seconds=ALERT_AFTER,
    )


# ---------------------------------------------------------------------------
# The saga
# ---------------------------------------------------------------------------


async def test_a_reserved_order_is_committed_together_with_its_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        result = await build_service(session, FakeInventory()).create_order(
            user_id=uuid.uuid4(),
            lines=LINES,
            idempotency_key="key-outbox-reserved",
        )

    [row] = await outbox_rows(session_factory)

    assert row.event_type == OrderEventType.RESERVED.value
    assert row.aggregate_id == result.record.order.id
    assert row.payload["status"] == OrderStatus.RESERVED.value
    assert row.payload["inventory_reservation_id"] == str(RESERVATION_ID)
    assert row.payload["order_id"] == str(result.record.order.id)
    # `occurred_at` is the commit timestamp, so the row knows when it happened;
    # nobody has published it yet.
    assert row.occurred_at is not None
    assert row.published_at is None
    assert row.attempts == 0


async def test_a_refusal_commits_the_failed_event_before_the_error_is_raised(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    inventory = FakeInventory(ReservationResult(ReservationOutcome.REJECTED))

    async with session_factory() as session:
        with pytest.raises(InsufficientStockError):
            await build_service(session, inventory).create_order(
                user_id=uuid.uuid4(),
                lines=LINES,
            )

    # The order *is* failed and committed, so the event describing it must be
    # committed too: dropping it would hide a real transition.
    [row] = await outbox_rows(session_factory)
    assert row.event_type == OrderEventType.FAILED.value
    assert row.payload["status"] == OrderStatus.FAILED.value
    assert row.payload["reason"] == OrderReason.INSUFFICIENT_STOCK.value


async def test_an_unknown_answer_publishes_nothing(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    inventory = FakeInventory(ReservationResult(ReservationOutcome.UNAVAILABLE))

    async with session_factory() as session:
        result = await build_service(session, inventory).create_order(
            user_id=uuid.uuid4(),
            lines=LINES,
        )

    assert result.reservation_pending
    # `pending` is not a fact: the order will be settled by the reconciler.
    assert await outbox_rows(session_factory) == []


async def test_a_replayed_create_does_not_publish_twice(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()

    async with session_factory() as session:
        service = build_service(session, FakeInventory())
        await service.create_order(
            user_id=user_id,
            lines=LINES,
            idempotency_key="key-outbox-replay",
        )
        await service.create_order(
            user_id=user_id,
            lines=LINES,
            idempotency_key="key-outbox-replay",
        )

    assert len(await outbox_rows(session_factory)) == 1


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


async def test_a_cancel_publishes_one_event_and_a_repeat_publishes_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    created = await insert_order(session_factory, user_id=user_id)
    await age_lease(
        session_factory,
        order_id=created.order.id,
        seconds=STALE_AFTER * 10,
    )

    async with session_factory() as session:
        await build_service(session, FakeInventory()).cancel_order(
            order_id=created.order.id,
            user_id=user_id,
        )

    [row] = await outbox_rows(session_factory)
    assert row.event_type == OrderEventType.CANCELLED.value
    assert row.payload["reason"] == OrderReason.USER_REQUESTED.value

    async with session_factory() as other:
        with pytest.raises(OrderNotCancellableError):
            await build_service(other, FakeInventory()).cancel_order(
                order_id=created.order.id,
                user_id=user_id,
            )

    assert len(await outbox_rows(session_factory)) == 1


async def test_a_cancel_that_wins_the_race_leaves_only_its_own_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Inventory reserved, but the order was already cancelled: no `reserved`."""
    user_id = uuid.uuid4()

    async def cancel_during_reserve(order_id: uuid.UUID) -> None:
        await age_lease(
            session_factory,
            order_id=order_id,
            seconds=STALE_AFTER * 10,
        )
        async with session_factory() as other:
            # Through the service, so the cancel publishes its own fact.
            await build_service(other, FakeInventory()).cancel_order(
                order_id=order_id,
                user_id=user_id,
            )

    async with session_factory() as session:
        await build_service(
            session,
            FakeInventory(before_return=cancel_during_reserve),
        ).create_order(user_id=user_id, lines=LINES)

    [row] = await outbox_rows(session_factory)
    # The saga lost the compare-and-set, so it must not announce a reservation
    # for an order that never reached `reserved`.
    assert row.event_type == OrderEventType.CANCELLED.value


# ---------------------------------------------------------------------------
# The reconciler and transaction boundaries
# ---------------------------------------------------------------------------


async def test_the_reconciler_publishes_the_event_the_saga_would_have(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    created = await insert_order(session_factory, user_id=uuid.uuid4())
    await age_lease(
        session_factory,
        order_id=created.order.id,
        seconds=STALE_AFTER * 10,
    )

    report = await build_reconciler(session_factory, FakeInventory()).process_batch()

    assert report.reserved == 1
    [row] = await outbox_rows(session_factory)
    assert row.event_type == OrderEventType.RESERVED.value
    assert row.aggregate_id == created.order.id
    assert row.payload["inventory_reservation_id"] == str(RESERVATION_ID)


async def test_an_event_staged_in_a_rolled_back_transaction_never_exists(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    record = await insert_order(session_factory, user_id=uuid.uuid4())

    async with session_factory() as session:
        await SqlOutboxRepository(session).enqueue(
            event=order_reserved(record=record).to_outbox_event(),
        )
        # Exactly what a failed TX2 does: the row is staged, never committed.
        await session.rollback()

    assert await outbox_rows(session_factory) == []
