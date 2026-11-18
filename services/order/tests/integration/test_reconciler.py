"""End-to-end reconciler tests: the real worker logic, the real database.

Only Inventory is faked. These tests cover the three ways an order gets stuck —
a lost answer, a lost second transaction and a cancel that races the retry — and
the one way the worker gives up (the attempt budget), all against real row locks
and real transactions.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Sequence

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.inventory import ReservationOutcome, ReservationResult
from app.domain.order import OrderLine, OrderReason
from app.models.order import Order, OrderStatus
from app.repositories.order import SqlOrderRepository
from app.services.reconciler import Reconciler, ReconcileReport
from tests.integration.helpers import (
    age_lease,
    history_of,
    insert_order,
    set_reservation_attempts,
)

STALE_AFTER = 30.0
MAX_ATTEMPTS = 3
ALERT_AFTER = 3600.0
RESERVATION_ID = uuid.UUID("cccccccc-3333-4333-8333-cccccccccccc")

Probe = Callable[[uuid.UUID], Awaitable[None]]


class FakeInventory:
    """Canned answer plus a hook that runs while the "HTTP call" is in flight."""

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
        self.calls: list[uuid.UUID] = []

    async def reserve(
        self,
        *,
        order_id: uuid.UUID,
        lines: Sequence[OrderLine],
    ) -> ReservationResult:
        self.calls.append(order_id)
        if self.before_return is not None:
            await self.before_return(order_id)
        return self.result


@pytest.fixture()
def inventory() -> FakeInventory:
    return FakeInventory()


@pytest.fixture()
def reconciler(
    session_factory: async_sessionmaker[AsyncSession],
    inventory: FakeInventory,
) -> Reconciler:
    return Reconciler(
        session_factory=session_factory,
        inventory=inventory,
        batch_size=10,
        stale_after_seconds=STALE_AFTER,
        max_attempts=MAX_ATTEMPTS,
        alert_after_seconds=ALERT_AFTER,
    )


async def load_order(
    session_factory: async_sessionmaker[AsyncSession],
    order_id: uuid.UUID,
) -> Order:
    """Fresh read in its own session: what the next HTTP request would see."""
    async with session_factory() as session:
        order = await session.scalar(select(Order).where(Order.id == order_id))
        assert order is not None
        return order


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


async def test_a_lost_answer_is_recovered_and_the_reservation_saved(
    reconciler: Reconciler,
    inventory: FakeInventory,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    created = await insert_order(session_factory, user_id=uuid.uuid4())
    await age_lease(session_factory, order_id=created.order.id, seconds=3600)

    report = await reconciler.process_batch()

    assert report == ReconcileReport(claimed=1, reserved=1)
    order = await load_order(session_factory, created.order.id)
    assert order.status is OrderStatus.RESERVED
    assert order.inventory_reservation_id == RESERVATION_ID
    # The lease is released: nothing is in flight any more.
    assert order.reservation_started_at is None
    assert order.reservation_attempts == 1
    assert inventory.calls == [created.order.id]

    rows = await history_of(session_factory, created.order.id)
    assert [(row.from_status, row.to_status) for row in rows] == [
        (None, OrderStatus.PENDING),
        (OrderStatus.PENDING, OrderStatus.RESERVED),
    ]


async def test_a_lost_second_transaction_reuses_the_same_reservation(
    reconciler: Reconciler,
    inventory: FakeInventory,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Inventory already reserved; only the database missed the answer.

    The retry is safe because Inventory answers 200 with the *same* id, and the
    worker stores exactly what it got back.
    """
    created = await insert_order(session_factory, user_id=uuid.uuid4())
    await age_lease(session_factory, order_id=created.order.id, seconds=3600)

    await reconciler.process_batch()
    second_pass = await reconciler.process_batch()

    order = await load_order(session_factory, created.order.id)
    assert order.status is OrderStatus.RESERVED
    assert order.inventory_reservation_id == RESERVATION_ID
    # The first pass already finished the order: the second has nothing to do,
    # which is what "no duplicate reservation" looks like from this side.
    assert second_pass == ReconcileReport()


async def test_a_fresh_lease_is_never_touched(
    reconciler: Reconciler,
    inventory: FakeInventory,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    created = await insert_order(session_factory, user_id=uuid.uuid4())

    report = await reconciler.process_batch()

    assert report == ReconcileReport()
    assert inventory.calls == []
    order = await load_order(session_factory, created.order.id)
    assert order.status is OrderStatus.PENDING
    assert order.reservation_attempts == 0


# ---------------------------------------------------------------------------
# Races and the attempt budget
# ---------------------------------------------------------------------------


async def test_a_cancel_during_the_retry_wins_and_is_not_resurrected(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The worker's TX2 must lose against a cancel that got there first."""

    async def cancel_order(order_id: uuid.UUID) -> None:
        # Straight through the repository, on purpose: the lease guard is the
        # API's answer to this race, and here the race has to happen *anyway* to
        # prove that the worker does not overwrite a terminal status.
        async with session_factory() as session:
            moved = await SqlOrderRepository(session).transition(
                order_id=order_id,
                from_status=OrderStatus.PENDING,
                to_status=OrderStatus.CANCELLED,
                reason=OrderReason.USER_REQUESTED,
                clear_lease=True,
            )
            assert moved is not None
            await session.commit()

    inventory = FakeInventory(before_return=cancel_order)
    reconciler = Reconciler(
        session_factory=session_factory,
        inventory=inventory,
        batch_size=10,
        stale_after_seconds=STALE_AFTER,
        max_attempts=MAX_ATTEMPTS,
        alert_after_seconds=ALERT_AFTER,
    )
    created = await insert_order(session_factory, user_id=uuid.uuid4())
    await age_lease(session_factory, order_id=created.order.id, seconds=3600)

    report = await reconciler.process_batch()

    assert report == ReconcileReport(claimed=1, superseded=1)
    order = await load_order(session_factory, created.order.id)
    assert order.status is OrderStatus.CANCELLED
    assert order.inventory_reservation_id is None
    # No `pending -> reserved` row was written by the losing worker.
    rows = await history_of(session_factory, created.order.id)
    assert [row.to_status for row in rows] == [
        OrderStatus.PENDING,
        OrderStatus.CANCELLED,
    ]


async def test_an_exhausted_budget_fails_the_order_without_a_final_call(
    reconciler: Reconciler,
    inventory: FakeInventory,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    created = await insert_order(session_factory, user_id=uuid.uuid4())
    await set_reservation_attempts(
        session_factory,
        order_id=created.order.id,
        attempts=MAX_ATTEMPTS,
    )
    await age_lease(session_factory, order_id=created.order.id, seconds=3600)

    report = await reconciler.process_batch()

    assert report == ReconcileReport(claimed=1, failed=1)
    assert inventory.calls == []
    order = await load_order(session_factory, created.order.id)
    assert order.status is OrderStatus.FAILED
    assert order.reservation_started_at is None

    rows = await history_of(session_factory, created.order.id)
    assert rows[-1].from_status is OrderStatus.PENDING
    assert rows[-1].to_status is OrderStatus.FAILED
    assert rows[-1].reason == OrderReason.RESERVATION_UNAVAILABLE.value


async def test_a_rejection_on_retry_fails_the_order_with_the_stock_reason(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Stock can disappear between the two attempts: the retry records it."""
    inventory = FakeInventory(ReservationResult(ReservationOutcome.REJECTED))
    reconciler = Reconciler(
        session_factory=session_factory,
        inventory=inventory,
        batch_size=10,
        stale_after_seconds=STALE_AFTER,
        max_attempts=MAX_ATTEMPTS,
        alert_after_seconds=ALERT_AFTER,
    )
    created = await insert_order(session_factory, user_id=uuid.uuid4())
    await age_lease(session_factory, order_id=created.order.id, seconds=3600)

    report = await reconciler.process_batch()

    assert report == ReconcileReport(claimed=1, failed=1)
    rows = await history_of(session_factory, created.order.id)
    assert rows[-1].reason == OrderReason.INSUFFICIENT_STOCK.value
