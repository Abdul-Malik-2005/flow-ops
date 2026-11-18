"""Saga tests: the real service, the real database, a fake Inventory.

Inventory is faked on purpose — the interesting behaviour is what Order does with
each answer (which transaction, which status, which reason), not Inventory's own
stock logic, which its service already tests.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.inventory import ReservationOutcome, ReservationResult
from app.domain.order import OrderLine, OrderReason
from app.models.order import Order, OrderStatus
from app.repositories.order import CancelStatus, OrderCursor, SqlOrderRepository
from app.services.exceptions import (
    IdempotencyConflictError,
    InsufficientStockError,
    InventoryItemNotFoundError,
    InventoryRejectedError,
    OrderNotCancellableError,
    OrderNotFoundError,
)
from app.services.order import OrderService
from app.services.pricing import NullPricing
from tests.integration.helpers import (
    PRODUCT_A,
    PRODUCT_B,
    WAREHOUSE,
    age_lease,
    history_of,
    insert_order,
    set_created_at,
)

STALE_AFTER = 30.0
RESERVATION_ID = uuid.UUID("cccccccc-3333-4333-8333-cccccccccccc")

Probe = Callable[[uuid.UUID], Awaitable[None]]


class FakeInventory:
    """Returns a canned outcome and records the calls it received."""

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
        self.calls: list[tuple[uuid.UUID, tuple[OrderLine, ...]]] = []

    async def reserve(
        self,
        *,
        order_id: uuid.UUID,
        lines: tuple[OrderLine, ...],
    ) -> ReservationResult:
        self.calls.append((order_id, tuple(lines)))
        if self.before_return is not None:
            await self.before_return(order_id)
        return self.result


@pytest.fixture()
def inventory() -> FakeInventory:
    return FakeInventory()


@pytest.fixture()
async def service(
    session_factory: async_sessionmaker[AsyncSession],
    inventory: FakeInventory,
) -> AsyncIterator[OrderService]:
    async with session_factory() as session:
        yield OrderService(
            session=session,
            repository=SqlOrderRepository(session),
            inventory=inventory,
            pricing=NullPricing(),
            currency="RUB",
            stale_after_seconds=STALE_AFTER,
        )


LINES = (OrderLine(PRODUCT_A, WAREHOUSE, 2),)


async def test_successful_reservation_moves_the_order_to_reserved(
    service: OrderService,
    inventory: FakeInventory,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()

    result = await service.create_order(
        user_id=user_id,
        lines=LINES,
        idempotency_key="key-success",
    )

    assert result.created
    assert not result.reservation_pending
    assert result.record.order.status == OrderStatus.RESERVED
    assert result.record.order.inventory_reservation_id == RESERVATION_ID
    assert result.record.order.reservation_started_at is None
    assert [item.quantity for item in result.record.items] == [2]

    rows = await history_of(session_factory, result.record.order.id)
    assert [(row.from_status, row.to_status) for row in rows] == [
        (None, OrderStatus.PENDING),
        (OrderStatus.PENDING, OrderStatus.RESERVED),
    ]
    assert inventory.calls[0][0] == result.record.order.id


async def test_duplicate_positions_are_collapsed_before_the_call(
    service: OrderService,
    inventory: FakeInventory,
) -> None:
    """Two identical positions would violate the unique index on order_items."""
    result = await service.create_order(
        user_id=uuid.uuid4(),
        lines=(
            OrderLine(PRODUCT_A, WAREHOUSE, 1),
            OrderLine(PRODUCT_A, WAREHOUSE, 2),
            OrderLine(PRODUCT_B, WAREHOUSE, 1),
        ),
    )

    assert len(result.record.items) == 2
    quantities = {item.product_id: item.quantity for item in result.record.items}
    assert quantities == {PRODUCT_A: 3, PRODUCT_B: 1}
    assert inventory.calls[0][1] == (
        OrderLine(PRODUCT_A, WAREHOUSE, 3),
        OrderLine(PRODUCT_B, WAREHOUSE, 1),
    )


async def test_the_order_is_committed_before_inventory_is_called(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Order-first: the row must be visible to another connection mid-saga."""
    observed: list[OrderStatus] = []

    async def probe(order_id: uuid.UUID) -> None:
        # A separate session, so an uncommitted TX1 would be invisible here.
        async with session_factory() as other:
            status = await other.scalar(
                select(Order.status).where(Order.id == order_id)
            )
        observed.append(status)

    inventory = FakeInventory(before_return=probe)
    async with session_factory() as session:
        service = OrderService(
            session=session,
            repository=SqlOrderRepository(session),
            inventory=inventory,
            pricing=NullPricing(),
            currency="RUB",
            stale_after_seconds=STALE_AFTER,
        )
        await service.create_order(user_id=uuid.uuid4(), lines=LINES)

    assert observed == [OrderStatus.PENDING]


@pytest.mark.parametrize(
    ("outcome", "error", "reason"),
    [
        (
            ReservationOutcome.REJECTED,
            InsufficientStockError,
            OrderReason.INSUFFICIENT_STOCK,
        ),
        (
            ReservationOutcome.ITEM_NOT_FOUND,
            InventoryItemNotFoundError,
            OrderReason.INVENTORY_ITEM_NOT_FOUND,
        ),
        (
            ReservationOutcome.INVALID_REQUEST,
            InventoryRejectedError,
            OrderReason.INVENTORY_REJECTED,
        ),
    ],
)
async def test_terminal_refusal_fails_the_order_with_a_reason(
    outcome: ReservationOutcome,
    error: type[Exception],
    reason: OrderReason,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        service = OrderService(
            session=session,
            repository=SqlOrderRepository(session),
            inventory=FakeInventory(ReservationResult(outcome)),
            pricing=NullPricing(),
            currency="RUB",
            stale_after_seconds=STALE_AFTER,
        )

        with pytest.raises(error) as excinfo:
            await service.create_order(user_id=uuid.uuid4(), lines=LINES)

    order_id = excinfo.value.order_id
    async with session_factory() as session:
        order = await session.get(Order, order_id)
        assert order is not None
        assert order.status == OrderStatus.FAILED
        assert order.reservation_started_at is None

    rows = await history_of(session_factory, order_id)
    assert rows[-1].to_status == OrderStatus.FAILED
    assert rows[-1].reason == reason.value


async def test_unknown_outcome_keeps_the_order_pending(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Nothing is written in TX2: `pending` is the honest state."""
    async with session_factory() as session:
        service = OrderService(
            session=session,
            repository=SqlOrderRepository(session),
            inventory=FakeInventory(ReservationResult(ReservationOutcome.UNAVAILABLE)),
            pricing=NullPricing(),
            currency="RUB",
            stale_after_seconds=STALE_AFTER,
        )

        result = await service.create_order(user_id=uuid.uuid4(), lines=LINES)

        assert result.reservation_pending
        assert result.record.order.status == OrderStatus.PENDING
        # The lease stays fresh, which is what blocks cancellation.
        assert result.record.order.reservation_started_at is not None


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


async def test_replay_returns_the_original_order_without_calling_inventory_again(
    service: OrderService,
    inventory: FakeInventory,
) -> None:
    user_id = uuid.uuid4()

    first = await service.create_order(
        user_id=user_id,
        lines=LINES,
        idempotency_key="key-replay",
    )
    second = await service.create_order(
        user_id=user_id,
        lines=LINES,
        idempotency_key="key-replay",
    )

    assert first.created
    assert not second.created
    assert second.record.order.id == first.record.order.id
    assert len(inventory.calls) == 1


async def test_same_key_with_a_different_body_is_a_conflict(
    service: OrderService,
    inventory: FakeInventory,
) -> None:
    user_id = uuid.uuid4()
    await service.create_order(
        user_id=user_id,
        lines=LINES,
        idempotency_key="key-conflict",
    )

    with pytest.raises(IdempotencyConflictError):
        await service.create_order(
            user_id=user_id,
            lines=(OrderLine(PRODUCT_A, WAREHOUSE, 5),),
            idempotency_key="key-conflict",
        )

    assert len(inventory.calls) == 1


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


async def test_cancel_is_blocked_while_the_lease_is_fresh_and_allowed_after_it(
    service: OrderService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    created = await insert_order(session_factory, user_id=user_id)

    with pytest.raises(OrderNotCancellableError):
        await service.cancel_order(order_id=created.order.id, user_id=user_id)

    await age_lease(
        session_factory,
        order_id=created.order.id,
        seconds=STALE_AFTER * 10,
    )

    cancelled = await service.cancel_order(
        order_id=created.order.id,
        user_id=user_id,
    )

    assert cancelled.order.status == OrderStatus.CANCELLED
    rows = await history_of(session_factory, created.order.id)
    assert rows[-1].reason == OrderReason.USER_REQUESTED.value


async def test_cancel_of_a_foreign_order_is_not_found(
    service: OrderService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    created = await insert_order(session_factory, user_id=uuid.uuid4())

    with pytest.raises(OrderNotFoundError):
        await service.cancel_order(
            order_id=created.order.id,
            user_id=uuid.uuid4(),
        )


# ---------------------------------------------------------------------------
# Reads and the cancel-versus-reserve race
# ---------------------------------------------------------------------------


async def test_get_order_hides_foreign_orders(
    service: OrderService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    created = await insert_order(session_factory, user_id=uuid.uuid4())

    with pytest.raises(OrderNotFoundError):
        await service.get_order(order_id=created.order.id, user_id=uuid.uuid4())


async def test_list_orders_pages_with_an_opaque_cursor(
    service: OrderService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    base = datetime(2026, 1, 1, tzinfo=UTC)
    created_ids: list[uuid.UUID] = []

    for index in range(3):
        created = await insert_order(session_factory, user_id=user_id)
        await set_created_at(
            session_factory,
            order_id=created.order.id,
            moment=base + timedelta(minutes=index),
        )
        created_ids.append(created.order.id)

    first_page, cursor = await service.list_orders(user_id=user_id, limit=2)

    assert [record.order.id for record in first_page] == [
        created_ids[2],
        created_ids[1],
    ]
    assert cursor is not None

    second_page, next_cursor = await service.list_orders(
        user_id=user_id,
        limit=2,
        before=OrderCursor(created_at=cursor.created_at, order_id=cursor.order_id),
    )

    assert [record.order.id for record in second_page] == [created_ids[0]]
    assert next_cursor is None


async def test_a_cancel_that_wins_the_race_is_not_overwritten(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Inventory reserved, but the user cancelled in between: stay cancelled."""
    user_id = uuid.uuid4()

    async def cancel_during_reserve(order_id: uuid.UUID) -> None:
        await age_lease(
            session_factory,
            order_id=order_id,
            seconds=STALE_AFTER * 10,
        )
        async with session_factory() as other:
            result = await SqlOrderRepository(other).cancel(
                order_id=order_id,
                user_id=user_id,
                stale_after_seconds=STALE_AFTER,
                reason=OrderReason.USER_REQUESTED,
            )
            await other.commit()
        assert result.status is CancelStatus.CANCELLED

    async with session_factory() as session:
        service = OrderService(
            session=session,
            repository=SqlOrderRepository(session),
            inventory=FakeInventory(before_return=cancel_during_reserve),
            pricing=NullPricing(),
            currency="RUB",
            stale_after_seconds=STALE_AFTER,
        )

        result = await service.create_order(user_id=user_id, lines=LINES)

    # The saga lost the compare-and-set, so it must report reality instead of
    # pushing the order back to `reserved`.
    assert result.record.order.status == OrderStatus.CANCELLED
    assert result.record.order.inventory_reservation_id is None
