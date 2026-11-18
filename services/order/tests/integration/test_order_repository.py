"""Repository tests against real PostgreSQL.

Fakes cannot prove `ON CONFLICT DO NOTHING`, row comparison in keyset
pagination, or that a conditional `UPDATE` loses the race on purpose — those are
database semantics, so they are tested on a database.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.order import OrderLine, OrderReason
from app.models.order import OrderStatus
from app.repositories.order import (
    CancelStatus,
    OrderCursor,
    SqlOrderRepository,
)
from tests.integration.helpers import (
    PRODUCT_A,
    PRODUCT_B,
    WAREHOUSE,
    age_lease,
    build_order,
    count_orders,
    history_of,
    insert_order,
    set_created_at,
)

STALE_AFTER = 30.0


@pytest.fixture()
async def session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


@pytest.fixture()
def repository(session: AsyncSession) -> SqlOrderRepository:
    return SqlOrderRepository(session)


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------


async def test_insert_persists_order_items_and_first_history_row(
    repository: SqlOrderRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    order, items, history = build_order(user_id=user_id)

    assert await repository.insert(order=order, items=items, history=history)
    await session.commit()

    stored = await repository.get(order_id=order.id, user_id=user_id)

    assert stored is not None
    assert stored.order.status == OrderStatus.PENDING
    # The lease and both timestamps come from server-side defaults.
    assert stored.order.reservation_started_at is not None
    assert stored.order.created_at is not None
    assert [item.quantity for item in stored.items] == [2]

    rows = await history_of(session_factory, order.id)
    assert [(row.from_status, row.to_status, row.reason) for row in rows] == [
        (None, OrderStatus.PENDING, None)
    ]


async def test_duplicate_idempotency_key_does_not_insert_and_keeps_the_session_usable(
    repository: SqlOrderRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    first = await insert_order(
        session_factory, user_id=user_id, idempotency_key="key-1"
    )

    order, items, history = build_order(user_id=user_id, idempotency_key="key-1")
    assert not await repository.insert(order=order, items=items, history=history)

    # If the statement had raised, this read would fail with
    # "current transaction is aborted" — that is the point of DO NOTHING.
    existing = await repository.get_by_idempotency_key(
        user_id=user_id,
        idempotency_key="key-1",
    )
    await session.commit()

    assert existing is not None
    assert existing.order.id == first.order.id
    assert await count_orders(session_factory) == 1


async def test_the_same_key_is_scoped_per_user(
    repository: SqlOrderRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await insert_order(session_factory, user_id=uuid.uuid4(), idempotency_key="shared")

    order, items, history = build_order(
        user_id=uuid.uuid4(),
        idempotency_key="shared",
    )
    assert await repository.insert(order=order, items=items, history=history)
    await session.commit()

    assert await count_orders(session_factory) == 2


async def test_orders_without_a_key_never_conflict(
    repository: SqlOrderRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    for _ in range(2):
        order, items, history = build_order(user_id=user_id)
        assert await repository.insert(order=order, items=items, history=history)
    await session.commit()

    assert await count_orders(session_factory) == 2


async def test_get_hides_foreign_orders(
    repository: SqlOrderRepository,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner_id = uuid.uuid4()
    created = await insert_order(session_factory, user_id=owner_id)

    assert await repository.get(order_id=created.order.id, user_id=uuid.uuid4()) is None
    assert await repository.get(order_id=created.order.id, user_id=owner_id) is not None


async def test_get_by_idempotency_key_is_scoped_per_user(
    repository: SqlOrderRepository,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await insert_order(session_factory, user_id=uuid.uuid4(), idempotency_key="key-2")

    assert (
        await repository.get_by_idempotency_key(
            user_id=uuid.uuid4(),
            idempotency_key="key-2",
        )
        is None
    )


# ---------------------------------------------------------------------------
# Transitions
# ---------------------------------------------------------------------------


async def test_transition_moves_the_order_and_records_the_reason(
    repository: SqlOrderRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    created = await insert_order(session_factory, user_id=user_id)
    reservation_id = uuid.uuid4()

    updated = await repository.transition(
        order_id=created.order.id,
        from_status=OrderStatus.PENDING,
        to_status=OrderStatus.RESERVED,
        inventory_reservation_id=reservation_id,
        clear_lease=True,
    )
    await session.commit()

    assert updated is not None
    assert updated.order.status == OrderStatus.RESERVED
    assert updated.order.inventory_reservation_id == reservation_id
    assert updated.order.reservation_started_at is None

    rows = await history_of(session_factory, created.order.id)
    assert [(row.from_status, row.to_status, row.reason) for row in rows] == [
        (None, OrderStatus.PENDING, None),
        (OrderStatus.PENDING, OrderStatus.RESERVED, None),
    ]


async def test_transition_from_an_unexpected_status_changes_nothing(
    repository: SqlOrderRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    created = await insert_order(session_factory, user_id=uuid.uuid4())

    updated = await repository.transition(
        order_id=created.order.id,
        from_status=OrderStatus.RESERVED,
        to_status=OrderStatus.FAILED,
        reason=OrderReason.INSUFFICIENT_STOCK,
    )
    await session.commit()

    assert updated is None
    stored = await repository.get(
        order_id=created.order.id,
        user_id=created.order.user_id,
    )
    assert stored is not None
    assert stored.order.status == OrderStatus.PENDING


# ---------------------------------------------------------------------------
# Cancellation and the reservation lease
# ---------------------------------------------------------------------------


async def test_cancel_succeeds_once_the_lease_is_stale(
    repository: SqlOrderRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    created = await insert_order(session_factory, user_id=user_id)
    await age_lease(
        session_factory,
        order_id=created.order.id,
        seconds=STALE_AFTER * 10,
    )

    result = await repository.cancel(
        order_id=created.order.id,
        user_id=user_id,
        stale_after_seconds=STALE_AFTER,
        reason=OrderReason.USER_REQUESTED,
    )
    await session.commit()

    assert result.status is CancelStatus.CANCELLED
    assert result.record is not None
    assert result.record.order.status == OrderStatus.CANCELLED
    assert result.record.order.reservation_started_at is None

    rows = await history_of(session_factory, created.order.id)
    assert rows[-1].to_status == OrderStatus.CANCELLED
    assert rows[-1].reason == OrderReason.USER_REQUESTED.value


async def test_cancel_is_refused_while_the_lease_is_fresh(
    repository: SqlOrderRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A fresh lease means the reserve call may still be running."""
    user_id = uuid.uuid4()
    created = await insert_order(session_factory, user_id=user_id)

    result = await repository.cancel(
        order_id=created.order.id,
        user_id=user_id,
        stale_after_seconds=STALE_AFTER,
        reason=OrderReason.USER_REQUESTED,
    )
    await session.commit()

    assert result.status is CancelStatus.NOT_CANCELLABLE
    assert result.record is None

    stored = await repository.get(
        order_id=created.order.id,
        user_id=user_id,
    )
    assert stored is not None
    assert stored.order.status == OrderStatus.PENDING


async def test_cancel_is_refused_for_a_reserved_order(
    repository: SqlOrderRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Inventory cannot release stock, so a reserved order stays reserved."""
    user_id = uuid.uuid4()
    created = await insert_order(session_factory, user_id=user_id)
    await repository.transition(
        order_id=created.order.id,
        from_status=OrderStatus.PENDING,
        to_status=OrderStatus.RESERVED,
        inventory_reservation_id=uuid.uuid4(),
        clear_lease=True,
    )
    await session.commit()

    result = await repository.cancel(
        order_id=created.order.id,
        user_id=user_id,
        stale_after_seconds=STALE_AFTER,
        reason=OrderReason.USER_REQUESTED,
    )

    assert result.status is CancelStatus.NOT_CANCELLABLE


async def test_cancel_of_a_foreign_order_is_not_found(
    repository: SqlOrderRepository,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    created = await insert_order(session_factory, user_id=uuid.uuid4())

    result = await repository.cancel(
        order_id=created.order.id,
        user_id=uuid.uuid4(),
        stale_after_seconds=STALE_AFTER,
        reason=OrderReason.USER_REQUESTED,
    )

    # 404 rather than 403: the caller must not learn that the order exists.
    assert result.status is CancelStatus.NOT_FOUND


async def test_cancel_of_an_unknown_order_is_not_found(
    repository: SqlOrderRepository,
) -> None:
    result = await repository.cancel(
        order_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        stale_after_seconds=STALE_AFTER,
        reason=OrderReason.USER_REQUESTED,
    )

    assert result.status is CancelStatus.NOT_FOUND


async def test_cancel_twice_is_not_cancellable(
    repository: SqlOrderRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    created = await insert_order(session_factory, user_id=user_id)
    await age_lease(
        session_factory,
        order_id=created.order.id,
        seconds=STALE_AFTER * 10,
    )

    first = await repository.cancel(
        order_id=created.order.id,
        user_id=user_id,
        stale_after_seconds=STALE_AFTER,
        reason=OrderReason.USER_REQUESTED,
    )
    await session.commit()
    second = await repository.cancel(
        order_id=created.order.id,
        user_id=user_id,
        stale_after_seconds=STALE_AFTER,
        reason=OrderReason.USER_REQUESTED,
    )

    assert first.status is CancelStatus.CANCELLED
    assert second.status is CancelStatus.NOT_CANCELLABLE


# ---------------------------------------------------------------------------
# Keyset pagination
# ---------------------------------------------------------------------------


async def test_list_returns_newest_first_with_items(
    repository: SqlOrderRepository,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    older = await insert_order(session_factory, user_id=user_id)
    newer = await insert_order(
        session_factory,
        user_id=user_id,
        lines=(
            OrderLine(PRODUCT_A, WAREHOUSE, 1),
            OrderLine(PRODUCT_B, WAREHOUSE, 3),
        ),
    )
    base = datetime(2026, 1, 1, tzinfo=UTC)
    await set_created_at(session_factory, order_id=older.order.id, moment=base)
    await set_created_at(
        session_factory,
        order_id=newer.order.id,
        moment=base + timedelta(minutes=1),
    )

    records = await repository.list_for_user(user_id=user_id, limit=10)

    assert [record.order.id for record in records] == [newer.order.id, older.order.id]
    # Positions come back sorted, and only for their own order.
    assert [item.product_id for item in records[0].items] == [PRODUCT_A, PRODUCT_B]
    assert len(records[1].items) == 1


async def test_keyset_pagination_neither_repeats_nor_skips_rows(
    repository: SqlOrderRepository,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    base = datetime(2026, 1, 1, tzinfo=UTC)
    order_ids: list[uuid.UUID] = []

    for index in range(3):
        created = await insert_order(session_factory, user_id=user_id)
        await set_created_at(
            session_factory,
            order_id=created.order.id,
            moment=base + timedelta(minutes=index),
        )
        order_ids.append(created.order.id)

    first_page = await repository.list_for_user(user_id=user_id, limit=2)
    assert [record.order.id for record in first_page] == [order_ids[2], order_ids[1]]

    last = first_page[-1]
    second_page = await repository.list_for_user(
        user_id=user_id,
        limit=2,
        before=OrderCursor(
            created_at=last.order.created_at,
            order_id=last.order.id,
        ),
    )

    # Only the oldest order is left: the cursor is exclusive.
    assert [record.order.id for record in second_page] == [order_ids[0]]


async def test_keyset_handles_equal_timestamps_by_comparing_ids(
    repository: SqlOrderRepository,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """`created_at` alone is not unique: the id is the tie-breaker."""
    user_id = uuid.uuid4()
    moment = datetime(2026, 1, 1, tzinfo=UTC)

    created_ids = []
    for _ in range(2):
        created = await insert_order(session_factory, user_id=user_id)
        await set_created_at(session_factory, order_id=created.order.id, moment=moment)
        created_ids.append(created.order.id)

    first_page = await repository.list_for_user(user_id=user_id, limit=1)
    assert len(first_page) == 1

    last = first_page[0]
    second_page = await repository.list_for_user(
        user_id=user_id,
        limit=1,
        before=OrderCursor(
            created_at=last.order.created_at,
            order_id=last.order.id,
        ),
    )

    assert len(second_page) == 1
    assert second_page[0].order.id != last.order.id
    assert {record.order.id for record in first_page + second_page} == set(created_ids)


async def test_list_is_scoped_to_the_owner(
    repository: SqlOrderRepository,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner_id = uuid.uuid4()
    await insert_order(session_factory, user_id=owner_id)
    await insert_order(session_factory, user_id=uuid.uuid4())

    records = await repository.list_for_user(user_id=owner_id, limit=10)

    assert len(records) == 1
    assert records[0].order.user_id == owner_id
