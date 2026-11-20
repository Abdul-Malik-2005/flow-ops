"""Helpers for integration tests: build order rows and move time around.

Two things are surprisingly hard to test without touching the database directly:
"the lease is stale" and "these orders were created in this order". Both are
expressed with SQL intervals and explicit timestamps here, so tests stay
deterministic without sleeping.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.order import OrderLine, canonical_request_hash, normalize_lines
from app.models.order import Order, OrderStatus
from app.models.order_item import OrderItem
from app.models.order_status_history import OrderStatusHistory
from app.models.outbox_event import OutboxEvent
from app.repositories.order import OrderRecord, SqlOrderRepository
from app.repositories.outbox import SqlOutboxRepository

SERVICE_ROOT = Path(__file__).resolve().parents[2]
TEST_DATABASE_SUFFIX = "_test_db"

PRODUCT_A = uuid.UUID("11111111-1111-4111-8111-111111111111")
PRODUCT_B = uuid.UUID("22222222-2222-4222-8222-222222222222")
WAREHOUSE = uuid.UUID("33333333-3333-4333-8333-333333333333")

DEFAULT_LINES = (OrderLine(PRODUCT_A, WAREHOUSE, 2),)


def run_alembic(database_url: str, *args: str) -> None:
    """Run Alembic in a subprocess.

    `migrations/env.py` calls `asyncio.run()`, which cannot nest inside the
    running pytest event loop. A subprocess also mirrors how the container
    applies migrations on start-up.
    """
    env = {**os.environ, "DATABASE_URL": database_url}
    completed = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=SERVICE_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"alembic {' '.join(args)} failed with exit code "
            f"{completed.returncode}\nstdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )


def build_order(
    *,
    user_id: uuid.UUID,
    idempotency_key: str | None = None,
    currency: str = "RUB",
    lines: tuple[OrderLine, ...] = DEFAULT_LINES,
) -> tuple[Order, list[OrderItem], OrderStatusHistory]:
    """Assemble the aggregate exactly as the service would.

    Goes through `normalize_lines`, so duplicate positions are collapsed here
    too and the tests cannot accidentally prove behaviour production never has.
    """
    normalized = normalize_lines(lines)
    request_hash = canonical_request_hash(normalized, currency=currency)
    order = Order(
        id=uuid.uuid4(),
        user_id=user_id,
        status=OrderStatus.PENDING,
        currency=currency,
        total_amount=None,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )
    items = [
        OrderItem(
            order_id=order.id,
            product_id=line.product_id,
            warehouse_id=line.warehouse_id,
            quantity=line.quantity,
        )
        for line in normalized
    ]
    history = OrderStatusHistory(
        order_id=order.id,
        from_status=None,
        to_status=OrderStatus.PENDING,
        reason=None,
    )
    return order, items, history


async def insert_order(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    user_id: uuid.UUID,
    idempotency_key: str | None = None,
    lines: tuple[OrderLine, ...] = DEFAULT_LINES,
) -> OrderRecord:
    """Create an order through the repository and commit it."""
    order, items, history = build_order(
        user_id=user_id,
        idempotency_key=idempotency_key,
        lines=lines,
    )
    async with session_factory() as session:
        repository = SqlOrderRepository(session)
        inserted = await repository.insert(order=order, items=items, history=history)
        assert inserted
        await session.commit()
        return OrderRecord(order=order, items=items)


async def age_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    order_id: uuid.UUID,
    seconds: float,
) -> None:
    """Move `reservation_started_at` into the past by `seconds`.

    Uses the database clock, so the staleness check under test is the same one
    production runs.
    """
    async with session_factory() as session:
        await session.execute(
            update(Order)
            .where(Order.id == order_id)
            .values(
                reservation_started_at=func.now()
                - text("make_interval(secs => :seconds)").bindparams(seconds=seconds)
            )
        )
        await session.commit()


async def clear_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    order_id: uuid.UUID,
) -> None:
    """Drop the lease marker: the state of an order no worker ever touched.

    `NULL` is what the claim predicate treats as "definitely no call in flight",
    which is a different case from "the call was lost long ago" (`age_lease`).
    """
    async with session_factory() as session:
        await session.execute(
            update(Order)
            .where(Order.id == order_id)
            .values(reservation_started_at=None)
        )
        await session.commit()


async def set_reservation_attempts(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    order_id: uuid.UUID,
    attempts: int,
) -> None:
    """Pin the attempt counter, so the budget cap can be reached in one pass."""
    async with session_factory() as session:
        await session.execute(
            update(Order)
            .where(Order.id == order_id)
            .values(reservation_attempts=attempts)
        )
        await session.commit()


async def set_created_at(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    order_id: uuid.UUID,
    moment: datetime,
) -> None:
    async with session_factory() as session:
        await session.execute(
            update(Order).where(Order.id == order_id).values(created_at=moment)
        )
        await session.commit()


async def history_of(
    session_factory: async_sessionmaker[AsyncSession],
    order_id: uuid.UUID,
) -> list[OrderStatusHistory]:
    """Status history in insertion order."""
    async with session_factory() as session:
        rows = await session.scalars(
            select(OrderStatusHistory)
            .where(OrderStatusHistory.order_id == order_id)
            .order_by(OrderStatusHistory.created_at, OrderStatusHistory.id)
        )
        return list(rows)


async def count_orders(session_factory: async_sessionmaker[AsyncSession]) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(Order)) or 0


async def insert_outbox_event(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    event_type: str = "order.reserved",
    aggregate_id: uuid.UUID | None = None,
    payload: dict[str, object] | None = None,
    attempts: int | None = None,
    occurred_at: datetime | None = None,
) -> uuid.UUID:
    """Write one outbox row outside the service, already committed.

    Bypasses the payload builders on purpose: these tests are about storage
    semantics, and a hand-built row proves the repository does not depend on the
    shape of the domain event.
    """
    event_id = uuid.uuid4()
    aggregate = aggregate_id or uuid.uuid4()
    event = OutboxEvent(
        id=event_id,
        aggregate_id=aggregate,
        event_type=event_type,
        payload=payload if payload is not None else {"order_id": str(aggregate)},
    )
    if attempts is not None:
        event.attempts = attempts
    if occurred_at is not None:
        event.occurred_at = occurred_at

    async with session_factory() as session:
        await SqlOutboxRepository(session).enqueue(event=event)
        await session.commit()

    return event_id


async def age_outbox_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    event_id: uuid.UUID,
    seconds: float,
) -> None:
    """Move `locked_at` into the past: a lease the publisher never came back from.

    Uses the database clock, so the staleness predicate under test is exactly the
    one the relay runs.
    """
    async with session_factory() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                locked_at=func.now()
                - text("make_interval(secs => :seconds)").bindparams(seconds=seconds)
            )
        )
        await session.commit()


async def outbox_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[OutboxEvent]:
    """Every outbox row, oldest first — the way a consumer would see them."""
    async with session_factory() as session:
        rows = await session.scalars(
            select(OutboxEvent).order_by(OutboxEvent.occurred_at, OutboxEvent.id)
        )
        return list(rows)
