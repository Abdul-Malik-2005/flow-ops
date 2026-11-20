"""Projection storage semantics against real PostgreSQL.

The journal is the consumer's receipt for a message: one row per `event_id`,
even when the broker delivers the same message again. A fake cannot prove that
`ON CONFLICT (event_id) DO NOTHING` turns a duplicate into a no-op instead of an
`IntegrityError`, so the interesting cases run here.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.order_event_projection import OrderEventProjection
from app.repositories.projection import SqlProjectionRepository


@pytest.fixture()
async def session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


@pytest.fixture()
def repository(session: AsyncSession) -> SqlProjectionRepository:
    return SqlProjectionRepository(session)


async def projection_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[OrderEventProjection]:
    """Every journal row, oldest first — the way a reader would see them."""
    async with session_factory() as session:
        rows = await session.scalars(
            select(OrderEventProjection).order_by(
                OrderEventProjection.received_at,
                OrderEventProjection.event_id,
            )
        )
        return list(rows)


async def test_record_writes_the_journal_row(
    repository: SqlProjectionRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = uuid.uuid4()
    aggregate_id = uuid.uuid4()
    occurred_at = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    payload = {"order_id": str(aggregate_id)}

    recorded = await repository.record(
        event_id=event_id,
        aggregate_id=aggregate_id,
        event_type="order.reserved",
        payload=payload,
        occurred_at=occurred_at,
    )
    await session.commit()

    assert recorded is True
    stored = (await projection_rows(session_factory))[0]
    assert stored.event_id == event_id
    assert stored.aggregate_id == aggregate_id
    assert stored.event_type == "order.reserved"
    assert stored.payload == payload
    assert stored.occurred_at == occurred_at
    # The receipt time comes from the server clock, not from the message.
    assert stored.received_at is not None


async def test_a_replay_is_a_no_op_and_never_rewrites_the_row(
    repository: SqlProjectionRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = uuid.uuid4()
    aggregate_id = uuid.uuid4()

    first = await repository.record(
        event_id=event_id,
        aggregate_id=aggregate_id,
        event_type="order.reserved",
        payload={"order_id": str(aggregate_id)},
        occurred_at=None,
    )
    await session.commit()

    duplicate = await repository.record(
        event_id=event_id,
        aggregate_id=aggregate_id,
        event_type="order.paid",
        payload={"order_id": str(aggregate_id), "reason": "replay"},
        occurred_at=None,
    )
    await session.commit()

    assert first is True
    assert duplicate is False

    rows = await projection_rows(session_factory)
    assert len(rows) == 1
    # The first writer wins: a replay must not mutate the journal, otherwise
    # "what did we consume" would depend on delivery order.
    assert rows[0].event_type == "order.reserved"
    assert rows[0].payload == {"order_id": str(aggregate_id)}
    # `occurred_at` is nullable because the contract allows `null`.
    assert rows[0].occurred_at is None


async def test_duplicate_from_another_session_is_also_a_no_op(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = uuid.uuid4()
    aggregate_id = uuid.uuid4()

    async with session_factory() as first:
        assert (
            await SqlProjectionRepository(first).record(
                event_id=event_id,
                aggregate_id=aggregate_id,
                event_type="order.reserved",
                payload={"order_id": str(aggregate_id)},
                occurred_at=None,
            )
            is True
        )
        await first.commit()

    # A separate transaction is what a second consumer (or a reconnect) looks
    # like: the conflict is resolved by PostgreSQL, not by in-process state.
    async with session_factory() as second:
        assert (
            await SqlProjectionRepository(second).record(
                event_id=event_id,
                aggregate_id=aggregate_id,
                event_type="order.reserved",
                payload={"order_id": str(aggregate_id)},
                occurred_at=None,
            )
            is False
        )
        await second.commit()

    assert len(await projection_rows(session_factory)) == 1
