"""The inbox adapter against a real PostgreSQL: `ON CONFLICT DO NOTHING`.

The unit test proves the adapter never commits; this one proves the SQL the
deduplication relies on: a repeated `event_id` is a no-op, not an error, and one
row survives a replay. It runs the real migration and the real `RETURNING`.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.notification_inbox import NotificationInbox
from app.repositories.inbox import SqlInboxRepository
from tests.integration.helpers import (
    age_inbox_lease,
    inbox_rows,
    insert_inbox_event,
)

pytestmark = pytest.mark.integration

LEASE_SECONDS = 30.0
MAX_ATTEMPTS = 3

EVENT_ID = uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa")
ORDER_ID = uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb")
OCCURRED_AT = datetime(2026, 11, 19, 12, 30, tzinfo=UTC)


async def count_rows(session_factory: async_sessionmaker[AsyncSession]) -> int:
    async with session_factory() as session:
        return (
            await session.scalar(select(func.count()).select_from(NotificationInbox))
        ) or 0


async def read_row(
    session_factory: async_sessionmaker[AsyncSession],
    event_id: uuid.UUID,
) -> NotificationInbox:
    async with session_factory() as session:
        return await session.scalar(
            select(NotificationInbox).where(NotificationInbox.event_id == event_id)
        )


async def test_a_new_event_is_inserted_and_committed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        recorded = await SqlInboxRepository(session).record(
            event_id=EVENT_ID,
            aggregate_id=ORDER_ID,
            event_type="order.reserved",
            payload={"order_id": str(ORDER_ID), "status": "reserved"},
            occurred_at=OCCURRED_AT,
        )
        await session.commit()

    assert recorded is True
    row = await read_row(session_factory, EVENT_ID)
    assert row.aggregate_id == ORDER_ID
    assert row.event_type == "order.reserved"
    assert row.payload == {"order_id": str(ORDER_ID), "status": "reserved"}
    assert row.occurred_at == OCCURRED_AT


async def test_a_replay_is_a_no_op_with_one_row_surviving(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        first = await SqlInboxRepository(session).record(
            event_id=EVENT_ID,
            aggregate_id=ORDER_ID,
            event_type="order.reserved",
            payload={"order_id": str(ORDER_ID)},
            occurred_at=OCCURRED_AT,
        )
        await session.commit()

    async with session_factory() as session:
        second = await SqlInboxRepository(session).record(
            event_id=EVENT_ID,
            aggregate_id=ORDER_ID,
            event_type="order.reserved",
            payload={"order_id": str(ORDER_ID)},
            occurred_at=OCCURRED_AT,
        )
        await session.commit()

    # The second delivery of the same at-least-once message is a success that
    # wrote nothing: that is what lets the consumer ack a duplicate.
    assert first is True
    assert second is False
    assert await count_rows(session_factory) == 1


async def test_an_uncommitted_event_stores_a_null_timestamp(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = uuid.uuid4()

    async with session_factory() as session:
        recorded = await SqlInboxRepository(session).record(
            event_id=event_id,
            aggregate_id=uuid.uuid4(),
            event_type="order.cancelled",
            payload={"order_id": str(uuid.uuid4()), "reason": "user_requested"},
            occurred_at=None,
        )
        await session.commit()

    assert recorded is True
    assert (await read_row(session_factory, event_id)).occurred_at is None


async def test_received_at_is_stamped_by_the_database(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await SqlInboxRepository(session).record(
            event_id=EVENT_ID,
            aggregate_id=ORDER_ID,
            event_type="order.failed",
            payload={"order_id": str(ORDER_ID), "reason": "insufficient_stock"},
            occurred_at=None,
        )
        await session.commit()

    # A `server_default` proves the clock is the database's, not the producer's.
    assert (await read_row(session_factory, EVENT_ID)).received_at is not None


# ---------------------------------------------------------------------------
# Delivery state: claim / mark / count
# ---------------------------------------------------------------------------


async def test_claim_batch_leases_unsent_rows_and_spends_an_attempt(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_inbox_event(session_factory)

    async with session_factory() as session:
        claimed = await SqlInboxRepository(session).claim_batch(
            limit=10,
            lease_seconds=LEASE_SECONDS,
            max_attempts=MAX_ATTEMPTS,
        )
        await session.commit()

    assert [row.event_id for row in claimed] == [event_id]
    # The claim moves the lease and spends an attempt *before* the send call.
    assert claimed[0].attempts == 1
    assert claimed[0].locked_at is not None
    assert claimed[0].sent_at is None


async def test_claim_batch_skips_rows_already_sent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await insert_inbox_event(session_factory, sent_at=OCCURRED_AT)

    async with session_factory() as session:
        claimed = await SqlInboxRepository(session).claim_batch(
            limit=10,
            lease_seconds=LEASE_SECONDS,
            max_attempts=MAX_ATTEMPTS,
        )
        await session.commit()

    assert claimed == []


async def test_a_fresh_lease_hides_the_row_until_it_expires(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_inbox_event(session_factory)

    async with session_factory() as session:
        first = await SqlInboxRepository(session).claim_batch(
            limit=10,
            lease_seconds=LEASE_SECONDS,
            max_attempts=MAX_ATTEMPTS,
        )
        await session.commit()
    assert [row.event_id for row in first] == [event_id]

    # A live lease means "someone is delivering this row right now".
    async with session_factory() as session:
        again = await SqlInboxRepository(session).claim_batch(
            limit=10,
            lease_seconds=LEASE_SECONDS,
            max_attempts=MAX_ATTEMPTS,
        )
        await session.commit()
    assert again == []

    # Once the lease is stale the row is claimable, with a second attempt spent.
    await age_inbox_lease(session_factory, event_id=event_id, seconds=LEASE_SECONDS + 1)
    async with session_factory() as session:
        recovered = await SqlInboxRepository(session).claim_batch(
            limit=10,
            lease_seconds=LEASE_SECONDS,
            max_attempts=MAX_ATTEMPTS,
        )
        await session.commit()

    assert [row.event_id for row in recovered] == [event_id]
    assert recovered[0].attempts == 2


async def test_claim_batch_skips_rows_that_spent_their_budget(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await insert_inbox_event(session_factory, attempts=MAX_ATTEMPTS)

    async with session_factory() as session:
        inbox = SqlInboxRepository(session)
        claimed = await inbox.claim_batch(
            limit=10,
            lease_seconds=LEASE_SECONDS,
            max_attempts=MAX_ATTEMPTS,
        )
        exhausted = await inbox.count_exhausted_unsent(max_attempts=MAX_ATTEMPTS)
        await session.commit()

    # A spent budget is terminal for the feed, but the row is counted so a human
    # can be told about it.
    assert claimed == []
    assert exhausted == 1


async def test_mark_sent_is_a_compare_and_set(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_inbox_event(session_factory)

    async with session_factory() as session:
        first = await SqlInboxRepository(session).mark_sent(event_id=event_id)
        await session.commit()
    async with session_factory() as session:
        second = await SqlInboxRepository(session).mark_sent(event_id=event_id)
        await session.commit()

    # The first mark wins, the second reports the row was already delivered.
    assert first is True
    assert second is False
    [row] = await inbox_rows(session_factory)
    assert row.sent_at is not None
