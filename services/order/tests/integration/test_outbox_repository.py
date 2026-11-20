"""Outbox storage semantics against real PostgreSQL.

The interesting cases are all database semantics that a fake cannot reproduce:
`FOR UPDATE SKIP LOCKED` handing a row to exactly one claimer, the attempt
counter moving *before* the publish call, and a compare-and-set mark that loses
the race on purpose. Everything here is the storage half of the at-least-once
contract; the relay tests build on top of it.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.outbox_event import OutboxEvent
from app.repositories.outbox import SqlOutboxRepository
from tests.integration.helpers import (
    age_outbox_lease,
    insert_outbox_event,
    outbox_rows,
)

LEASE_SECONDS = 30.0
MAX_ATTEMPTS = 3


@pytest.fixture()
async def session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


@pytest.fixture()
def repository(session: AsyncSession) -> SqlOutboxRepository:
    return SqlOutboxRepository(session)


def build_event() -> OutboxEvent:
    aggregate_id = uuid.uuid4()
    return OutboxEvent(
        id=uuid.uuid4(),
        aggregate_id=aggregate_id,
        event_type="order.reserved",
        payload={"order_id": str(aggregate_id)},
    )


# ---------------------------------------------------------------------------
# Enqueue: the caller owns the transaction
# ---------------------------------------------------------------------------


async def test_enqueue_does_not_survive_a_rollback(
    repository: SqlOutboxRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await repository.enqueue(event=build_event())
    await session.rollback()

    # This is the whole point of the pattern: the event exists only if the fact
    # it describes was committed.
    assert await outbox_rows(session_factory) == []


async def test_a_committed_event_is_unpublished_with_a_full_budget(
    repository: SqlOutboxRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event = build_event()

    await repository.enqueue(event=event)
    await session.commit()

    stored = (await outbox_rows(session_factory))[0]
    assert stored.id == event.id
    assert stored.payload == {"order_id": str(event.aggregate_id)}
    # `occurred_at` and `attempts` come from server-side defaults.
    assert stored.occurred_at is not None
    assert stored.attempts == 0
    assert stored.published_at is None
    assert stored.locked_at is None


# ---------------------------------------------------------------------------
# Claim: lease + attempt budget
# ---------------------------------------------------------------------------


async def test_claim_leases_the_row_and_spends_an_attempt(
    repository: SqlOutboxRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_outbox_event(session_factory)

    claimed = await repository.claim_batch(
        limit=10,
        lease_seconds=LEASE_SECONDS,
        max_attempts=MAX_ATTEMPTS,
    )
    await session.commit()

    assert [event.id for event in claimed] == [event_id]
    # The attempt is spent *before* the publish call, so a crash mid-publish
    # still costs budget and cannot loop forever.
    assert claimed[0].attempts == 1
    assert claimed[0].locked_at is not None

    stored = (await outbox_rows(session_factory))[0]
    assert stored.attempts == 1
    assert stored.locked_at is not None
    assert stored.published_at is None


async def test_a_fresh_lease_is_not_claimed_twice(
    repository: SqlOutboxRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await insert_outbox_event(session_factory)

    first = await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )
    await session.commit()
    # A second relay starting while the first is still publishing must not pick
    # the same row: a live lease means "someone is working on it".
    second = await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )

    assert len(first) == 1
    assert second == []


async def test_an_expired_lease_is_claimed_again(
    repository: SqlOutboxRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_outbox_event(session_factory)
    await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )
    await session.commit()

    # The publisher never came back: only the lease expiry makes the row
    # claimable again, and that expiry is also the retry backoff.
    await age_outbox_lease(
        session_factory, event_id=event_id, seconds=LEASE_SECONDS + 1
    )
    retried = await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )

    assert [event.id for event in retried] == [event_id]
    assert retried[0].attempts == 2


async def test_claim_returns_oldest_first_and_respects_the_limit(
    repository: SqlOutboxRepository,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    base = datetime.now(UTC) - timedelta(hours=1)
    ids = [
        await insert_outbox_event(
            session_factory, occurred_at=base + timedelta(seconds=position)
        )
        for position in range(3)
    ]

    claimed = await repository.claim_batch(
        limit=2, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )

    assert [event.id for event in claimed] == ids[:2]


async def test_claim_ignores_published_events(
    repository: SqlOutboxRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_outbox_event(session_factory)

    claimed = await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )
    assert await repository.mark_published(event_id=claimed[0].id)
    await session.commit()

    feed = await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )
    assert event_id not in {event.id for event in feed}


async def test_claim_ignores_events_that_spent_their_budget(
    repository: SqlOutboxRepository,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await insert_outbox_event(session_factory, attempts=MAX_ATTEMPTS)

    claimed = await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )
    assert claimed == []


async def test_the_last_allowed_attempt_is_spent_exactly_once(
    repository: SqlOutboxRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_outbox_event(session_factory, attempts=MAX_ATTEMPTS - 1)

    claimed = await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )
    await session.commit()

    assert [event.id for event in claimed] == [event_id]
    assert claimed[0].attempts == MAX_ATTEMPTS

    # Reaching the cap is terminal: the next pass must not publish a 4th time.
    await age_outbox_lease(
        session_factory, event_id=event_id, seconds=LEASE_SECONDS + 1
    )
    again = await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )

    assert again == []
    assert await repository.count_exhausted_unpublished(max_attempts=MAX_ATTEMPTS) == 1


async def test_two_claimers_never_share_a_row(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await insert_outbox_event(session_factory)

    # Both sessions hold an open transaction, exactly like two relay processes
    # racing for the same feed. SKIP LOCKED is what makes the loser move on
    # instead of blocking behind the winner.
    async with session_factory() as first_session, session_factory() as second_session:
        first = await SqlOutboxRepository(first_session).claim_batch(
            limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
        )
        second = await SqlOutboxRepository(second_session).claim_batch(
            limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
        )
        await first_session.rollback()
        await second_session.rollback()

    assert len(first) == 1
    assert second == []


# ---------------------------------------------------------------------------
# Mark published: compare-and-set
# ---------------------------------------------------------------------------


async def test_mark_published_is_a_compare_and_set(
    repository: SqlOutboxRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_outbox_event(session_factory)
    claimed = await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS
    )

    assert await repository.mark_published(event_id=claimed[0].id)
    await session.commit()

    # A duplicate delivery is detected exactly here: the second mark reports
    # `False`, so the relay can log "already published" instead of pretending it
    # was the one that did it.
    assert not await repository.mark_published(event_id=event_id)
    stored = (await outbox_rows(session_factory))[0]
    assert stored.published_at is not None


async def test_count_exhausted_unpublished_counts_only_the_stuck_rows(
    repository: SqlOutboxRepository,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await insert_outbox_event(session_factory, attempts=MAX_ATTEMPTS)
    await insert_outbox_event(session_factory, attempts=MAX_ATTEMPTS + 5)
    await insert_outbox_event(session_factory)
    published_and_stuck = await insert_outbox_event(
        session_factory, attempts=MAX_ATTEMPTS
    )

    claimed = await repository.claim_batch(
        limit=10, lease_seconds=LEASE_SECONDS, max_attempts=MAX_ATTEMPTS + 10
    )
    assert published_and_stuck in {event.id for event in claimed}
    assert await repository.mark_published(event_id=published_and_stuck)
    await session.commit()

    assert await repository.count_exhausted_unpublished(max_attempts=MAX_ATTEMPTS) == 2
