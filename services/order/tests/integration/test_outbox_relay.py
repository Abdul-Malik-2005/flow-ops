"""The relay against real PostgreSQL: claim → publish → mark, end to end.

The storage half is covered by `tests/integration/test_outbox_repository.py` and
the loop by `tests/test_outbox_relay.py`; here the two meet, and only a real
database can prove the interesting cases:

* a saga-committed fact travels through the relay and is marked, and a second
  pass finds nothing to do;
* a failed publish writes nothing, so the row waits for the lease and only then
  is claimed again with a second attempt spent;
* a row that spent its budget is out of the feed and counted as a dead letter;
* two relays in sequence never deliver one row twice;
* a mark that loses the compare-and-set is reported as a duplicate instead of a
  success — the reason consumers must dedupe by `event_id`.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.inventory import ReservationOutcome, ReservationResult
from app.domain.order import OrderLine
from app.models.outbox_event import OutboxEvent
from app.repositories.order import SqlOrderRepository
from app.repositories.outbox import SqlOutboxRepository
from app.services.order import OrderService
from app.services.outbox_relay import EventPublisherPort, OutboxRelay, RelayReport
from app.services.pricing import NullPricing
from tests.integration.helpers import (
    PRODUCT_A,
    WAREHOUSE,
    age_outbox_lease,
    insert_outbox_event,
    outbox_rows,
)

BATCH_SIZE = 10
LEASE_SECONDS = 30.0
MAX_ATTEMPTS = 3
RESERVATION_ID = uuid.UUID("cccccccc-3333-4333-8333-cccccccccccc")
LINES = (OrderLine(PRODUCT_A, WAREHOUSE, 2),)


class FakeInventory:
    """Always reserves: the saga only needs a `reserved` answer to emit a fact."""

    async def reserve(
        self,
        *,
        order_id: uuid.UUID,
        lines: Sequence[OrderLine],
    ) -> ReservationResult:
        return ReservationResult(
            ReservationOutcome.RESERVED,
            reservation_id=RESERVATION_ID,
        )


class RecordingPublisher:
    """Records every delivery; `failing=True` models a transport that is down."""

    def __init__(self, *, failing: bool = False) -> None:
        self.failing = failing
        self.events: list[OutboxEvent] = []
        #: Every attempt, including the ones that raised.
        self.attempts: list[uuid.UUID] = []

    @property
    def delivered(self) -> list[uuid.UUID]:
        return [event.id for event in self.events]

    async def publish(self, *, event: OutboxEvent) -> None:
        self.attempts.append(event.id)
        if self.failing:
            raise RuntimeError("transport is down")
        self.events.append(event)

    async def aclose(self) -> None:
        """The relay may close its transport; this fake holds nothing."""


class MarkingPublisher:
    """A publisher that marks the row itself — another relay, finishing first.

    Used to force the compare-and-set in `mark_published` to lose, which is not
    reachable sequentially and would otherwise stay untested.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self.delivered: list[uuid.UUID] = []

    async def publish(self, *, event: OutboxEvent) -> None:
        self.delivered.append(event.id)
        async with self._session_factory() as session:
            marked = await SqlOutboxRepository(session).mark_published(
                event_id=event.id
            )
            assert marked
            await session.commit()

    async def aclose(self) -> None:
        """The relay may close its transport; this fake holds nothing."""


def build_relay(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: EventPublisherPort,
) -> OutboxRelay:
    """The entry point's wiring, with a recording transport."""
    return OutboxRelay(
        session_factory=session_factory,
        publisher=publisher,
        batch_size=BATCH_SIZE,
        lease_seconds=LEASE_SECONDS,
        max_attempts=MAX_ATTEMPTS,
    )


async def create_reserved_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> uuid.UUID:
    """Drive the saga, so the outbox row is written by production code."""
    async with session_factory() as session:
        created = await OrderService(
            session=session,
            repository=SqlOrderRepository(session),
            outbox=SqlOutboxRepository(session),
            inventory=FakeInventory(),
            pricing=NullPricing(),
            currency="RUB",
            stale_after_seconds=LEASE_SECONDS,
        ).create_order(user_id=uuid.uuid4(), lines=LINES)
    return created.record.order.id


# ---------------------------------------------------------------------------
# The happy path, end to end
# ---------------------------------------------------------------------------


async def test_a_saga_committed_event_is_published_and_marked(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    order_id = await create_reserved_order(session_factory)
    [staged] = await outbox_rows(session_factory)
    assert staged.aggregate_id == order_id
    assert staged.published_at is None
    assert staged.attempts == 0

    publisher = RecordingPublisher()
    report = await build_relay(session_factory, publisher).process_batch()

    assert report == RelayReport(claimed=1, published=1)
    assert publisher.delivered == [staged.id]
    # The transport sees the stored payload, not a re-derived one: a consumer
    # reads exactly what the transaction committed.
    assert publisher.events[0].payload == staged.payload
    assert publisher.events[0].event_type == staged.event_type

    [stored] = await outbox_rows(session_factory)
    assert stored.published_at is not None
    assert stored.attempts == 1


async def test_a_second_pass_finds_nothing_to_publish(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await create_reserved_order(session_factory)
    publisher = RecordingPublisher()
    relay = build_relay(session_factory, publisher)

    await relay.process_batch()
    second = await relay.process_batch()

    assert second == RelayReport()
    assert len(publisher.attempts) == 1


async def test_two_sequential_relays_never_publish_a_row_twice(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The mark is committed before the next relay looks at the feed."""
    await create_reserved_order(session_factory)
    publisher = RecordingPublisher()

    await build_relay(session_factory, publisher).process_batch()
    await build_relay(session_factory, publisher).process_batch()

    assert len(publisher.attempts) == 1


# ---------------------------------------------------------------------------
# Failures and the attempt budget
# ---------------------------------------------------------------------------


async def test_a_failed_publish_waits_for_the_lease_before_being_retried(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_outbox_event(session_factory)
    failing = RecordingPublisher(failing=True)
    relay = build_relay(session_factory, failing)

    report = await relay.process_batch()

    assert report == RelayReport(claimed=1, published=0, failed=1)
    [stored] = await outbox_rows(session_factory)
    # The attempt is spent and nothing is marked: the row is claimable again only
    # when the lease expires, which doubles as the backoff.
    assert stored.id == event_id
    assert stored.attempts == 1
    assert stored.published_at is None

    # A fresh lease hides the row from this pass and from any other relay.
    again = await relay.process_batch()
    assert again == RelayReport()
    assert len(failing.attempts) == 1

    await age_outbox_lease(
        session_factory, event_id=event_id, seconds=LEASE_SECONDS + 1
    )
    healthy = RecordingPublisher()
    recovered = await build_relay(session_factory, healthy).process_batch()

    assert recovered == RelayReport(claimed=1, published=1)
    assert healthy.delivered == [event_id]
    [after_retry] = await outbox_rows(session_factory)
    assert after_retry.attempts == 2
    assert after_retry.published_at is not None


async def test_a_row_that_spent_its_budget_leaves_the_feed_and_is_counted(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_outbox_event(
        session_factory,
        attempts=MAX_ATTEMPTS,
    )
    publisher = RecordingPublisher()

    report = await build_relay(session_factory, publisher).process_batch()

    # Not claimed at all: the strict `<` in the claim predicate treats a spent
    # budget as terminal, so the transport is never asked again.
    assert report == RelayReport(exhausted=1)
    assert publisher.attempts == []
    [stored] = await outbox_rows(session_factory)
    assert stored.id == event_id
    assert stored.attempts == MAX_ATTEMPTS
    assert stored.published_at is None


async def test_a_lost_compare_and_set_is_reported_as_a_duplicate(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_outbox_event(session_factory)

    report = await build_relay(
        session_factory,
        MarkingPublisher(session_factory),
    ).process_batch()

    # Delivered twice, marked once: at-least-once, and the relay says so instead
    # of pretending it was the process that completed the event.
    assert report == RelayReport(claimed=1, published=1, duplicates=1)
    [stored] = await outbox_rows(session_factory)
    assert stored.id == event_id
    assert stored.published_at is not None
