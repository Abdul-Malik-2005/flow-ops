"""The sender against real PostgreSQL: claim → send → mark, end to end.

The storage half is covered by `tests/integration/test_inbox_repository.py` and
the loop by `tests/test_notification_sender.py`; here the two meet, and only a
real database can prove the interesting cases:

* a recorded row travels through the sender and is marked, and a second pass
  finds nothing to do;
* a failed send writes nothing, so the row waits for the lease and only then is
  claimed again with a second attempt spent;
* a row that spent its budget is out of the feed and counted as a dead letter;
* a mark that loses the compare-and-set is reported as a duplicate instead of a
  success — the reason delivery is at-least-once and `event_id` is the key.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.notifications import NotificationMessage
from app.repositories.inbox import SqlInboxRepository
from app.services.notification_sender import NotificationSender, SendReport
from tests.integration.helpers import age_inbox_lease, inbox_rows, insert_inbox_event

pytestmark = pytest.mark.integration

BATCH_SIZE = 10
LEASE_SECONDS = 30.0
MAX_ATTEMPTS = 3


class RecordingSender:
    """Records every delivery; `failing=True` models a provider that is down."""

    def __init__(self, *, failing: bool = False) -> None:
        self.failing = failing
        self.delivered: list[uuid.UUID] = []
        #: The composed message each `send` received.
        self.messages: list[NotificationMessage] = []
        #: Every attempt, including the ones that raised.
        self.attempts: list[uuid.UUID] = []

    async def send(
        self,
        *,
        event_id: uuid.UUID,
        message: NotificationMessage,
    ) -> None:
        self.attempts.append(event_id)
        self.messages.append(message)
        if self.failing:
            raise RuntimeError("provider is down")
        self.delivered.append(event_id)

    async def aclose(self) -> None:
        """The sender may close its transport; this fake holds nothing."""


class MarkingSender:
    """A sender that marks the row itself — another sender, finishing first.

    Used to force the compare-and-set in `mark_sent` to lose, which is not
    reachable sequentially and would otherwise stay untested.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self.delivered: list[uuid.UUID] = []

    async def send(
        self,
        *,
        event_id: uuid.UUID,
        message: NotificationMessage,
    ) -> None:
        self.delivered.append(event_id)
        async with self._session_factory() as session:
            await SqlInboxRepository(session).mark_sent(event_id=event_id)
            await session.commit()

    async def aclose(self) -> None:
        """Nothing to release."""


def build_sender(
    session_factory: async_sessionmaker[AsyncSession],
    transport: RecordingSender | MarkingSender,
) -> NotificationSender:
    return NotificationSender(
        session_factory=session_factory,
        sender=transport,
        batch_size=BATCH_SIZE,
        lease_seconds=LEASE_SECONDS,
        max_attempts=MAX_ATTEMPTS,
    )


async def test_a_recorded_row_is_delivered_and_marked(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_inbox_event(session_factory)
    transport = RecordingSender()

    report = await build_sender(session_factory, transport).process_batch()

    assert report == SendReport(claimed=1, sent=1)
    assert transport.delivered == [event_id]
    [row] = await inbox_rows(session_factory)
    assert row.event_id == event_id
    assert row.sent_at is not None

    # The row is out of the feed: a second pass has nothing to deliver.
    again = await build_sender(session_factory, transport).process_batch()
    assert again == SendReport()


async def test_a_failed_send_waits_for_the_lease_before_being_retried(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_inbox_event(session_factory)
    failing = RecordingSender(failing=True)
    sender = build_sender(session_factory, failing)

    report = await sender.process_batch()

    assert report == SendReport(claimed=1, failed=1)
    [stored] = await inbox_rows(session_factory)
    # The attempt is spent and nothing is marked: the row is claimable again only
    # when the lease expires, which doubles as the backoff.
    assert stored.event_id == event_id
    assert stored.attempts == 1
    assert stored.sent_at is None

    # A fresh lease hides the row from this pass and from any other sender.
    again = await sender.process_batch()
    assert again == SendReport()
    assert len(failing.attempts) == 1

    await age_inbox_lease(session_factory, event_id=event_id, seconds=LEASE_SECONDS + 1)
    healthy = RecordingSender()
    recovered = await build_sender(session_factory, healthy).process_batch()

    assert recovered == SendReport(claimed=1, sent=1)
    assert healthy.delivered == [event_id]
    [after_retry] = await inbox_rows(session_factory)
    assert after_retry.attempts == 2
    assert after_retry.sent_at is not None


async def test_a_row_that_spent_its_budget_leaves_the_feed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_inbox_event(session_factory, attempts=MAX_ATTEMPTS)
    transport = RecordingSender()

    report = await build_sender(session_factory, transport).process_batch()

    # Not claimed at all: the strict `<` in the claim predicate treats a spent
    # budget as terminal, so the provider is never asked again.
    assert report == SendReport(exhausted=1)
    assert transport.attempts == []
    [stored] = await inbox_rows(session_factory)
    assert stored.event_id == event_id
    assert stored.attempts == MAX_ATTEMPTS
    assert stored.sent_at is None


async def test_a_lost_compare_and_set_is_reported_as_a_duplicate(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event_id = await insert_inbox_event(session_factory)

    report = await build_sender(
        session_factory,
        MarkingSender(session_factory),
    ).process_batch()

    # Delivered twice, marked once: at-least-once, and the sender says so instead
    # of pretending it was the process that completed the row.
    assert report == SendReport(claimed=1, sent=1, duplicates=1)
    [stored] = await inbox_rows(session_factory)
    assert stored.event_id == event_id
    assert stored.sent_at is not None
