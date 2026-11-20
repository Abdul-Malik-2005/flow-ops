"""Unit tests for the relay: the transaction order and the decision table.

The SQL of `claim_batch`/`mark_published` is proved against a real PostgreSQL in
`tests/integration/test_outbox_relay.py`; here only the loop is under test, with
a fake storage and a fake transport. What matters is that the batch is committed
*before* the first publish, that a failed publish is never marked, and that a
lost compare-and-set is reported as a duplicate instead of a success.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from app.models.outbox_event import OutboxEvent
from app.services import outbox_relay as service
from app.services.outbox_relay import OutboxRelay, RelayReport

BATCH_SIZE = 10
LEASE_SECONDS = 30.0
MAX_ATTEMPTS = 3
OCCURRED_AT = datetime(2026, 11, 19, 12, 30, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeSession:
    def __init__(self, journal: list[str] | None = None) -> None:
        self.commits = 0
        self._journal = journal

    async def commit(self) -> None:
        self.commits += 1
        if self._journal is not None:
            self._journal.append("commit")

    async def rollback(self) -> None:
        raise AssertionError("the relay never rolls back explicitly")


class _SessionContext:
    def __init__(self, session: FakeSession) -> None:
        self._session = session

    async def __aenter__(self) -> FakeSession:
        return self._session

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class FakeSessionFactory:
    def __init__(self, journal: list[str] | None = None) -> None:
        self.sessions: list[FakeSession] = []
        self._journal = journal

    def __call__(self) -> _SessionContext:
        session = FakeSession(self._journal)
        self.sessions.append(session)
        return _SessionContext(session)


class FakeOutbox:
    """Storage with observable effects, limited to what the relay calls."""

    def __init__(
        self,
        events: Sequence[OutboxEvent] = (),
        *,
        exhausted: int = 0,
        already_marked: set[uuid.UUID] | None = None,
    ) -> None:
        self.events = list(events)
        self.exhausted = exhausted
        self.already_marked = already_marked or set()
        self.claims: list[tuple[int, float, int]] = []
        self.marked: list[uuid.UUID] = []
        self.counted_with: list[int] = []

    async def claim_batch(
        self,
        *,
        limit: int,
        lease_seconds: float,
        max_attempts: int,
    ) -> list[OutboxEvent]:
        self.claims.append((limit, lease_seconds, max_attempts))
        batch, self.events = self.events[:limit], self.events[limit:]
        return batch

    async def mark_published(self, *, event_id: uuid.UUID) -> bool:
        self.marked.append(event_id)
        return event_id not in self.already_marked

    async def count_exhausted_unpublished(self, *, max_attempts: int) -> int:
        self.counted_with.append(max_attempts)
        return self.exhausted


class FakePublisher:
    def __init__(
        self,
        *,
        failing: set[uuid.UUID] | None = None,
        journal: list[str] | None = None,
    ) -> None:
        self.failing = failing or set()
        self.journal = journal
        self.published: list[uuid.UUID] = []
        self.closed = False

    async def publish(self, *, event: OutboxEvent) -> None:
        if self.journal is not None:
            self.journal.append(f"publish:{event.id}")
        if event.id in self.failing:
            raise RuntimeError("transport is down")
        self.published.append(event.id)

    async def aclose(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# The happy path
# ---------------------------------------------------------------------------


async def test_a_batch_is_published_then_marked_in_order() -> None:
    first, second = build_event(1), build_event(2)
    relay, outbox, publisher, _ = build_relay(outbox=FakeOutbox([first, second]))

    report = await relay.process_batch()

    assert report == RelayReport(claimed=2, published=2, failed=0, duplicates=0)
    assert publisher.published == [first.id, second.id]
    assert outbox.marked == [first.id, second.id]
    # The claim is asked with exactly the configured knobs: a hard-coded lease
    # would silently disagree with the settings the process was started with.
    assert outbox.claims == [(BATCH_SIZE, LEASE_SECONDS, MAX_ATTEMPTS)]
    assert outbox.counted_with == [MAX_ATTEMPTS]


async def test_the_claim_is_committed_before_the_first_publish() -> None:
    journal: list[str] = []
    event = build_event(1)
    relay, _, _, _ = build_relay(
        outbox=FakeOutbox([event]),
        journal=journal,
    )

    await relay.process_batch()

    # `publish` must never run while the claim transaction is open: the lease is
    # only visible to the next pass after the commit.
    assert journal[0] == "commit"
    assert journal[1:] == [f"publish:{event.id}", "commit"]


async def test_an_empty_claim_does_not_touch_the_transport() -> None:
    relay, outbox, publisher, _ = build_relay()

    report = await relay.process_batch()

    assert report == RelayReport(exhausted=0)
    assert publisher.published == []
    assert outbox.marked == []


# ---------------------------------------------------------------------------
# Failures
# ---------------------------------------------------------------------------


async def test_a_failed_publish_is_never_marked() -> None:
    broken, healthy = build_event(1), build_event(2)
    relay, outbox, publisher, _ = build_relay(
        outbox=FakeOutbox([broken, healthy]),
        publisher=FakePublisher(failing={broken.id}),
    )

    report = await relay.process_batch()

    assert report == RelayReport(claimed=2, published=1, failed=1)
    # The rest of the batch still goes out: one bad event is not a reason to
    # hold back facts other consumers are waiting for.
    assert publisher.published == [healthy.id]
    assert outbox.marked == [healthy.id]


async def test_a_failed_publish_is_logged_with_the_event_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broken = build_event(1)
    relay, _, _, _ = build_relay(
        outbox=FakeOutbox([broken]),
        publisher=FakePublisher(failing={broken.id}),
    )

    with caplog.at_level(logging.ERROR, logger="app.services.outbox_relay"):
        await relay.process_batch()

    assert str(broken.id) in caplog.text
    assert "order.reserved" in caplog.text


async def test_a_row_marked_by_someone_else_counts_as_a_duplicate() -> None:
    event = build_event(1)
    relay, _, _, _ = build_relay(
        outbox=FakeOutbox([event], already_marked={event.id}),
    )

    report = await relay.process_batch()

    # The publish succeeded, so re-delivery is real — and it is not an error.
    assert report == RelayReport(claimed=1, published=1, failed=0, duplicates=1)


async def test_dead_letters_are_surfaced_even_with_an_empty_batch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    relay, _, publisher, _ = build_relay(outbox=FakeOutbox(exhausted=2))

    with caplog.at_level(logging.ERROR, logger="app.services.outbox_relay"):
        report = await relay.process_batch()

    assert report == RelayReport(exhausted=2)
    assert publisher.published == []
    assert "2 outbox event(s) exhausted 3 publish attempts" in caplog.text


async def test_a_pass_that_knew_the_backlog_was_empty_does_not_count_again() -> None:
    relay, outbox, _, _ = build_relay(outbox=FakeOutbox([build_event(1)]))

    await relay.process_batch()  # the count is unknown: read it
    await relay.process_batch()  # idle, nothing failed, nothing can have changed

    # One `count(*)` for two passes instead of one per pass.
    assert outbox.counted_with == [MAX_ATTEMPTS]


async def test_a_failed_pass_rereads_the_dead_letter_backlog() -> None:
    event = build_event(1)
    outbox = FakeOutbox([event])
    relay, _, _, _ = build_relay(
        outbox=outbox,
        publisher=FakePublisher(failing={event.id}),
    )

    first = await relay.process_batch()
    outbox.exhausted = 1  # the publish above just spent the last attempt
    outbox.events = [event]  # the lease expired, the row is claimed again
    second = await relay.process_batch()

    assert first.exhausted == 0
    assert second.exhausted == 1
    assert outbox.counted_with == [MAX_ATTEMPTS, MAX_ATTEMPTS]


async def test_a_known_backlog_is_reread_until_it_is_cleared() -> None:
    outbox = FakeOutbox(exhausted=2)
    relay, _, _, _ = build_relay(outbox=outbox)

    await relay.process_batch()
    outbox.exhausted = 0  # an operator cleaned it up
    report = await relay.process_batch()

    # A non-zero backlog is never trusted: otherwise the warning would never
    # stop, even after the rows were dealt with.
    assert report == RelayReport()
    assert outbox.counted_with == [MAX_ATTEMPTS, MAX_ATTEMPTS]


async def test_a_stale_zero_is_reread_within_the_recheck_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(service, "DEAD_LETTER_RECHECK_PASSES", 3)
    relay, outbox, _, _ = build_relay(outbox=FakeOutbox())

    for _ in range(5):
        await relay.process_batch()

    # A dead letter created by another instance cannot stay invisible forever.
    assert outbox.counted_with == [MAX_ATTEMPTS, MAX_ATTEMPTS]


async def test_a_dead_letter_is_reported_once_and_then_only_reminded(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(service, "DEAD_LETTER_REMINDER_PASSES", 2)
    relay, _, _, _ = build_relay(outbox=FakeOutbox(exhausted=1))

    with caplog.at_level(logging.INFO, logger="app.services.outbox_relay"):
        await relay.process_batch()  # the backlog appeared: ERROR
        await relay.process_batch()  # unchanged: the reminder is not due yet
        await relay.process_batch()  # unchanged and due: WARNING, not ERROR

    levels = [record.levelno for record in caplog.records]
    assert levels.count(logging.ERROR) == 1
    assert levels.count(logging.WARNING) == 1


async def test_an_operator_cleanup_resets_the_dead_letter_alarm(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Zero passes in the recheck window: read the count on every pass, so this
    # test is about the alarm and not about how often the count is trusted.
    monkeypatch.setattr(service, "DEAD_LETTER_RECHECK_PASSES", 0)
    outbox = FakeOutbox(exhausted=1)
    relay, _, _, _ = build_relay(outbox=outbox)

    with caplog.at_level(logging.ERROR, logger="app.services.outbox_relay"):
        await relay.process_batch()  # ERROR: one dead letter
        outbox.exhausted = 0
        await relay.process_batch()  # the backlog is gone
        outbox.exhausted = 1
        report = await relay.process_batch()  # a new one is a new incident

    assert report == RelayReport(exhausted=1)
    assert caplog.text.count("exhausted 3 publish attempts") == 2


async def test_a_batch_uses_one_transaction_per_phase() -> None:
    relay, _, _, factory = build_relay(outbox=FakeOutbox([build_event(1)]))

    await relay.process_batch()

    # claim, mark, count: three short sessions, none of them held across a call.
    assert len(factory.sessions) == 3
    assert [session.commits for session in factory.sessions] == [1, 1, 0]


async def test_the_relay_releases_the_transport_it_was_built_with() -> None:
    """Ownership: the adapter went in through the constructor, so it comes out."""
    relay, _, publisher, _ = build_relay()

    await relay.aclose()

    assert publisher.closed


def build_event(index: int, *, attempts: int = 1) -> OutboxEvent:
    order_id = uuid.UUID(int=index)
    return OutboxEvent(
        id=uuid.UUID(int=index + 1000),
        aggregate_id=order_id,
        event_type="order.reserved",
        payload={"order_id": str(order_id), "status": "reserved"},
        occurred_at=OCCURRED_AT,
        attempts=attempts,
    )


def build_relay(
    *,
    outbox: FakeOutbox | None = None,
    publisher: FakePublisher | None = None,
    journal: list[str] | None = None,
) -> tuple[OutboxRelay, FakeOutbox, FakePublisher, FakeSessionFactory]:
    outbox = outbox or FakeOutbox()
    publisher = publisher or FakePublisher(journal=journal)
    factory = FakeSessionFactory(journal)
    relay = OutboxRelay(
        session_factory=factory,  # type: ignore[arg-type]
        publisher=publisher,
        batch_size=BATCH_SIZE,
        lease_seconds=LEASE_SECONDS,
        max_attempts=MAX_ATTEMPTS,
        outbox_factory=lambda _session: outbox,  # type: ignore[arg-type,return-value]
    )
    return relay, outbox, publisher, factory
