"""The consumer callback: the commit/ack ordering and the two refusal paths.

No database and no broker: the session is a fake that journals its `commit`, and
the message is a fake that journals its `ack`/`nack`. Because both write into the
same list, the ordering guarantee — commit strictly before ack — is an assertion
on a list instead of a comment. The real AMQP path is covered by
`tests/integration/test_event_consumer_rabbitmq.py`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Mapping
from typing import Any, Self

import pytest

from app import event_consumer as entrypoint
from app.core.config import Settings
from app.domain.events import OrderEventType
from app.event_consumer import (
    ROUTING_KEYS,
    EventJournalConsumer,
    build_consumer,
    main,
)
from app.services.event_projection import UnsupportedEventTypeError
from tests.test_rabbitmq_consumer import FakeConnection, noop

EVENT_ID = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
ORDER_ID = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"


class FakeMessage:
    """An incoming message whose verdicts land in the shared journal."""

    def __init__(
        self,
        body: bytes,
        journal: list[str],
        *,
        message_id: str | None = "event-1",
        headers: dict[str, Any] | None = None,
    ) -> None:
        self.body = body
        self.journal = journal
        self.message_id = message_id
        self.headers = headers or {}
        self.ack_failure: BaseException | None = None
        self.nack_failure: BaseException | None = None

    async def ack(self) -> None:
        self.journal.append("ack")
        if self.ack_failure is not None:
            raise self.ack_failure

    async def nack(self, *, requeue: bool = True) -> None:
        self.journal.append(f"nack(requeue={requeue})")
        if self.nack_failure is not None:
            raise self.nack_failure


class FakeSession:
    def __init__(self, journal: list[str], *, fail_commit: bool = False) -> None:
        self._journal = journal
        self._fail_commit = fail_commit

    async def commit(self) -> None:
        self._journal.append("commit")
        if self._fail_commit:
            raise RuntimeError("the database went away")

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class FakeSessionFactory:
    def __init__(self, session: FakeSession) -> None:
        self._session = session

    def __call__(self) -> FakeSession:
        return self._session


async def record_ok(_session: Any, _payload: Mapping[str, Any]) -> bool:
    return True


async def record_duplicate(_session: Any, _payload: Mapping[str, Any]) -> bool:
    return False


def record_raising(error: BaseException) -> Any:
    async def record(_session: Any, _payload: Mapping[str, Any]) -> bool:
        raise error

    return record


def build_handler(
    *,
    journal: list[str],
    record: Any = record_ok,
    fail_commit: bool = False,
    stop: asyncio.Event | None = None,
    requeue_delay_seconds: float = 0.0,
    max_delivery_count: int = 5,
    report_interval_seconds: float = 60.0,
    clock: Callable[[], float] | None = None,
) -> EventJournalConsumer:
    session = FakeSession(journal, fail_commit=fail_commit)
    return EventJournalConsumer(
        session_factory=FakeSessionFactory(session),  # type: ignore[arg-type]
        stop=stop if stop is not None else asyncio.Event(),
        requeue_delay_seconds=requeue_delay_seconds,
        max_delivery_count=max_delivery_count,
        report_interval_seconds=report_interval_seconds,
        record=record,
        **({} if clock is None else {"clock": clock}),
    )


def envelope(**overrides: Any) -> bytes:
    message: dict[str, Any] = {
        "schema_version": 1,
        "event_id": EVENT_ID,
        "event_type": OrderEventType.RESERVED.value,
        "aggregate_id": ORDER_ID,
        "occurred_at": "2026-11-19T12:30:00+00:00",
        "payload": {"order_id": ORDER_ID, "status": "reserved"},
    }
    message.update(overrides)
    return json.dumps(message).encode()


async def test_the_row_is_committed_before_the_message_is_acknowledged() -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal)

    await handler(FakeMessage(envelope(), journal))

    # The whole contract in one line: a crash after "commit" is a duplicate, a
    # crash after "ack" without a commit would be a lost event.
    assert journal == ["commit", "ack"]


async def test_a_duplicate_is_acknowledged_without_a_retry() -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal, record=record_duplicate)

    await handler(FakeMessage(envelope(), journal))

    # At-least-once delivery makes this a normal outcome: the row exists, and
    # refusing to ack would replay a message that is already journaled.
    assert journal == ["commit", "ack"]


async def test_the_decoded_object_is_handed_to_the_projector() -> None:
    seen: list[Mapping[str, Any]] = []

    async def record(_session: Any, payload: Mapping[str, Any]) -> bool:
        seen.append(payload)
        return True

    journal: list[str] = []
    await build_handler(journal=journal, record=record)(
        FakeMessage(envelope(), journal)
    )

    assert seen == [json.loads(envelope())]


@pytest.mark.parametrize(
    "body",
    [b"not json", b"[]", b'["order.reserved"]', b"\xff\xfe"],
    ids=["not-json", "json-array", "json-array-of-strings", "not-utf8"],
)
async def test_a_body_that_is_not_an_envelope_goes_to_the_dead_letter_path(
    body: bytes,
) -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal)

    await handler(FakeMessage(body, journal))

    # No commit: nothing was written, and a retry cannot turn bad bytes into JSON.
    assert journal == ["nack(requeue=False)"]


async def test_an_unknown_event_type_goes_to_the_dead_letter_path() -> None:
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        record=record_raising(UnsupportedEventTypeError("order.created")),
    )

    await handler(FakeMessage(envelope(), journal))

    assert journal == ["nack(requeue=False)"]


async def test_a_transient_failure_is_requeued_after_a_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []

    async def fake_sleep(_stop: asyncio.Event, seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr(entrypoint, "sleep_or_stop", fake_sleep)
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        record=record_raising(RuntimeError("the database is down")),
        requeue_delay_seconds=2.0,
    )

    await handler(FakeMessage(envelope(), journal))

    # Requeue, not DLQ: the message is fine, the dependency is not. The pause is
    # what keeps this from becoming a tight loop that burns the delivery budget.
    assert journal == ["nack(requeue=True)"]
    assert delays == [2.0]


async def test_the_last_allowed_delivery_is_dead_lettered_without_a_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []

    async def fake_sleep(_stop: asyncio.Event, seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr(entrypoint, "sleep_or_stop", fake_sleep)
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        record=record_raising(RuntimeError("still down")),
        max_delivery_count=3,
    )

    # The broker has already acquired this message twice: this is delivery 3 of
    # 3, so a requeue would be the endless loop the counter exists to prevent.
    await handler(FakeMessage(envelope(), journal, headers={"x-acquired-count": 2}))

    assert journal == ["nack(requeue=False)"]
    assert delays == []


async def test_a_malformed_delivery_counter_does_not_dead_letter_early() -> None:
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        record=record_raising(RuntimeError("still down")),
        max_delivery_count=3,
    )

    # A header we cannot read must not mean "limit reached": losing a message to
    # a typo in the broker is worse than one extra retry.
    await handler(FakeMessage(envelope(), journal, headers={"x-acquired-count": "two"}))

    assert journal == ["nack(requeue=True)"]


async def test_a_budget_of_one_attempt_dead_letters_the_first_failure() -> None:
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        record=record_raising(RuntimeError("still down")),
        max_delivery_count=1,
    )

    await handler(FakeMessage(envelope(), journal))

    # `max_delivery_count=1` means exactly one attempt: the failure is already
    # the last one, so there is nothing to retry.
    assert journal == ["nack(requeue=False)"]


async def test_a_failed_commit_is_never_acknowledged() -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal, fail_commit=True)

    await handler(FakeMessage(envelope(), journal))

    # The ordering proof: the insert may already be in the transaction, but the
    # broker must not learn that this message is done.
    assert journal == ["commit", "nack(requeue=True)"]
    assert "ack" not in journal


async def test_a_lost_ack_does_not_kill_the_consumer(
    caplog: pytest.LogCaptureFixture,
) -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal)
    message = FakeMessage(envelope(), journal)
    message.ack_failure = RuntimeError("the channel closed")

    with caplog.at_level(logging.WARNING, logger="app.event_consumer"):
        await handler(message)

    # The row is committed; the broker will redeliver and the journal dedupes.
    # Raising would only kill the loop over a recoverable transport failure.
    assert journal == ["commit", "ack"]
    assert "could not acknowledge" in caplog.text


async def test_a_lost_nack_does_not_kill_the_consumer(
    caplog: pytest.LogCaptureFixture,
) -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal)
    message = FakeMessage(b"not json", journal)
    message.nack_failure = RuntimeError("the channel closed")

    with caplog.at_level(logging.WARNING, logger="app.event_consumer"):
        await handler(message)

    assert journal == ["nack(requeue=False)"]
    assert "could not reject" in caplog.text


def test_the_allow_list_is_exactly_the_domain_event_types() -> None:
    # The subscription and the projector must not drift apart: both derive from
    # `OrderEventType`, so a new fact is consumed without touching this file.
    assert ROUTING_KEYS == tuple(event_type.value for event_type in OrderEventType)


async def test_build_consumer_maps_every_setting_onto_the_topology() -> None:
    config = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://test:test@localhost:5432/order_test_db",
        rabbitmq_url="amqp://flowops:secret@localhost:5672/",
        outbox_exchange="order.events.test",
        event_consumer_queue="order.test-projections",
        event_consumer_dlx_exchange="order.test.dlx",
        event_consumer_dlq="order.test-projections.dlq",
        event_consumer_prefetch_count=3,
        event_consumer_max_delivery_count=7,
    )
    connections: list[FakeConnection] = []

    async def connect(url: str, *, timeout: float) -> FakeConnection:
        connection = FakeConnection()
        connections.append(connection)
        return connection

    consumer = build_consumer(config, connect=connect)
    await consumer.start(noop)

    channel = connections[0].channels[0]
    queue = channel.queue(config.event_consumer_queue)
    dead_letter_queue = channel.queue(config.event_consumer_dlq)
    assert channel.qos == [config.event_consumer_prefetch_count]
    assert queue.arguments is not None
    assert queue.arguments["x-delivery-limit"] == 7
    assert (
        queue.arguments["x-dead-letter-exchange"] == config.event_consumer_dlx_exchange
    )
    assert queue.bindings == [(config.outbox_exchange, key) for key in ROUTING_KEYS]
    assert dead_letter_queue.bindings == [
        (config.event_consumer_dlx_exchange, config.event_consumer_dlq)
    ]


def test_main_refuses_to_start_without_a_broker_address(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(entrypoint.settings, "rabbitmq_url", "")

    with caplog.at_level(logging.ERROR, logger="app.event_consumer"):
        assert main([]) == 2

    # A consumer without an address would otherwise retry a connection it can
    # never make, forever, without saying why.
    assert "RABBITMQ_URL is required" in caplog.text


class FakeClock:
    """A clock the test advances by hand: reports must not depend on wall time."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


async def test_a_report_counts_new_rows_and_duplicates(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = FakeClock()
    journal: list[str] = []
    outcomes = iter([True, False])

    async def record(_session: Any, _payload: Mapping[str, Any]) -> bool:
        return next(outcomes)

    handler = build_handler(
        journal=journal,
        record=record,
        report_interval_seconds=0.0,
        clock=clock,
    )

    with caplog.at_level(logging.INFO, logger="app.event_consumer"):
        await handler(FakeMessage(envelope(), journal))
        await handler(FakeMessage(envelope(), journal, message_id="event-2"))

    # Two reports because the interval is zero; each one resets its counters.
    assert "processed=1" in caplog.text
    assert "duplicates=1" in caplog.text


async def test_a_refusal_is_counted_in_the_report(
    caplog: pytest.LogCaptureFixture,
) -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal, report_interval_seconds=0.0)

    with caplog.at_level(logging.INFO, logger="app.event_consumer"):
        await handler(FakeMessage(b"not json", journal))

    # A refusal is an incident, not traffic: it gets its own counter.
    assert "refused=1" in caplog.text


async def test_a_retry_is_counted_in_the_report(
    caplog: pytest.LogCaptureFixture,
) -> None:
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        record=record_raising(RuntimeError("the database is down")),
        report_interval_seconds=0.0,
        max_delivery_count=5,
    )

    with caplog.at_level(logging.INFO, logger="app.event_consumer"):
        await handler(FakeMessage(envelope(), journal))

    assert "retried=1" in caplog.text


async def test_a_dead_letter_is_counted_in_the_report(
    caplog: pytest.LogCaptureFixture,
) -> None:
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        record=record_raising(RuntimeError("still down")),
        report_interval_seconds=0.0,
        max_delivery_count=1,
    )

    with caplog.at_level(logging.INFO, logger="app.event_consumer"):
        await handler(FakeMessage(envelope(), journal))

    assert "dead_lettered=1" in caplog.text


async def test_a_report_waits_for_its_interval(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = FakeClock()
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        report_interval_seconds=60.0,
        clock=clock,
    )

    with caplog.at_level(logging.INFO, logger="app.event_consumer"):
        await handler(FakeMessage(envelope(), journal))
        # One message is not a report: at 60 s the line would be noise.
        assert "consumer report" not in caplog.text

        clock.advance(61.0)
        handler.report()

    assert "processed=1" in caplog.text


async def test_a_forced_report_flushes_the_last_interval(
    caplog: pytest.LogCaptureFixture,
) -> None:
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        report_interval_seconds=60.0,
        clock=FakeClock(),
    )

    with caplog.at_level(logging.INFO, logger="app.event_consumer"):
        await handler(FakeMessage(envelope(), journal))
        handler.report(force=True)

    # Shutdown must not swallow the last interval.
    assert "processed=1" in caplog.text
