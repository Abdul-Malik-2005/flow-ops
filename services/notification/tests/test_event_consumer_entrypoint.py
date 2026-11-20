"""The consumer callback: commit/ack ordering and the two refusal paths.

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
    InboxConsumer,
    build_consumer,
    main,
)
from app.services.event_handler import UnsupportedEventTypeError

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


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def envelope(**overrides: Any) -> bytes:
    message: dict[str, Any] = {
        "event_id": EVENT_ID,
        "event_type": OrderEventType.RESERVED.value,
        "aggregate_id": ORDER_ID,
        "occurred_at": "2026-11-19T12:30:00+00:00",
        "payload": {"order_id": ORDER_ID, "status": "reserved"},
    }
    message.update(overrides)
    return json.dumps(message).encode()


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
) -> InboxConsumer:
    return InboxConsumer(
        session_factory=FakeSessionFactory(  # type: ignore[arg-type]
            FakeSession(journal, fail_commit=fail_commit)
        ),
        stop=stop if stop is not None else asyncio.Event(),
        requeue_delay_seconds=requeue_delay_seconds,
        max_delivery_count=max_delivery_count,
        report_interval_seconds=report_interval_seconds,
        record=record,
        clock=clock or FakeClock(),
    )


async def test_commit_happens_before_ack() -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal)

    await handler(FakeMessage(envelope(), journal))

    # The one ordering that makes at-least-once safe: a crash after `commit`
    # redelivers a message the inbox deduplicates; a crash after an early `ack`
    # would lose it for good.
    assert journal == ["commit", "ack"]


async def test_a_duplicate_is_acknowledged() -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal, record=record_duplicate)

    await handler(FakeMessage(envelope(), journal))

    # A duplicate is a success, not a retry: the event was already recorded.
    assert journal == ["commit", "ack"]


async def test_a_malformed_body_is_dead_lettered() -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal)

    await handler(FakeMessage(b"not json", journal))

    # Retrying cannot fix broken JSON, so it goes to the DLQ on the first try.
    assert journal == ["nack(requeue=False)"]


async def test_a_deterministic_refusal_is_dead_lettered() -> None:
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        record=record_raising(UnsupportedEventTypeError("order.paid")),
    )

    await handler(FakeMessage(envelope(event_type="order.paid"), journal))

    assert journal == ["nack(requeue=False)"]


async def test_a_failed_commit_is_requeued() -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal, fail_commit=True)

    await handler(FakeMessage(envelope(), journal))

    # A transient database failure must leave the message unacknowledged: the
    # commit is journalled but the ack is not, so the broker redelivers.
    assert journal == ["commit", "nack(requeue=True)"]


async def test_the_last_allowed_delivery_is_dead_lettered() -> None:
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        record=record_raising(RuntimeError("still down")),
        max_delivery_count=2,
    )

    # `x-acquired-count` equals the number of previous acquisitions, so 1 here
    # means this is the second delivery — the last one this consumer accepts.
    await handler(FakeMessage(envelope(), journal, headers={"x-acquired-count": 1}))

    # RabbitMQ 4 does not apply `x-delivery-limit` to an explicit requeue, so the
    # runtime refuses it itself and the DLX takes over.
    assert journal == ["nack(requeue=False)"]


async def test_a_lost_ack_is_swallowed() -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal)
    message = FakeMessage(envelope(), journal)
    message.ack_failure = RuntimeError("the channel died")

    # The row is committed; a lost ack only means redelivery, which the inbox
    # deduplicates. It must not kill the consumer loop.
    await handler(message)

    assert journal == ["commit", "ack"]


async def test_a_lost_nack_is_swallowed() -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal)
    message = FakeMessage(b"not json", journal)
    message.nack_failure = RuntimeError("the channel died")

    await handler(message)

    # An unsettled message returns to the queue when the channel closes, so the
    # warning is enough; raising would mask the real reason of the stop.
    assert journal == ["nack(requeue=False)"]


def test_routing_keys_come_from_the_contract_enum() -> None:
    # A literal list would drift from the contract after the first addition; the
    # enum is the single place a new event type is taught to this consumer.
    assert ROUTING_KEYS == tuple(event_type.value for event_type in OrderEventType)


def test_build_consumer_wires_the_settings() -> None:
    config = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://test:test@localhost:5432/notification_test_db",
        rabbitmq_url="amqp://flowops:secret@localhost:5672/",
        environment="testing",
    )

    consumer = build_consumer(config)

    # Private on purpose: the point is that every name came from configuration,
    # not that the adapter exposes them.
    assert consumer._exchange_name == config.order_events_exchange
    assert consumer._queue_name == config.event_consumer_queue
    assert consumer._routing_keys == ROUTING_KEYS


def test_main_refuses_to_run_without_a_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # There is no HTTP process to fall back on: a consumer without a broker must
    # say so and exit before it connects in a loop.
    monkeypatch.setattr(entrypoint.settings, "rabbitmq_url", "")

    assert main([]) == 2


async def test_the_report_counts_new_rows_and_duplicates(
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
    assert "received=1" in caplog.text
    assert "duplicates=1" in caplog.text


async def test_a_forced_report_flushes_the_last_interval(
    caplog: pytest.LogCaptureFixture,
) -> None:
    journal: list[str] = []
    handler = build_handler(journal=journal)

    with caplog.at_level(logging.INFO, logger="app.event_consumer"):
        await handler(FakeMessage(envelope(), journal))
        # At 60 s one message is not a report line; shutdown must still flush it.
        assert "consumer report" not in caplog.text
        handler.report(force=True)

    assert "received=1" in caplog.text


async def test_the_report_carries_an_idle_heartbeat(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = FakeClock()
    journal: list[str] = []
    handler = build_handler(
        journal=journal,
        report_interval_seconds=0.0,
        clock=clock,
    )

    with caplog.at_level(logging.INFO, logger="app.event_consumer"):
        handler.report()  # nothing handled yet
        await handler(FakeMessage(envelope(), journal))
        clock.advance(42.0)
        handler.report()

    # The heartbeat proves "alive": never processed, then just processed, then
    # 42 s idle since the last success.
    assert "idle_s=never" in caplog.text
    assert "idle_s=0.0" in caplog.text
    assert "idle_s=42.0" in caplog.text


def test_the_parser_reads_the_once_mode() -> None:
    args = entrypoint._parse_args(["--once", "--drain-timeout", "5"])

    assert args.once is True
    assert args.drain_timeout == 5.0


def test_the_parser_defaults_to_run_forever() -> None:
    args = entrypoint._parse_args([])

    assert args.once is False
    assert args.drain_timeout == 30.0
