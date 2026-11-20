"""The DLQ replay tool, pinned down without a broker.

The fake queue hands out messages and the fake default exchange records publishes
and their arguments (mandatory, timeout, routing key). What is asserted is the
operational contract: replay publishes into the consumer's *own* queue, a message
is acked only after the publish was confirmed, a broken body is never replayed, and
a failed publish leaves the message where it was. The real AMQP path lives in
`tests/integration/test_dlq_replay.py`.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import pytest

from app import dlq_replay as entrypoint
from app.dlq_replay import (
    PUBLISH_TIMEOUT_SECONDS,
    ReplayReport,
    replay_dead_letters,
)

URL = "amqp://flowops:secret@localhost:5672/"
TARGET_QUEUE = "notification.order-events"
DLQ = "notification.order-events.dlq"


class FakeMessage:
    def __init__(
        self,
        body: bytes,
        journal: list[str],
        *,
        message_id: str,
        content_type: str = "application/json",
    ) -> None:
        self.body = body
        self.journal = journal
        self.message_id = message_id
        self.content_type = content_type

    async def ack(self) -> None:
        self.journal.append("ack")

    async def nack(self, *, requeue: bool = True) -> None:
        self.journal.append(f"nack(requeue={requeue})")


class FakeExchange:
    """Stands in for the default exchange: records publishes, never routes."""

    def __init__(self, journal: list[str]) -> None:
        self.journal = journal
        self.published: list[dict[str, Any]] = []
        self.failure: BaseException | None = None

    async def publish(
        self,
        message: Any,
        routing_key: str,
        *,
        mandatory: bool,
        timeout: float,
    ) -> bool:
        self.journal.append(f"publish:{routing_key}")
        if self.failure is not None:
            raise self.failure
        self.published.append(
            {
                "message": message,
                "routing_key": routing_key,
                "mandatory": mandatory,
                "timeout": timeout,
            }
        )
        return True


class FakeQueue:
    def __init__(self, messages: list[FakeMessage], journal: list[str]) -> None:
        self.messages = messages
        self.journal = journal

    async def get(
        self,
        *,
        no_ack: bool,
        fail: bool,
        timeout: float,
    ) -> FakeMessage | None:
        self.journal.append("get")
        return self.messages.pop(0) if self.messages else None

    @property
    def remaining(self) -> int:
        return len(self.messages)


class FakeChannel:
    """Records declares; the default exchange is just the fake exchange."""

    def __init__(
        self,
        exchange: FakeExchange,
        source_queue: FakeQueue,
        journal: list[str],
    ) -> None:
        self._exchange = exchange
        self._source = source_queue
        self._journal = journal
        self.declared_queues: list[tuple[str, bool, bool]] = []

    @property
    def default_exchange(self) -> FakeExchange:
        return self._exchange

    async def declare_queue(
        self,
        name: str,
        *,
        durable: bool,
        passive: bool = False,
    ) -> FakeQueue:
        self.declared_queues.append((name, durable, passive))
        if name == DLQ:
            return self._source
        # The target queue is passive-declared to prove it exists; the tool never
        # reads from it, so an empty fake is enough.
        return FakeQueue([], self._journal)


class FakeConnection:
    def __init__(self, channel: FakeChannel) -> None:
        self.channel_instance = channel
        self.channel_options: list[dict[str, Any]] = []
        self.closed = False

    async def channel(self, **options: Any) -> FakeChannel:
        self.channel_options.append(options)
        return self.channel_instance

    async def close(self) -> None:
        self.closed = True


@dataclass
class Broker:
    connect: Any
    exchange: FakeExchange
    queue: FakeQueue
    connection: FakeConnection
    connects: list[dict[str, Any]]

    @property
    def channel(self) -> FakeChannel:
        return self.connection.channel_instance


def envelope(
    event_type: str = "order.reserved",
    *,
    event_id: str = "event-1",
) -> bytes:
    return json.dumps({"event_id": event_id, "event_type": event_type}).encode()


def build(
    *,
    bodies: list[bytes],
    failure: BaseException | None = None,
) -> tuple[Broker, list[str]]:
    """A tool wired to fakes; no I/O happens until `replay_dead_letters`."""
    journal: list[str] = []
    messages = [
        FakeMessage(body, journal, message_id=f"event-{index}")
        for index, body in enumerate(bodies)
    ]
    exchange = FakeExchange(journal)
    exchange.failure = failure
    source_queue = FakeQueue(messages, journal)
    channel = FakeChannel(exchange, source_queue, journal)
    connection = FakeConnection(channel)
    connects: list[dict[str, Any]] = []

    async def connect(url: str, *, timeout: float) -> FakeConnection:
        connects.append({"url": url, "timeout": timeout})
        return connection

    broker = Broker(connect, exchange, source_queue, connection, connects)
    return broker, journal


async def replay(broker: Broker, *, limit: int = 10) -> ReplayReport:
    return await replay_dead_letters(
        url=URL,
        dead_letter_queue=DLQ,
        target_queue=TARGET_QUEUE,
        limit=limit,
        connect=broker.connect,
        connect_timeout=5.0,
    )


async def test_a_dead_letter_is_replayed_into_the_own_queue_before_ack() -> None:
    broker, journal = build(bodies=[envelope()])

    report = await replay(broker, limit=1)

    assert report == ReplayReport(examined=1, replayed=1)
    # The ordering is the guarantee: ack after publish, never before.
    assert journal == ["get", f"publish:{TARGET_QUEUE}", "ack"]

    [sent] = broker.exchange.published
    # Routed to this consumer's own queue, not back through `order.events`.
    assert sent["routing_key"] == TARGET_QUEUE
    assert sent["mandatory"] is True
    assert sent["timeout"] == PUBLISH_TIMEOUT_SECONDS
    message = sent["message"]
    assert message.body == envelope()
    assert message.message_id == "event-0"
    assert message.type == "order.reserved"

    # Source DLQ and target queue are passive-declared: the tool never creates or
    # reconfigures another service's topology.
    assert broker.channel.declared_queues == [
        (DLQ, True, True),
        (TARGET_QUEUE, True, True),
    ]
    # Confirms and returns are what make the ack mean "the broker has it".
    assert broker.connection.channel_options == [
        {"publisher_confirms": True, "on_return_raises": True}
    ]
    assert broker.connects == [{"url": URL, "timeout": 5.0}]
    assert broker.connection.closed is True


async def test_the_routing_key_is_the_queue_not_the_event_type() -> None:
    broker, _ = build(bodies=[envelope("order.failed")])

    await replay(broker, limit=1)

    [sent] = broker.exchange.published
    # Even a non-reserved event goes to the same queue: routing is the consumer's
    # input, not the topic the producer used.
    assert sent["routing_key"] == TARGET_QUEUE
    assert sent["message"].type == "order.failed"


async def test_a_body_without_an_envelope_stays_in_the_queue() -> None:
    broker, journal = build(bodies=[b"not json", envelope()])

    report = await replay(broker)

    # No publish, no guess: the message is returned and the run stops, because
    # the next one may be just as broken (and would bounce right back).
    assert report == ReplayReport(examined=1, skipped=1)
    assert journal == ["get", "nack(requeue=True)"]
    assert broker.queue.remaining == 1


async def test_a_failed_publish_never_acknowledges() -> None:
    broker, journal = build(
        bodies=[envelope()],
        failure=RuntimeError("no route"),
    )

    report = await replay(broker)

    assert report == ReplayReport(examined=1, failed=1)
    assert journal == ["get", f"publish:{TARGET_QUEUE}", "nack(requeue=True)"]
    assert "ack" not in journal


async def test_the_limit_bounds_one_run() -> None:
    broker, _ = build(bodies=[envelope()] * 3)

    report = await replay(broker, limit=2)

    assert report == ReplayReport(examined=2, replayed=2)
    assert broker.queue.remaining == 1


async def test_an_empty_queue_is_a_no_op() -> None:
    broker, journal = build(bodies=[])

    report = await replay(broker)

    assert report == ReplayReport()
    assert journal == ["get"]


async def test_a_non_positive_limit_is_rejected_before_connecting() -> None:
    broker, journal = build(bodies=[])

    with pytest.raises(ValueError, match="positive"):
        await replay(broker, limit=0)

    assert broker.connects == []
    assert journal == []


def test_main_refuses_without_a_broker_address(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(entrypoint.settings, "rabbitmq_url", "")

    with caplog.at_level(logging.ERROR, logger="app.dlq_replay"):
        assert entrypoint.main([]) == 2

    assert "RABBITMQ_URL is required" in caplog.text
