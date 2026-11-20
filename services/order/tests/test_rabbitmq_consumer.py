"""The inbound broker adapter, pinned down without a broker.

A fake connection/channel/queue is enough to prove what the topology depends on:
the queue is a quorum queue with a delivery limit and a dead-letter policy, only
the allow-list of routing keys is bound, the DLX routes dead letters into a
durable DLQ, and a second `start()` does not subscribe twice. Ack/nack behaviour
is not tested here because it is not here: the adapter only hands messages to the
runtime.

The real AMQP path (queue declaration, `Basic.Ack`, redelivery counting) needs a
live broker and lives in `tests/integration/test_event_consumer_rabbitmq.py`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pytest
from aio_pika import ExchangeType

from app.consumers.rabbitmq import (
    DEAD_LETTER_EXCHANGE_ARGUMENT,
    DEAD_LETTER_ROUTING_KEY_ARGUMENT,
    DELIVERY_LIMIT_ARGUMENT,
    QUEUE_TYPE_ARGUMENT,
    RabbitMQEventConsumer,
)

URL = "amqp://flowops:secret@localhost:5672/"
EXCHANGE = "order.events"
QUEUE = "order.event-projections"
DLX = "order.events.dlx"
DLQ = "order.event-projections.dlq"
ROUTING_KEYS = ("order.reserved", "order.failed", "order.cancelled")
PREFETCH = 10
DELIVERY_LIMIT = 5


class FakeExchange:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeQueue:
    def __init__(
        self,
        name: str,
        durable: bool,
        arguments: dict[str, Any] | None,
    ) -> None:
        self.name = name
        self.durable = durable
        self.arguments = arguments
        self.bindings: list[tuple[str, str]] = []
        self.consumers: list[Any] = []

    async def bind(self, exchange: FakeExchange, *, routing_key: str) -> None:
        self.bindings.append((exchange.name, routing_key))

    async def consume(self, handler: Any) -> str:
        self.consumers.append(handler)
        return f"ctag-{len(self.consumers)}"


class FakeChannel:
    def __init__(self) -> None:
        self.is_closed = False
        self.qos: list[int] = []
        self.exchanges: list[tuple[str, ExchangeType, bool]] = []
        self.queues: list[FakeQueue] = []

    async def set_qos(self, *, prefetch_count: int) -> None:
        self.qos.append(prefetch_count)

    async def declare_exchange(
        self,
        name: str,
        type_: ExchangeType,
        *,
        durable: bool,
    ) -> FakeExchange:
        self.exchanges.append((name, type_, durable))
        return FakeExchange(name)

    async def declare_queue(
        self,
        name: str,
        *,
        durable: bool,
        arguments: dict[str, Any] | None = None,
    ) -> FakeQueue:
        queue = FakeQueue(name, durable, arguments)
        self.queues.append(queue)
        return queue

    def queue(self, name: str) -> FakeQueue:
        return next(queue for queue in self.queues if queue.name == name)


class FakeConnection:
    def __init__(self) -> None:
        self.is_closed = False
        self.channels: list[FakeChannel] = []
        self.closed = 0
        self.fail_close = False

    async def channel(self) -> FakeChannel:
        channel = FakeChannel()
        self.channels.append(channel)
        return channel

    async def close(self) -> None:
        self.closed += 1
        if self.fail_close:
            raise RuntimeError("broker went away")
        self.is_closed = True
        for channel in self.channels:
            channel.is_closed = True


@dataclass
class Broker:
    """The adapter plus the fakes it talks to."""

    consumer: RabbitMQEventConsumer
    connections: list[FakeConnection]
    connects: list[dict[str, Any]]

    @property
    def connection(self) -> FakeConnection:
        return self.connections[-1]

    @property
    def channel(self) -> FakeChannel:
        return self.connection.channels[-1]

    @property
    def queue(self) -> FakeQueue:
        return self.channel.queue(QUEUE)

    @property
    def dead_letter_queue(self) -> FakeQueue:
        return self.channel.queue(DLQ)


async def noop(_message: Any) -> None:
    """A handler that does nothing: the adapter must not care what it does."""


def build_consumer(**overrides: Any) -> Broker:
    """An adapter wired to fakes; no I/O happens until `start` is awaited.

    Every `connect` call hands back a *new* connection, like the real one: a fake
    returning a half-used connection would hide a reconnect bug.
    """
    connections: list[FakeConnection] = []
    connects: list[dict[str, Any]] = []

    async def connect(url: str, *, timeout: float) -> FakeConnection:
        connects.append({"url": url, "timeout": timeout})
        connection = FakeConnection()
        connections.append(connection)
        return connection

    values: dict[str, Any] = {
        "url": URL,
        "exchange_name": EXCHANGE,
        "queue_name": QUEUE,
        "dead_letter_exchange": DLX,
        "dead_letter_queue": DLQ,
        "routing_keys": ROUTING_KEYS,
        "prefetch_count": PREFETCH,
        "max_delivery_count": DELIVERY_LIMIT,
    }
    values.update(overrides)
    consumer = RabbitMQEventConsumer(connect=connect, **values)
    return Broker(consumer, connections, connects)


async def test_the_queue_is_quorum_with_a_delivery_limit_and_a_dlx() -> None:
    broker = build_consumer()

    await broker.consumer.start(noop)

    # These four arguments are the queue's identity: a redeclaration with other
    # values is rejected by the broker, so they are asserted as one exact dict.
    assert broker.queue.arguments == {
        QUEUE_TYPE_ARGUMENT: "quorum",
        DELIVERY_LIMIT_ARGUMENT: DELIVERY_LIMIT,
        DEAD_LETTER_EXCHANGE_ARGUMENT: DLX,
        DEAD_LETTER_ROUTING_KEY_ARGUMENT: DLQ,
    }
    assert broker.queue.durable is True


async def test_only_the_allow_list_is_bound_to_the_producer_exchange() -> None:
    broker = build_consumer()

    await broker.consumer.start(noop)

    assert broker.queue.bindings == [(EXCHANGE, key) for key in ROUTING_KEYS]
    # `order.#` would silently turn every future event type into an input of a
    # consumer that cannot understand it.
    assert (EXCHANGE, "order.#") not in broker.queue.bindings


async def test_the_exchange_is_redeclared_and_the_dlx_is_direct() -> None:
    broker = build_consumer()

    await broker.consumer.start(noop)

    # The producer declares the exchange too; the consumer repeats it so it can
    # start first, and must use the same type and durability.
    assert broker.channel.exchanges == [
        (EXCHANGE, ExchangeType.TOPIC, True),
        (DLX, ExchangeType.DIRECT, True),
    ]


async def test_dead_letters_are_routed_into_a_durable_queue() -> None:
    broker = build_consumer()

    await broker.consumer.start(noop)

    assert broker.dead_letter_queue.durable is True
    # One explicit routing key (= the DLQ name) keeps dead letters of this queue
    # from leaking into another queue that shares the DLX.
    assert broker.dead_letter_queue.bindings == [(DLX, DLQ)]


async def test_prefetch_and_a_manual_ack_subscription_are_used() -> None:
    broker = build_consumer()

    await broker.consumer.start(noop)

    assert broker.channel.qos == [PREFETCH]
    # `consume` is called without `no_ack`: the runtime must be able to reject a
    # message after a failed commit.
    assert broker.queue.consumers == [noop]


async def test_the_connection_uses_the_configured_url_and_timeout() -> None:
    broker = build_consumer()

    await broker.consumer.start(noop)

    assert broker.connects == [{"url": URL, "timeout": 10.0}]


async def test_a_second_start_does_not_subscribe_twice() -> None:
    broker = build_consumer()

    await broker.consumer.start(noop)
    await broker.consumer.start(noop)

    # A duplicate subscription would deliver every event twice under different
    # consumer tags, and the journal would do the deduplication work for nothing.
    assert len(broker.connects) == 1
    assert len(broker.queue.consumers) == 1


async def test_a_closed_connection_is_replaced_on_the_next_start() -> None:
    broker = build_consumer()
    await broker.consumer.start(noop)
    broker.connection.is_closed = True

    await broker.consumer.start(noop)

    assert len(broker.connects) == 2
    assert len(broker.connections) == 2
    assert len(broker.queue.consumers) == 1


async def test_aclose_closes_the_connection_once() -> None:
    broker = build_consumer()
    await broker.consumer.start(noop)

    await broker.consumer.aclose()
    await broker.consumer.aclose()

    assert broker.connection.closed == 1
    assert broker.connection.is_closed is True


async def test_a_failing_close_is_logged_and_swallowed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker = build_consumer()
    await broker.consumer.start(noop)
    broker.connection.fail_close = True

    with caplog.at_level(logging.WARNING, logger="app.consumers.rabbitmq"):
        # Shutdown must not raise: that would mask the reason the process stopped
        # and skip disposing the engine.
        await broker.consumer.aclose()

    assert "closing the RabbitMQ consumer connection failed" in caplog.text


def test_a_consumer_without_routing_keys_is_rejected() -> None:
    with pytest.raises(ValueError, match="routing key"):
        build_consumer(routing_keys=())
