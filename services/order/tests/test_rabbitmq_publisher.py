"""The broker adapter, pinned down without a broker.

A fake connection/channel/exchange is enough to prove the four things the outbox
actually depends on: the bytes on the wire are the shared envelope, the publish
is `mandatory` on a durable topic exchange with confirms, a connection is opened
once and reused, and every unconfirmed outcome raises instead of returning.

The real AMQP path (exchange declaration, `Basic.Ack`, unroutable return) is the
one thing a fake cannot prove; it is covered by
`tests/integration/test_rabbitmq_publisher.py`, which needs a live broker.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, NamedTuple

import pytest
from aio_pika import DeliveryMode, ExchangeType
from aio_pika.exceptions import DeliveryError, PublishError
from aiormq.abc import DeliveredMessage
from pamqp import commands as pamqp_commands
from pamqp.header import ContentHeader

from app.models.outbox_event import OutboxEvent
from app.publishers.envelope import encode_event
from app.publishers.rabbitmq import (
    CONNECT_TIMEOUT_SECONDS,
    PUBLISH_TIMEOUT_SECONDS,
    EventNotDelivered,
    RabbitMQEventPublisher,
)

URL = "amqp://flowops:secret@localhost:5672/"
EXCHANGE = "order.events"
EVENT_ID = uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa")
ORDER_ID = uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb")
OCCURRED_AT = datetime(2026, 11, 19, 12, 30, tzinfo=UTC)


class Sent(NamedTuple):
    """One call the adapter made on the exchange."""

    message: Any
    routing_key: str
    mandatory: bool
    timeout: float | None


class FakeExchange:
    def __init__(self) -> None:
        self.sent: list[Sent] = []
        #: Returned to the caller: with confirms a success is `Basic.Ack`.
        self.verdict: Any = object()
        #: Raised instead of returning, to model `Nack`/`Reject`/`Return`.
        self.failure: BaseException | None = None

    async def publish(
        self,
        message: Any,
        routing_key: str,
        *,
        mandatory: bool,
        timeout: float | None,
    ) -> Any:
        self.sent.append(Sent(message, routing_key, mandatory, timeout))
        if self.failure is not None:
            raise self.failure
        return self.verdict


class FakeChannel:
    def __init__(self, exchange: FakeExchange) -> None:
        self.is_closed = False
        self.declared: list[tuple[str, ExchangeType, bool]] = []
        self._exchange = exchange

    async def declare_exchange(
        self,
        name: str,
        type_: ExchangeType,
        *,
        durable: bool,
    ) -> FakeExchange:
        self.declared.append((name, type_, durable))
        return self._exchange


class FakeConnection:
    def __init__(self, exchange: FakeExchange) -> None:
        self.is_closed = False
        self.channel_options: list[dict[str, Any]] = []
        self.exchange = exchange
        self.channels: list[FakeChannel] = []

    async def channel(self, **options: Any) -> FakeChannel:
        self.channel_options.append(options)
        channel = FakeChannel(self.exchange)
        self.channels.append(channel)
        return channel

    async def close(self) -> None:
        self.is_closed = True
        for channel in self.channels:
            channel.is_closed = True


@dataclass
class Broker:
    """The adapter plus the fakes it talks to."""

    publisher: RabbitMQEventPublisher
    exchange: FakeExchange
    connections: list[FakeConnection]
    connects: list[dict[str, Any]]

    @property
    def connection(self) -> FakeConnection:
        return self.connections[-1]

    @property
    def channel(self) -> FakeChannel:
        return self.connection.channels[-1]

    async def publish(self, event: OutboxEvent | None = None) -> Sent:
        await self.publisher.publish(event=event or build_event())
        return self.exchange.sent[-1]


def build_event(**overrides: Any) -> OutboxEvent:
    values: dict[str, Any] = {
        "id": EVENT_ID,
        "aggregate_id": ORDER_ID,
        "event_type": "order.reserved",
        "payload": {"order_id": str(ORDER_ID), "total_amount": "1990.00"},
        "occurred_at": OCCURRED_AT,
    }
    values.update(overrides)
    return OutboxEvent(**values)


def nacked() -> DeliveryError:
    """The real exception aio-pika raises on `Basic.Nack` — frames and all.

    Building it by hand would let a fake shape pass where the actual class cannot
    even be `str()`-ed; these constructors are the ones aio-pika uses.
    """
    return DeliveryError(
        None, pamqp_commands.Basic.Nack(delivery_tag=1, multiple=False)
    )


def returned() -> PublishError:
    """The real exception for `Basic.Return`, i.e. `mandatory` found no queue."""
    frame = pamqp_commands.Basic.Return(
        reply_code=312,
        reply_text="NO_ROUTE",
        exchange=EXCHANGE,
        routing_key="order.reserved",
    )
    message = DeliveredMessage(
        delivery=frame,
        header=ContentHeader(body_size=2, properties=pamqp_commands.Basic.Properties()),
        body=b"{}",
        channel=None,  # type: ignore[arg-type]
    )
    return PublishError(message, frame)


def build_broker(**kwargs: Any) -> Broker:
    """An adapter wired to fakes; no I/O happens until `publish` is awaited.

    Every `connect` call hands back a *new* connection, like the real one: a
    fake that returned an already-closed connection would hide a reconnect bug.
    """
    exchange = FakeExchange()
    connections: list[FakeConnection] = []
    connects: list[dict[str, Any]] = []

    async def connect(url: str, *, timeout: float) -> FakeConnection:
        connects.append({"url": url, "timeout": timeout})
        connection = FakeConnection(exchange)
        connections.append(connection)
        return connection

    publisher = RabbitMQEventPublisher(
        url=URL,
        exchange=EXCHANGE,
        connect=connect,
        **kwargs,
    )
    return Broker(publisher, exchange, connections, connects)


async def test_the_body_is_the_shared_envelope() -> None:
    """The broker and the log adapter must not be able to disagree on the bytes."""
    event = build_event()

    sent = await build_broker().publish(event)

    assert sent.message.body == encode_event(event).encode("utf-8")
    assert sent.message.content_type == "application/json"


async def test_the_message_carries_the_event_identity() -> None:
    """A consumer deduplicates and traces by these fields, not by the body."""
    sent = await build_broker().publish()

    assert sent.message.message_id == str(EVENT_ID)
    assert sent.message.type == "order.reserved"
    assert sent.message.headers == {"aggregate_id": str(ORDER_ID)}
    assert sent.message.timestamp == OCCURRED_AT


async def test_the_message_is_persistent() -> None:
    """A non-persistent message dies with the broker; so would the value of the row."""
    sent = await build_broker().publish()

    assert sent.message.delivery_mode == DeliveryMode.PERSISTENT


async def test_the_routing_key_is_the_event_type() -> None:
    """A consumer binds `order.reserved` or `order.#`; there is no mapping table."""
    sent = await build_broker().publish(build_event(event_type="order.cancelled"))

    assert sent.routing_key == "order.cancelled"


async def test_the_publish_is_mandatory_and_bounded() -> None:
    """`mandatory` turns "no queue bound" into a failure; the timeout keeps a hung
    publish inside the lease of the row it is publishing."""
    sent = await build_broker().publish()

    assert sent.mandatory is True
    assert sent.timeout == PUBLISH_TIMEOUT_SECONDS


async def test_the_exchange_is_durable_and_topical() -> None:
    broker = build_broker()

    await broker.publish()

    assert broker.channel.declared == [(EXCHANGE, ExchangeType.TOPIC, True)]


async def test_confirms_and_return_raising_are_requested() -> None:
    broker = build_broker()

    await broker.publish()

    assert broker.connection.channel_options == [
        {"publisher_confirms": True, "on_return_raises": True}
    ]


async def test_the_connection_is_opened_once_and_reused() -> None:
    """A batch of a hundred events is one handshake, not a hundred."""
    broker = build_broker()

    await broker.publish()
    await broker.publish()

    assert broker.connects == [{"url": URL, "timeout": CONNECT_TIMEOUT_SECONDS}]
    assert len(broker.channel.declared) == 1


async def test_a_nack_is_a_failed_publish() -> None:
    """`Basic.Nack`/`Basic.Reject`: the broker received the message and refused it."""
    broker = build_broker()
    broker.exchange.failure = nacked()

    with pytest.raises(EventNotDelivered, match="rejected.*Basic.Nack"):
        await broker.publisher.publish(event=build_event())


async def test_an_unroutable_event_is_a_failed_publish() -> None:
    """With `mandatory=True` an unbound routing key returns the message; mistaking
    that for delivery would mark the row published while no queue ever saw it."""
    broker = build_broker()
    broker.exchange.failure = returned()

    with pytest.raises(EventNotDelivered, match="unroutable"):
        await broker.publisher.publish(event=build_event())


async def test_a_publish_without_a_verdict_is_a_failed_publish() -> None:
    """Silence is not success: an absent confirm must keep the row unpublished."""
    broker = build_broker()
    broker.exchange.verdict = None

    with pytest.raises(EventNotDelivered, match="no confirmation"):
        await broker.publisher.publish(event=build_event())


async def test_a_dropped_connection_is_replaced_on_the_next_publish() -> None:
    """A broker restart is the normal case, not an exceptional one."""
    broker = build_broker()
    await broker.publish()
    # aio-pika closes the channels along with the connection, so a lost broker
    # shows up as a closed channel; the adapter must then reconnect.
    await broker.connection.close()

    await broker.publish()

    assert len(broker.connects) == 2


async def test_a_closed_channel_is_reopened_on_the_same_connection() -> None:
    """A channel can die while its connection lives; reconnecting would be waste."""
    broker = build_broker()
    await broker.publish()
    first_channel = broker.channel
    first_channel.is_closed = True

    await broker.publish()

    assert len(broker.connects) == 1
    assert broker.channel is not first_channel
    assert len(broker.channel.declared) == 1


async def test_closing_releases_the_connection() -> None:
    broker = build_broker()
    await broker.publish()

    await broker.publisher.aclose()

    assert broker.connection.is_closed is True


async def test_closing_twice_is_harmless() -> None:
    """`run_once` closes in a `finally`, and a signal-driven stop may close again."""
    broker = build_broker()
    await broker.publish()

    await broker.publisher.aclose()
    await broker.publisher.aclose()


async def test_closing_an_unused_publisher_never_connects() -> None:
    """`--once` on an empty outbox closes a transport that was never opened."""
    broker = build_broker()

    await broker.publisher.aclose()

    assert broker.connects == []
    assert broker.connections == []


async def test_a_failing_close_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Shutdown must still reach `engine.dispose()`; the broker reaps the session."""

    class BrokenConnection(FakeConnection):
        async def close(self) -> None:
            raise ConnectionResetError("socket already gone")

    broker = build_broker()
    broken = BrokenConnection(broker.exchange)
    broker.publisher._connection = broken  # type: ignore[assignment]

    with caplog.at_level(logging.WARNING, logger="app.publishers.rabbitmq"):
        await broker.publisher.aclose()

    assert "reap" in caplog.text


async def test_the_password_never_reaches_the_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker = build_broker()

    with caplog.at_level(logging.INFO, logger="app.publishers.rabbitmq"):
        await broker.publish()

    assert "secret" not in caplog.text
    assert "flowops:***@localhost" in caplog.text
