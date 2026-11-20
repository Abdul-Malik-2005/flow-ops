"""The real AMQP path, against a real broker.

The fakes in `tests/test_rabbitmq_publisher.py` pin the adapter's *decisions* but
cannot prove what the broker actually does: that a confirm arrives after
`publish()` returns, that the exchange survives, and that `mandatory=True` turns
an unbound routing key into an exception instead of a silent drop. That is what
this file checks.

Flag-gated on purpose: CI has no broker, and a test that silently skipped there
while failing on a laptop would be worse than an explicit flag. Run it against
the compose service:

    $env:ORDER_RABBITMQ_IT = "1"
    $env:RABBITMQ_URL = "amqp://flowops:dev_password@localhost:5672/"
    poetry run pytest tests/integration/test_rabbitmq_publisher.py -q

Each test uses its own uniquely named exchange, so runs never collide and the
shared `order.events` exchange is left untouched.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import aio_pika
import pytest
from aio_pika import ExchangeType

from app.models.outbox_event import OutboxEvent
from app.publishers.envelope import encode_event
from app.publishers.rabbitmq import EventNotDelivered, RabbitMQEventPublisher

pytestmark = pytest.mark.integration

BROKER_URL = os.getenv("RABBITMQ_URL", "")

if os.getenv("ORDER_RABBITMQ_IT") != "1" or not BROKER_URL:
    pytest.skip(
        "live broker test: set ORDER_RABBITMQ_IT=1 and RABBITMQ_URL to run it",
        allow_module_level=True,
    )


EVENT = OutboxEvent(
    id=uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"),
    aggregate_id=uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"),
    event_type="order.reserved",
    payload={
        "order_id": "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb",
        "total_amount": "1990.00",
    },
    occurred_at=datetime(2026, 11, 19, 12, 30, tzinfo=UTC),
)


@pytest.fixture
async def exchange_name() -> AsyncIterator[str]:
    """A throwaway exchange, deleted after the test even if the test failed."""
    name = f"order.events.it.{uuid.uuid4().hex}"
    yield name
    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        channel = await connection.channel()
        # Cleanup must not mask the real failure: a leftover exchange on a dev
        # broker is harmless, a `teardown error` hiding the assertion is not.
        with contextlib.suppress(Exception):
            await channel.exchange_delete(name)


@pytest.fixture
async def broker_channel() -> AsyncIterator[aio_pika.abc.AbstractChannel]:
    """A separate connection for the consumer side, so the publisher owns its own."""
    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        yield await connection.channel()


async def test_a_bound_queue_receives_the_published_envelope(
    exchange_name: str,
    broker_channel: aio_pika.abc.AbstractChannel,
) -> None:
    """The end-to-end claim: `publish()` returns, and the message really is there."""
    exchange = await broker_channel.declare_exchange(
        exchange_name, ExchangeType.TOPIC, durable=True
    )
    queue = await broker_channel.declare_queue("", exclusive=True)
    await queue.bind(exchange, routing_key="order.#")

    publisher = RabbitMQEventPublisher(url=BROKER_URL, exchange=exchange_name)
    try:
        await publisher.publish(event=EVENT)

        message = await queue.get(no_ack=True, timeout=5)

        assert message is not None
        assert message.body == encode_event(EVENT).encode("utf-8")
        assert message.routing_key == EVENT.event_type
        assert message.message_id == str(EVENT.id)
        assert message.type == EVENT.event_type
        assert message.headers == {"aggregate_id": str(EVENT.aggregate_id)}
    finally:
        await publisher.aclose()


async def test_an_unbound_routing_key_fails_the_publish(exchange_name: str) -> None:
    """`mandatory=True` is what keeps the outbox honest: with no consumer the
    message is returned, and the row must not be marked published."""
    publisher = RabbitMQEventPublisher(url=BROKER_URL, exchange=exchange_name)
    try:
        with pytest.raises(EventNotDelivered, match="unroutable"):
            await publisher.publish(event=EVENT)
    finally:
        await publisher.aclose()


async def test_the_publisher_survives_a_second_batch(
    exchange_name: str,
    broker_channel: aio_pika.abc.AbstractChannel,
) -> None:
    """One connection, many publishes: the daily path, not just the first one."""
    exchange = await broker_channel.declare_exchange(
        exchange_name, ExchangeType.TOPIC, durable=True
    )
    queue = await broker_channel.declare_queue("", exclusive=True)
    await queue.bind(exchange, routing_key="order.#")

    publisher = RabbitMQEventPublisher(url=BROKER_URL, exchange=exchange_name)
    try:
        await publisher.publish(event=EVENT)
        connection = publisher._connection
        await publisher.publish(event=EVENT)

        assert publisher._connection is connection
        assert connection is not None
        assert connection.is_closed is False
        assert await queue.get(no_ack=True, timeout=5) is not None
    finally:
        await publisher.aclose()
