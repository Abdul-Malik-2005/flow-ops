"""Notification and the broker together: delivery → inbox → ack, over real AMQP.

This is the cross-service hand-off `order.events` was built for: a message
published by Order's relay reaches a *different* service, which records it in its
own inbox. Neither unit half proves this alone — the transport test uses a fake
message, the inbox test never touches a broker.

* a delivered event is recorded once and acknowledged (the queue drains);
* the same `event_id` delivered twice leaves one row and acknowledges both copies;
* a deterministic refusal (bad JSON) lands in the DLQ immediately, with nothing
  written.

Flag-gated like the other broker tests: CI has no broker, and an integration test
that silently skipped there while failing on a laptop would be worse than an
explicit flag. Run it against the compose services:

    $env:NOTIFICATION_RABBITMQ_IT = "1"
    $env:RABBITMQ_URL = "amqp://flowops:dev_password@localhost:5672/"
    $env:TEST_DATABASE_URL = "postgresql+asyncpg://.../notification_test_db"
    poetry run pytest tests/integration/test_event_consumer_rabbitmq.py -q

Every test gets its own exchange, queue, DLX and DLQ, deleted afterwards. The
shared `order.events` exchange is never touched.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aio_pika
import pytest
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumers.rabbitmq import RabbitMQEventConsumer
from app.event_consumer import InboxConsumer
from app.models.notification_inbox import NotificationInbox

pytestmark = pytest.mark.integration

BROKER_URL = os.getenv("RABBITMQ_URL", "")

if os.getenv("NOTIFICATION_RABBITMQ_IT") != "1" or not BROKER_URL:
    pytest.skip(
        "end-to-end consumer test: set NOTIFICATION_RABBITMQ_IT=1, RABBITMQ_URL "
        "and TEST_DATABASE_URL to run it",
        allow_module_level=True,
    )

MAX_DELIVERY_COUNT = 3


@dataclass(frozen=True)
class Names:
    """Per-test topology names: nothing this suite creates is shared."""

    exchange: str
    queue: str
    dead_letter_exchange: str
    dead_letter_queue: str


@pytest.fixture
async def names() -> AsyncIterator[Names]:
    """Unique topology, deleted after the test even if the test failed."""
    suffix = uuid.uuid4().hex
    chosen = Names(
        exchange=f"order.events.it.{suffix}",
        queue=f"notification.order-events.it.{suffix}",
        dead_letter_exchange=f"notification.order-events.dlx.it.{suffix}",
        dead_letter_queue=f"notification.order-events.dlq.it.{suffix}",
    )
    yield chosen

    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        channel = await connection.channel()
        # Cleanup must not mask the real failure: a leftover queue on a dev
        # broker is harmless, a `teardown error` hiding an assertion is not.
        for queue_name in (chosen.queue, chosen.dead_letter_queue):
            with contextlib.suppress(Exception):
                await channel.queue_delete(queue_name)
        for exchange_name in (chosen.exchange, chosen.dead_letter_exchange):
            with contextlib.suppress(Exception):
                await channel.exchange_delete(exchange_name)


@pytest.fixture
async def broker_channel() -> AsyncIterator[AbstractChannel]:
    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        yield await connection.channel()


def build_consumer(names: Names) -> RabbitMQEventConsumer:
    return RabbitMQEventConsumer(
        url=BROKER_URL,
        exchange_name=names.exchange,
        queue_name=names.queue,
        dead_letter_exchange=names.dead_letter_exchange,
        dead_letter_queue=names.dead_letter_queue,
        routing_keys=("order.reserved",),
        prefetch_count=1,
        max_delivery_count=MAX_DELIVERY_COUNT,
    )


def journal_handler(
    session_factory: async_sessionmaker[AsyncSession],
) -> InboxConsumer:
    return InboxConsumer(
        session_factory=session_factory,
        stop=asyncio.Event(),
        requeue_delay_seconds=0.0,
        max_delivery_count=MAX_DELIVERY_COUNT,
        report_interval_seconds=60.0,
    )


async def publish(
    channel: AbstractChannel,
    exchange_name: str,
    *,
    body: bytes,
    message_id: str,
    routing_key: str = "order.reserved",
) -> None:
    exchange = await channel.declare_exchange(
        exchange_name,
        ExchangeType.TOPIC,
        durable=True,
    )
    await exchange.publish(
        Message(
            body=body,
            message_id=message_id,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key,
        mandatory=True,
    )


async def queue_depth(queue_name: str) -> int:
    """Read the depth with a *fresh* connection.

    A passive declare on a channel that has already been used reports a stale
    depth (0 on the same channel, 1 on another), which would make a draining
    assertion pass for the wrong reason.
    """
    connection = await aio_pika.connect_robust(BROKER_URL)
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, passive=True)
        return queue.declaration_result.message_count
    finally:
        await connection.close()


async def take_one(queue_name: str) -> AbstractIncomingMessage | None:
    connection = await aio_pika.connect_robust(BROKER_URL)
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, passive=True)
        return await queue.get(no_ack=True, fail=False)
    finally:
        await connection.close()


async def inbox_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[NotificationInbox]:
    async with session_factory() as session:
        return list(await session.scalars(select(NotificationInbox)))


async def eventually(
    predicate: Callable[[], Awaitable[Any]],
    *,
    timeout: float = 10.0,
) -> Any:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(0.05)
    pytest.fail("the expected broker/database state was not reached in time")


def envelope(*, event_id: uuid.UUID, order_id: uuid.UUID) -> bytes:
    message: dict[str, Any] = {
        "schema_version": 1,
        "event_id": str(event_id),
        "event_type": "order.reserved",
        "aggregate_id": str(order_id),
        "occurred_at": "2026-11-19T12:30:00+00:00",
        "payload": {"order_id": str(order_id), "status": "reserved"},
    }
    return json.dumps(message).encode()


async def drained(queue_name: str) -> bool:
    return await queue_depth(queue_name) == 0


async def test_a_published_event_is_recorded_and_acknowledged(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    names: Names,
) -> None:
    consumer = build_consumer(names)
    await consumer.start(journal_handler(session_factory))
    try:
        event_id, order_id = uuid.uuid4(), uuid.uuid4()

        await publish(
            broker_channel,
            names.exchange,
            body=envelope(event_id=event_id, order_id=order_id),
            message_id=str(event_id),
        )

        [row] = await eventually(lambda: inbox_rows(session_factory))

        # The same fact on both sides of the broker: the inbox stores what the
        # producer published, and `event_id` is the deduplication key.
        assert row.event_id == event_id
        assert row.aggregate_id == order_id
        assert row.event_type == "order.reserved"
        assert row.payload == {"order_id": str(order_id), "status": "reserved"}

        # The message was acknowledged only after the commit, so the queue drains.
        assert await eventually(lambda: drained(names.queue))
    finally:
        await consumer.aclose()


async def test_a_duplicate_delivery_leaves_one_row(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    names: Names,
) -> None:
    consumer = build_consumer(names)
    await consumer.start(journal_handler(session_factory))
    try:
        event_id, order_id = uuid.uuid4(), uuid.uuid4()
        body = envelope(event_id=event_id, order_id=order_id)

        # The same at-least-once message, delivered twice.
        await publish(
            broker_channel,
            names.exchange,
            body=body,
            message_id=str(event_id),
        )
        await publish(
            broker_channel,
            names.exchange,
            body=body,
            message_id=str(event_id),
        )

        rows = await eventually(lambda: inbox_rows(session_factory))
        # A duplicate is a success that wrote nothing: the primary key absorbs it.
        assert [row.event_id for row in rows] == [event_id]
        assert await eventually(lambda: drained(names.queue))
    finally:
        await consumer.aclose()


async def test_a_malformed_message_is_dead_lettered(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    names: Names,
) -> None:
    consumer = build_consumer(names)
    await consumer.start(journal_handler(session_factory))
    try:
        await publish(
            broker_channel,
            names.exchange,
            body=b"not json",
            message_id="bad-envelope",
        )

        dead = await eventually(
            lambda: take_one(names.dead_letter_queue),
            timeout=15.0,
        )
        assert dead.message_id == "bad-envelope"
        # The DLX, not the main queue: the consumer rejected it without requeue.
        assert dead.headers is not None
        assert "x-death" in dead.headers
        assert await inbox_rows(session_factory) == []
    finally:
        await consumer.aclose()
