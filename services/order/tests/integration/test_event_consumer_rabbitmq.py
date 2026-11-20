"""The consumer and the broker together: delivery → journal → ack, over real AMQP.

Neither half proves this alone. The unit tests use a fake message and a fake
session, so they cannot show that RabbitMQ really counts redeliveries; the
projection integration tests never touch a message. Here a published message goes
through the real topology, the real `EventJournalConsumer` and a real PostgreSQL:

* a delivered event is journaled once and acknowledged (the queue drains);
* the same `event_id` delivered twice leaves one row and acknowledges both
  copies — at-least-once plus a primary key;
* a deterministic refusal (bad JSON) lands in the DLQ immediately, with nothing
  written;
* a transient failure is requeued and succeeds on the second attempt;
* a failure that never resolves is dead-lettered by the broker's
  `x-delivery-limit`, not by an endless loop.

Flag-gated like the other broker tests: CI has no broker, and an integration test
that silently skipped there while failing on a laptop would be worse than an
explicit flag. Run it against the compose services:

    $env:ORDER_RABBITMQ_IT = "1"
    $env:RABBITMQ_URL = "amqp://flowops:dev_password@localhost:5672/"
    $env:TEST_DATABASE_URL = "postgresql+asyncpg://.../order_test_db"
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
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

import aio_pika
import pytest
from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractChannel, AbstractExchange
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumers.rabbitmq import RabbitMQEventConsumer
from app.domain.events import OrderEventType
from app.event_consumer import EventJournalConsumer, RecordProjection
from app.models.order_event_projection import OrderEventProjection
from app.repositories.projection import SqlProjectionRepository
from app.services.event_projection import OrderEventProjector

pytestmark = pytest.mark.integration

BROKER_URL = os.getenv("RABBITMQ_URL", "")

if os.getenv("ORDER_RABBITMQ_IT") != "1" or not BROKER_URL:
    pytest.skip(
        "end-to-end consumer test: set ORDER_RABBITMQ_IT=1, RABBITMQ_URL and "
        "TEST_DATABASE_URL to run it",
        allow_module_level=True,
    )

ROUTING_KEY = OrderEventType.RESERVED.value


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
        queue=f"order.event-projections.it.{suffix}",
        dead_letter_exchange=f"order.events.dlx.it.{suffix}",
        dead_letter_queue=f"order.event-projections.dlq.it.{suffix}",
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
    """A separate connection for the test side; the consumer owns its own."""
    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        yield await connection.channel()


@pytest.fixture
async def exchange(
    broker_channel: AbstractChannel,
    names: Names,
) -> AbstractExchange:
    # The same declaration the producer and the consumer make: a mismatch in
    # type or durability would fail here instead of in production.
    return await broker_channel.declare_exchange(
        names.exchange,
        ExchangeType.TOPIC,
        durable=True,
    )


@pytest.fixture
async def build_consumer(
    names: Names,
) -> AsyncIterator[Callable[..., RabbitMQEventConsumer]]:
    """Create consumers with the real adapter; close every one in teardown."""
    consumers: list[RabbitMQEventConsumer] = []

    def build(*, max_delivery_count: int = 3) -> RabbitMQEventConsumer:
        consumer = RabbitMQEventConsumer(
            url=BROKER_URL,
            exchange_name=names.exchange,
            queue_name=names.queue,
            dead_letter_exchange=names.dead_letter_exchange,
            dead_letter_queue=names.dead_letter_queue,
            routing_keys=(ROUTING_KEY,),
            prefetch_count=1,
            max_delivery_count=max_delivery_count,
        )
        consumers.append(consumer)
        return consumer

    yield build

    for consumer in consumers:
        await consumer.aclose()


def journal_handler(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    record: RecordProjection | None = None,
    max_delivery_count: int = 3,
) -> EventJournalConsumer:
    """The runtime callback with the real database and a zero pause.

    The pause is how production keeps a database outage from becoming a hot loop;
    here it is zero so a test measures the broker's policy, not the clock.
    """
    options: dict[str, Any] = {} if record is None else {"record": record}
    return EventJournalConsumer(
        session_factory=session_factory,
        stop=asyncio.Event(),
        requeue_delay_seconds=0.0,
        max_delivery_count=max_delivery_count,
        report_interval_seconds=60.0,
        **options,
    )


async def journal_record(
    session: AsyncSession,
    payload: Mapping[str, Any],
) -> bool:
    return await OrderEventProjector(SqlProjectionRepository(session)).handle(payload)


async def publish(
    exchange: AbstractExchange,
    *,
    body: bytes,
    message_id: str,
) -> None:
    await exchange.publish(
        Message(
            body=body,
            content_type="application/json",
            message_id=message_id,
            type=ROUTING_KEY,
        ),
        routing_key=ROUTING_KEY,
    )


def envelope(*, event_id: uuid.UUID, order_id: uuid.UUID) -> bytes:
    """A message shaped exactly like `encode_event`, but built locally.

    The test must not depend on the outbox: it exercises the consumer, and the
    relay already has its own end-to-end test.
    """
    payload = {
        "order_id": str(order_id),
        "user_id": str(uuid.uuid4()),
        "status": "reserved",
        "currency": "RUB",
        "total_amount": "1990.00",
        "items": [],
        "inventory_reservation_id": str(uuid.uuid4()),
    }
    message = {
        "schema_version": 1,
        "event_id": str(event_id),
        "event_type": ROUTING_KEY,
        "aggregate_id": str(order_id),
        "occurred_at": "2026-11-19T12:30:00+00:00",
        "payload": payload,
    }
    return json.dumps(message).encode()


async def journal_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[OrderEventProjection]:
    async with session_factory() as session:
        return list(await session.scalars(select(OrderEventProjection)))


async def queue_depth(channel: AbstractChannel, name: str) -> int:
    """Ready messages in a queue; `passive` never creates or changes it."""
    queue = await channel.declare_queue(name, passive=True)
    return queue.declaration_result.message_count


async def eventually(
    predicate: Callable[[], Awaitable[Any]],
    *,
    timeout: float = 10.0,
) -> Any:
    """Poll until the predicate is truthy, or fail with a useful message."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(0.05)
    pytest.fail("the expected broker/database state was not reached in time")


async def take_one(channel: AbstractChannel, name: str) -> Any:
    """Take one ready message, or `None`: the queue survives without consuming."""
    queue = await channel.declare_queue(name, passive=True)
    return await queue.get(no_ack=True, fail=False, timeout=0.5)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_a_delivered_event_is_journaled_and_acknowledged(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    exchange: AbstractExchange,
    names: Names,
    build_consumer: Callable[..., RabbitMQEventConsumer],
) -> None:
    event_id, order_id = uuid.uuid4(), uuid.uuid4()
    consumer = build_consumer(max_delivery_count=3)
    await consumer.start(journal_handler(session_factory))

    await publish(
        exchange,
        body=envelope(event_id=event_id, order_id=order_id),
        message_id=str(event_id),
    )

    [row] = await eventually(lambda: journal_rows(session_factory))
    assert row.event_id == event_id
    assert row.aggregate_id == order_id
    assert row.event_type == ROUTING_KEY
    assert row.payload["total_amount"] == "1990.00"
    assert row.occurred_at is not None

    # Acknowledged, not merely delivered: closing the consumer returns every
    # unacknowledged message to the queue, so an empty queue proves the verdict
    # reached the broker.
    await consumer.aclose()
    assert await queue_depth(broker_channel, names.queue) == 0


# ---------------------------------------------------------------------------
# At-least-once: the same event twice
# ---------------------------------------------------------------------------


async def test_a_duplicate_delivery_leaves_one_row_and_is_acknowledged(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    exchange: AbstractExchange,
    names: Names,
    build_consumer: Callable[..., RabbitMQEventConsumer],
) -> None:
    event_id, order_id = uuid.uuid4(), uuid.uuid4()
    consumer = build_consumer(max_delivery_count=3)
    await consumer.start(journal_handler(session_factory))
    body = envelope(event_id=event_id, order_id=order_id)

    await publish(exchange, body=body, message_id=str(event_id))
    await publish(exchange, body=body, message_id=str(event_id))

    # Both copies are consumed; the primary key makes the second one a no-op.
    # Waiting on the journal, not on the queue depth: the row count is the fact
    # under test, and the second copy may still be in flight when the first row
    # appears.
    [row] = await eventually(lambda: journal_rows(session_factory))
    assert row.event_id == event_id
    rows = await journal_rows(session_factory)
    assert [stored.event_id for stored in rows] == [event_id]


# ---------------------------------------------------------------------------
# Deterministic refusal → DLQ, no retry
# ---------------------------------------------------------------------------


async def test_a_malformed_message_is_dead_lettered_immediately(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    exchange: AbstractExchange,
    names: Names,
    build_consumer: Callable[..., RabbitMQEventConsumer],
) -> None:
    consumer = build_consumer(max_delivery_count=3)
    await consumer.start(journal_handler(session_factory))

    await publish(exchange, body=b"not json", message_id=str(uuid.uuid4()))

    dead = await eventually(lambda: take_one(broker_channel, names.dead_letter_queue))
    assert dead.body == b"not json"
    # The DLX, not the main queue: the broker moved it because the consumer
    # rejected it without requeue.
    assert dead.headers is not None
    assert "x-death" in dead.headers
    assert await journal_rows(session_factory) == []


# ---------------------------------------------------------------------------
# Transient failure → requeue → success
# ---------------------------------------------------------------------------


async def test_a_transient_failure_is_requeued_and_then_succeeds(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    exchange: AbstractExchange,
    names: Names,
    build_consumer: Callable[..., RabbitMQEventConsumer],
) -> None:
    attempts: list[Mapping[str, Any]] = []

    async def flaky(
        session: AsyncSession,
        payload: Mapping[str, Any],
    ) -> bool:
        attempts.append(payload)
        if len(attempts) == 1:
            raise RuntimeError("the database is down")
        return await journal_record(session, payload)

    event_id, order_id = uuid.uuid4(), uuid.uuid4()
    consumer = build_consumer(max_delivery_count=3)
    await consumer.start(journal_handler(session_factory, record=flaky))

    await publish(
        exchange,
        body=envelope(event_id=event_id, order_id=order_id),
        message_id=str(event_id),
    )

    [row] = await eventually(lambda: journal_rows(session_factory))
    assert row.event_id == event_id
    # Two attempts: the failure requeued the message, the retry committed it.
    assert len(attempts) == 2
    # Nothing was dead-lettered: one failure is a retry, not a poison message.
    assert await queue_depth(broker_channel, names.dead_letter_queue) == 0


# ---------------------------------------------------------------------------
# Permanent failure → the broker's delivery limit → DLQ
# ---------------------------------------------------------------------------


async def test_a_permanent_failure_is_dead_lettered_by_the_broker(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    exchange: AbstractExchange,
    names: Names,
    build_consumer: Callable[..., RabbitMQEventConsumer],
) -> None:
    attempts: list[int] = []

    async def always_failing(
        _session: AsyncSession,
        _payload: Mapping[str, Any],
    ) -> bool:
        attempts.append(1)
        raise RuntimeError("still down")

    event_id, order_id = uuid.uuid4(), uuid.uuid4()
    consumer = build_consumer(max_delivery_count=2)
    await consumer.start(
        journal_handler(
            session_factory,
            record=always_failing,
            max_delivery_count=2,
        )
    )

    await publish(
        exchange,
        body=envelope(event_id=event_id, order_id=order_id),
        message_id=str(event_id),
    )

    # The runtime, not the broker, stops the retry: RabbitMQ 4 reports the
    # acquisition count in `x-acquired-count` but does not apply
    # `x-delivery-limit` to an explicit requeue, so the last allowed delivery is
    # refused without requeue and the DLX moves it to the DLQ.
    dead = await eventually(
        lambda: take_one(broker_channel, names.dead_letter_queue),
        timeout=15.0,
    )
    assert dead.message_id == str(event_id)
    assert len(attempts) == 2
    assert await journal_rows(session_factory) == []
