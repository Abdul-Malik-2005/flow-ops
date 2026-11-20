"""The whole pipeline over real infrastructure: outbox → relay → queue → journal.

Each half is proved elsewhere; this test is the hand-off. It is what makes
`OUTBOX_PUBLISHER=rabbitmq` in compose safe: a committed outbox row is published
by the real relay through the real broker, consumed by the real runtime and
journaled exactly once — and the row is marked published only after the broker
confirmed the message.

Flag-gated like the other broker tests:

    $env:ORDER_RABBITMQ_IT = "1"
    $env:RABBITMQ_URL = "amqp://flowops:dev_password@localhost:5672/"
    $env:TEST_DATABASE_URL = "postgresql+asyncpg://.../order_test_db"
    poetry run pytest tests/integration/test_outbox_to_journal.py -q
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aio_pika
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumers.rabbitmq import RabbitMQEventConsumer
from app.event_consumer import EventJournalConsumer
from app.models.order_event_projection import OrderEventProjection
from app.publishers.rabbitmq import RabbitMQEventPublisher
from app.services.outbox_relay import OutboxRelay, RelayReport
from tests.integration.helpers import insert_outbox_event, outbox_rows

pytestmark = pytest.mark.integration

BROKER_URL = os.getenv("RABBITMQ_URL", "")

if os.getenv("ORDER_RABBITMQ_IT") != "1" or not BROKER_URL:
    pytest.skip(
        "end-to-end outbox test: set ORDER_RABBITMQ_IT=1, RABBITMQ_URL and "
        "TEST_DATABASE_URL to run it",
        allow_module_level=True,
    )

BATCH_SIZE = 10
LEASE_SECONDS = 30.0
MAX_ATTEMPTS = 3
MAX_DELIVERY_COUNT = 3


@dataclass(frozen=True)
class Names:
    """Per-test topology: nothing here touches the shared `order.events`."""

    exchange: str
    queue: str
    dead_letter_exchange: str
    dead_letter_queue: str


@pytest.fixture
async def names() -> AsyncIterator[Names]:
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


async def journal_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[OrderEventProjection]:
    async with session_factory() as session:
        return list(await session.scalars(select(OrderEventProjection)))


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


async def test_a_committed_outbox_event_reaches_the_journal(
    session_factory: async_sessionmaker[AsyncSession],
    names: Names,
) -> None:
    publisher = RabbitMQEventPublisher(url=BROKER_URL, exchange=names.exchange)
    relay = OutboxRelay(
        session_factory=session_factory,
        publisher=publisher,
        batch_size=BATCH_SIZE,
        lease_seconds=LEASE_SECONDS,
        max_attempts=MAX_ATTEMPTS,
    )
    consumer = RabbitMQEventConsumer(
        url=BROKER_URL,
        exchange_name=names.exchange,
        queue_name=names.queue,
        dead_letter_exchange=names.dead_letter_exchange,
        dead_letter_queue=names.dead_letter_queue,
        routing_keys=("order.reserved",),
        prefetch_count=1,
        max_delivery_count=MAX_DELIVERY_COUNT,
    )
    handler = EventJournalConsumer(
        session_factory=session_factory,
        stop=asyncio.Event(),
        requeue_delay_seconds=0.0,
        max_delivery_count=MAX_DELIVERY_COUNT,
        report_interval_seconds=60.0,
    )

    try:
        # The consumer owns the queue, so it must be bound before the relay
        # publishes: with no queue, `mandatory=True` would fail the publish — the
        # behaviour the `log` default existed to avoid.
        await consumer.start(handler)
        event_id = await insert_outbox_event(session_factory)

        report = await relay.process_batch()

        assert report == RelayReport(claimed=1, published=1)
        [row] = await eventually(lambda: journal_rows(session_factory))
        [stored] = await outbox_rows(session_factory)

        # The same fact on both sides of the broker: the journal stores what the
        # relay published, and the id is the deduplication key.
        assert row.event_id == event_id == stored.id
        assert row.event_type == stored.event_type == "order.reserved"
        assert row.aggregate_id == stored.aggregate_id
        assert row.payload == stored.payload
        assert row.occurred_at == stored.occurred_at

        # Marked only after the broker confirmed, and the attempt is accounted.
        assert stored.published_at is not None
        assert stored.attempts == 1
    finally:
        # The relay owns its transport: closing it closes the publisher. The
        # consumer owns its own connection.
        try:
            await relay.aclose()
        finally:
            await consumer.aclose()
