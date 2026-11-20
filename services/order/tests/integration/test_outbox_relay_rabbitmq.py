"""The relay and the broker together: claim → publish → mark, over real AMQP.

Neither half proves this on its own. `tests/integration/test_outbox_relay.py`
uses a recording publisher, so it cannot show that a returned `publish()` really
means "the broker has it"; `tests/integration/test_rabbitmq_publisher.py` builds
the event in memory, so it never touches the outbox. Here a committed row travels
the whole way, and the interesting cases are the ones only this combination
exposes:

* the message in a bound queue carries exactly the stored row — the body matches
  `encode_event`, the id is the `event_id` a consumer deduplicates by;
* an event with no consumer is **not** marked published: `mandatory=True` turns
  it into an exception, the attempt is spent, and the row waits for the lease —
  then a queue appears and the same row goes through;
* a second pass finds nothing, so one row never becomes two messages.

Flag-gated like the publisher test: CI has no broker, and a test that silently
skipped there while failing on a laptop would be worse than an explicit flag.
Run it against the compose services (both the broker and the database):

    $env:ORDER_RABBITMQ_IT = "1"
    $env:RABBITMQ_URL = "amqp://flowops:dev_password@localhost:5672/"
    $env:TEST_DATABASE_URL = "postgresql+asyncpg://.../order_test_db"
    poetry run pytest tests/integration/test_outbox_relay_rabbitmq.py -q

Every test gets its own exchange and queue, so runs never collide and the shared
`order.events` exchange is left untouched. The queue is declared here only
because this file plays the consumer; production code never declares one — a
producer must not know subscriber queue names.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import AsyncIterator

import aio_pika
import pytest
from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.publishers.envelope import encode_event
from app.publishers.rabbitmq import RabbitMQEventPublisher
from app.services.outbox_relay import OutboxRelay, RelayReport
from tests.integration.helpers import (
    age_outbox_lease,
    insert_outbox_event,
    outbox_rows,
)

pytestmark = pytest.mark.integration

BROKER_URL = os.getenv("RABBITMQ_URL", "")

if os.getenv("ORDER_RABBITMQ_IT") != "1" or not BROKER_URL:
    pytest.skip(
        "end-to-end relay test: set ORDER_RABBITMQ_IT=1, RABBITMQ_URL and "
        "TEST_DATABASE_URL to run it",
        allow_module_level=True,
    )

BATCH_SIZE = 10
LEASE_SECONDS = 30.0
MAX_ATTEMPTS = 3


def build_relay(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: RabbitMQEventPublisher,
) -> OutboxRelay:
    """The worker's wiring, with the real transport."""
    return OutboxRelay(
        session_factory=session_factory,
        publisher=publisher,
        batch_size=BATCH_SIZE,
        lease_seconds=LEASE_SECONDS,
        max_attempts=MAX_ATTEMPTS,
    )


async def subscribe(
    channel: AbstractChannel,
    exchange_name: str,
) -> AbstractQueue:
    """Play the consumer: declare the exchange idempotently, then a queue.

    The exchange is declared with the same arguments the publisher uses, so a
    mismatch (type, durability) fails here instead of in production. The queue is
    exclusive: the broker deletes it when this test's channel closes, which keeps
    the dev broker from collecting `it` queues.
    """
    exchange: AbstractExchange = await channel.declare_exchange(
        exchange_name, ExchangeType.TOPIC, durable=True
    )
    queue = await channel.declare_queue(f"{exchange_name}.q", exclusive=True)
    await queue.bind(exchange, routing_key="order.#")
    return queue


@pytest.fixture
async def exchange_name() -> AsyncIterator[str]:
    """A throwaway exchange, deleted after the test even if the test failed."""
    name = f"order.events.it.{uuid.uuid4().hex}"
    yield name
    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        channel = await connection.channel()
        # Cleanup must not mask the real failure: a leftover exchange on a dev
        # broker is harmless, a `teardown error` hiding an assertion is not.
        with contextlib.suppress(Exception):
            await channel.exchange_delete(name)


@pytest.fixture
async def broker_channel() -> AsyncIterator[AbstractChannel]:
    """A separate connection for the consumer side, so the publisher owns its own."""
    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        yield await connection.channel()


@pytest.fixture
async def publisher(exchange_name: str) -> AsyncIterator[RabbitMQEventPublisher]:
    """The real adapter; closed even when the test fails mid-flight."""
    adapter = RabbitMQEventPublisher(url=BROKER_URL, exchange=exchange_name)
    try:
        yield adapter
    finally:
        await adapter.aclose()


# ---------------------------------------------------------------------------
# The happy path: a committed row becomes two messages
# ---------------------------------------------------------------------------


async def test_committed_events_reach_a_bound_queue_and_are_marked(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    exchange_name: str,
    publisher: RabbitMQEventPublisher,
) -> None:
    queue = await subscribe(broker_channel, exchange_name)
    reserved_id = await insert_outbox_event(
        session_factory, event_type="order.reserved"
    )
    failed_id = await insert_outbox_event(session_factory, event_type="order.failed")

    report = await build_relay(session_factory, publisher).process_batch()

    assert report == RelayReport(claimed=2, published=2)

    messages = [await queue.get(no_ack=True, timeout=5) for _ in range(2)]
    assert all(message is not None for message in messages)
    by_id = {uuid.UUID(message.message_id): message for message in messages}
    assert set(by_id) == {reserved_id, failed_id}

    stored = {row.id: row for row in await outbox_rows(session_factory)}
    for event_id, message in by_id.items():
        row = stored[event_id]
        # The bytes on the wire are the stored row, not a re-derived event: what
        # the transaction committed is what the consumer reads.
        assert message.body == encode_event(row).encode("utf-8")
        assert message.routing_key == row.event_type
        assert message.type == row.event_type
        assert message.headers == {"aggregate_id": str(row.aggregate_id)}
        assert row.published_at is not None
        assert row.attempts == 1


# ---------------------------------------------------------------------------
# No consumer: the row must not be marked
# ---------------------------------------------------------------------------


async def test_an_event_without_a_consumer_is_not_marked_published(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    exchange_name: str,
    publisher: RabbitMQEventPublisher,
) -> None:
    """The default deployment, and the reason `mandatory=True` is mandatory.

    With `outbox_publisher=rabbitmq` and nothing bound, every publish would be an
    unroutable drop. `mandatory=True` turns that into `EventNotDelivered`, so the
    row stays unpublished and the attempt budget is spent honestly.
    """
    event_id = await insert_outbox_event(session_factory)

    report = await build_relay(session_factory, publisher).process_batch()

    assert report == RelayReport(claimed=1, failed=1)
    [stored] = await outbox_rows(session_factory)
    assert stored.id == event_id
    assert stored.published_at is None
    assert stored.attempts == 1
    # Leased: the next pass cannot hot-loop over a broker nobody listens on.
    assert stored.locked_at is not None

    # A consumer appears later, and the lease expires: the row goes through on
    # the second attempt instead of being lost.
    queue = await subscribe(broker_channel, exchange_name)
    await age_outbox_lease(
        session_factory, event_id=event_id, seconds=LEASE_SECONDS + 1
    )

    recovered = await build_relay(session_factory, publisher).process_batch()

    assert recovered == RelayReport(claimed=1, published=1)
    message = await queue.get(no_ack=True, timeout=5)
    assert message is not None
    assert message.message_id == str(event_id)
    [after_retry] = await outbox_rows(session_factory)
    assert after_retry.published_at is not None
    assert after_retry.attempts == 2


# ---------------------------------------------------------------------------
# One row, one message
# ---------------------------------------------------------------------------


async def test_a_second_pass_does_not_publish_the_row_again(
    session_factory: async_sessionmaker[AsyncSession],
    broker_channel: AbstractChannel,
    exchange_name: str,
    publisher: RabbitMQEventPublisher,
) -> None:
    queue = await subscribe(broker_channel, exchange_name)
    await insert_outbox_event(session_factory)
    relay = build_relay(session_factory, publisher)

    await relay.process_batch()
    second = await relay.process_batch()

    assert second == RelayReport()
    # Exactly one message: the mark committed before the second claim looked at
    # the feed, so the same row never becomes two messages in the normal path.
    assert await queue.get(no_ack=True, timeout=5) is not None
    assert await queue.get(no_ack=True, fail=False, timeout=1) is None

    # The worker owns its transport: closing the relay closes the connection.
    await relay.aclose()
    assert publisher._connection is None
