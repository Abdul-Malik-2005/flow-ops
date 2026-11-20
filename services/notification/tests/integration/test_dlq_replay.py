"""DLQ replay against a real broker: a dead letter moves back into the queue.

The unit test proves the decision table on fakes; this one proves the wire: the
message leaves `notification.order-events.dlq`, is republished to
`notification.order-events` (not to `order.events`), and arrives there without the
broker's old delivery headers.

Flag-gated like the other broker tests:

    $env:NOTIFICATION_RABBITMQ_IT = "1"
    $env:RABBITMQ_URL = "amqp://flowops:dev_password@localhost:5672/"
    poetry run pytest tests/integration/test_dlq_replay.py -q

Every test gets its own queue and DLQ, deleted afterwards.
"""

from __future__ import annotations

import contextlib
import json
import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

import aio_pika
import pytest
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractIncomingMessage

from app.dlq_replay import ReplayReport, replay_dead_letters

pytestmark = pytest.mark.integration

BROKER_URL = os.getenv("RABBITMQ_URL", "")

if os.getenv("NOTIFICATION_RABBITMQ_IT") != "1" or not BROKER_URL:
    pytest.skip(
        "end-to-end DLQ replay test: set NOTIFICATION_RABBITMQ_IT=1 and "
        "RABBITMQ_URL to run it",
        allow_module_level=True,
    )


@dataclass(frozen=True)
class Names:
    queue: str
    dlq: str


@pytest.fixture
async def names() -> AsyncIterator[Names]:
    suffix = uuid.uuid4().hex
    chosen = Names(
        queue=f"notification.e2e.replay.{suffix}",
        dlq=f"notification.e2e.replay.dlq.{suffix}",
    )
    yield chosen

    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        channel = await connection.channel()
        for queue_name in (chosen.queue, chosen.dlq):
            with contextlib.suppress(Exception):
                await channel.queue_delete(queue_name)


def envelope(*, event_id: uuid.UUID, order_id: uuid.UUID) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "event_id": str(event_id),
            "event_type": "order.reserved",
            "aggregate_id": str(order_id),
            "occurred_at": "2026-11-19T12:30:00+00:00",
            "payload": {"order_id": str(order_id), "status": "reserved"},
        }
    ).encode()


async def queue_depth(name: str) -> int:
    """Read the depth with a fresh connection (a used channel reports stale data)."""
    connection = await aio_pika.connect_robust(BROKER_URL)
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(name, passive=True)
        return queue.declaration_result.message_count
    finally:
        await connection.close()


async def take_one(name: str) -> AbstractIncomingMessage | None:
    connection = await aio_pika.connect_robust(BROKER_URL)
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(name, passive=True)
        return await queue.get(no_ack=True, fail=False)
    finally:
        await connection.close()


async def test_a_dead_letter_is_replayed_into_the_own_queue(names: Names) -> None:
    event_id, order_id = uuid.uuid4(), uuid.uuid4()
    body = envelope(event_id=event_id, order_id=order_id)

    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(names.queue, durable=True)
        await channel.declare_queue(names.dlq, durable=True)
        # Put the message straight into the DLQ: this test is about the move out
        # of it, not about how a message got dead-lettered.
        await channel.default_exchange.publish(
            Message(
                body=body,
                message_id=str(event_id),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key=names.dlq,
        )

    report = await replay_dead_letters(
        url=BROKER_URL,
        dead_letter_queue=names.dlq,
        target_queue=names.queue,
        limit=10,
    )

    assert report == ReplayReport(examined=1, replayed=1)

    replayed = await take_one(names.queue)
    assert replayed is not None
    assert replayed.message_id == str(event_id)
    assert replayed.body == body
    # The broker's old delivery metadata is gone: a copied `x-death` would make
    # the consumer dead-letter this fresh message immediately.
    assert "x-death" not in (replayed.headers or {})
    # The DLQ is drained: the message was acked only after the republish.
    assert await queue_depth(names.dlq) == 0
