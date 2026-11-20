"""The DLQ replay tool against a real broker.

The fakes prove the decision table; this proves the AMQP path: a message moved
into a DLQ with the default exchange is republished by the tool onto the real
topic exchange, lands in a bound queue with its body and `message_id` intact, and
disappears from the DLQ only then. A body without `event_type` stays where it is.

Flag-gated like the other broker tests:

    $env:ORDER_RABBITMQ_IT = "1"
    $env:RABBITMQ_URL = "amqp://flowops:dev_password@localhost:5672/"
    poetry run pytest tests/integration/test_dlq_replay.py -q
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
from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractChannel

from app.dlq_replay import ReplayReport, replay_dead_letters

pytestmark = pytest.mark.integration

BROKER_URL = os.getenv("RABBITMQ_URL", "")

if os.getenv("ORDER_RABBITMQ_IT") != "1" or not BROKER_URL:
    pytest.skip(
        "end-to-end DLQ replay test: set ORDER_RABBITMQ_IT=1 and RABBITMQ_URL "
        "to run it",
        allow_module_level=True,
    )


@dataclass(frozen=True)
class Names:
    exchange: str
    queue: str
    dead_letter_queue: str


@pytest.fixture
async def names() -> AsyncIterator[Names]:
    suffix = uuid.uuid4().hex
    chosen = Names(
        exchange=f"order.events.it.{suffix}",
        queue=f"order.event.projections.it.{suffix}",
        dead_letter_queue=f"order.event.projections.dlq.it.{suffix}",
    )
    yield chosen

    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        channel = await connection.channel()
        for queue_name in (chosen.queue, chosen.dead_letter_queue):
            with contextlib.suppress(Exception):
                await channel.queue_delete(queue_name)
        with contextlib.suppress(Exception):
            await channel.exchange_delete(chosen.exchange)


@pytest.fixture
async def channel() -> AsyncIterator[AbstractChannel]:
    connection = await aio_pika.connect_robust(BROKER_URL)
    async with connection:
        yield await connection.channel()


async def queue_depth(name: str) -> int:
    """Ready messages, read over a fresh connection.

    A `passive` declare on a channel that has already used the queue reports a
    stale depth (verified: the same channel says 0 while another connection sees
    the requeued message). Polling semantics matter here, so every read gets its
    own connection.
    """
    connection = await aio_pika.connect_robust(BROKER_URL)
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(name, passive=True)
        return queue.declaration_result.message_count
    finally:
        await connection.close()


async def has_depth(name: str, expected: int) -> bool:
    return await queue_depth(name) == expected


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
    pytest.fail("the expected broker state was not reached in time")


def envelope() -> bytes:
    return json.dumps(
        {
            "event_id": "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa",
            "schema_version": 1,
            "event_type": "order.reserved",
            "aggregate_id": "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb",
            "occurred_at": "2026-11-19T12:30:00+00:00",
            "payload": {"order_id": "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"},
        }
    ).encode()


async def test_a_dead_letter_is_republished_to_a_bound_queue(
    names: Names,
    channel: AbstractChannel,
) -> None:
    exchange = await channel.declare_exchange(
        names.exchange,
        ExchangeType.TOPIC,
        durable=True,
    )
    queue = await channel.declare_queue(names.queue, durable=True)
    await queue.bind(exchange, routing_key="order.reserved")
    await channel.declare_queue(names.dead_letter_queue, durable=True)

    body = envelope()
    # The default exchange routes by queue name: this is exactly how a rejected
    # message reaches the DLQ in the first place.
    await channel.default_exchange.publish(
        Message(body=body, message_id="event-1", content_type="application/json"),
        routing_key=names.dead_letter_queue,
    )

    report = await replay_dead_letters(
        url=BROKER_URL,
        exchange_name=names.exchange,
        queue_name=names.dead_letter_queue,
        limit=10,
    )

    assert report == ReplayReport(examined=1, replayed=1)

    message = await queue.get(no_ack=True, timeout=5)
    assert message is not None
    assert message.body == body
    assert message.message_id == "event-1"
    assert message.routing_key == "order.reserved"

    # The DLQ is empty only because the publish was confirmed before the ack.
    assert await queue_depth(names.dead_letter_queue) == 0


async def test_a_body_without_an_event_type_stays_in_the_dlq(
    names: Names,
    channel: AbstractChannel,
) -> None:
    await channel.declare_exchange(names.exchange, ExchangeType.TOPIC, durable=True)
    await channel.declare_queue(names.dead_letter_queue, durable=True)
    await channel.default_exchange.publish(
        Message(body=b"not json", message_id="broken"),
        routing_key=names.dead_letter_queue,
    )

    report = await replay_dead_letters(
        url=BROKER_URL,
        exchange_name=names.exchange,
        queue_name=names.dead_letter_queue,
        limit=10,
    )

    assert report == ReplayReport(examined=1, skipped=1)
    # Still there, with nothing published: the operator gets a report, not a
    # silently discarded message. The requeue is asynchronous, so the depth is
    # polled rather than read once.
    assert await eventually(lambda: has_depth(names.dead_letter_queue, 1))
