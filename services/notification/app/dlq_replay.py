"""Operational tool: replay a consumer's own dead letters after the cause is fixed.

A dead letter is a message this consumer refused (broken envelope, unknown type,
a schema version it cannot read) or a message whose delivery budget ran out.
Fixing the cause does not help the copy already sitting in the DLQ, so this tool
moves it back for reprocessing.

The one decision that differs from Order's producer-side replay: **a consumer
replays into its own queue, not into the producer's exchange.** Notification does
not own `order.events` and never publishes a fact there; republishing to that
topic exchange would re-broadcast another service's events to every bound queue
(Order's journal, future consumers). Publishing to `notification.order-events` via
the *default exchange* (routing key = queue name) re-delivers exactly to this
consumer's input, which is the whole point of a DLQ replay here.

Guarantees, in order of importance:

* **`ack` only after the broker confirmed.** The message is published with
  publisher confirms and `mandatory=True`; a crash or a routing failure leaves it
  in the DLQ and the next run picks it up.
* **A body that is not a parseable envelope is never replayed.** Sending a blob
  back would only bounce it into the DLQ again; it is returned and reported for a
  human instead.
* **Broker headers are not copied.** `x-death` and `x-acquired-count` describe the
  *previous* deliveries; carrying them over would make the consumer see an
  exhausted budget on a fresh message and dead-letter it again immediately.

Run it as `python -m app.dlq_replay --limit 100`; the exit code is `0` when
everything was replayed, `2` when something was left for a human, and `1` when the
broker refused a publish.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, Message
from aio_pika.abc import (
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)

from app.core.config import settings
from app.runtime import configure_logging

logger = logging.getLogger("app.dlq_replay")

#: Bound on one connection attempt; mirrors the consumer so a stalled broker is
#: given up on in the same order of time.
CONNECT_TIMEOUT_SECONDS = 10.0

#: Bound on one replay publish. A publish that silently hangs must fail the run,
#: not hold the operator's terminal forever.
PUBLISH_TIMEOUT_SECONDS = 5.0

#: How long a single `basic.get` may wait for a message before the run treats the
#: queue as drained. Short on purpose: the tool is interactive, and an empty DLQ
#: should answer immediately.
GET_TIMEOUT_SECONDS = 1.0

ConnectFactory = Callable[..., Awaitable[AbstractRobustConnection]]


@dataclass(frozen=True)
class ReplayReport:
    """What one run did — printed on stdout for the operator and the tests."""

    examined: int = 0
    replayed: int = 0
    skipped: int = 0
    failed: int = 0


async def replay_dead_letters(
    *,
    url: str,
    dead_letter_queue: str,
    target_queue: str,
    limit: int,
    connect: ConnectFactory = aio_pika.connect_robust,
    connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
) -> ReplayReport:
    """Move up to `limit` dead letters back into the consumer's own queue."""
    if limit < 1:
        raise ValueError("limit must be positive")

    connection = await connect(url, timeout=connect_timeout)
    try:
        # Confirms make `publish()` return only after `Basic.Ack`; `on_return_raises`
        # turns an unroutable message into an exception instead of a shrug.
        channel = await connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )
        # Passive: the DLQ belongs to the consumer, and this tool must never
        # change its arguments or create it by accident.
        queue = await channel.declare_queue(
            dead_letter_queue,
            durable=True,
            passive=True,
        )
        # The target must already exist (the consumer declares it at start-up).
        # A passive declare here turns "the consumer never ran" into a clear error
        # instead of every publish being returned as unroutable.
        await channel.declare_queue(target_queue, durable=True, passive=True)
        return await _replay(
            exchange=channel.default_exchange,
            queue=queue,
            target_queue=target_queue,
            limit=limit,
        )
    finally:
        await _close(connection)


async def _replay(
    *,
    exchange: AbstractExchange,
    queue: AbstractQueue,
    target_queue: str,
    limit: int,
) -> ReplayReport:
    examined = replayed = skipped = failed = 0

    while examined < limit:
        message = await queue.get(
            no_ack=False,
            fail=False,
            timeout=GET_TIMEOUT_SECONDS,
        )
        if message is None:
            break
        examined += 1

        event_type = _event_type(message)
        if event_type is None:
            skipped += 1
            logger.error(
                "dead letter %s is not a parseable envelope; left in the queue",
                _label(message),
            )
            # Return it and stop: replaying a blob would only bounce it back into
            # the DLQ, and the next message may be just as broken.
            await _settle(message.nack(requeue=True))
            break

        try:
            await exchange.publish(
                _forward(message, event_type),
                routing_key=target_queue,
                mandatory=True,
                timeout=PUBLISH_TIMEOUT_SECONDS,
            )
        except Exception:
            failed += 1
            logger.exception(
                "could not replay dead letter %s; it stays in the queue",
                _label(message),
            )
            await _settle(message.nack(requeue=True))
            break

        # Only now: the broker has the message, so removing it from the DLQ is
        # safe. A crash between the two re-delivers a duplicate, which the inbox
        # absorbs by `event_id`.
        await _settle(message.ack())
        replayed += 1
        logger.info(
            "replayed dead letter %s into %s as %s",
            _label(message),
            target_queue,
            event_type,
        )

    return ReplayReport(
        examined=examined,
        replayed=replayed,
        skipped=skipped,
        failed=failed,
    )


def _event_type(message: AbstractIncomingMessage) -> str | None:
    """The event type from the body, or `None` if the body is not an envelope.

    It is not needed for routing here (the routing key is the queue name, not the
    event type), but a body without one would only be dead-lettered again, so it
    is refused before the round trip.
    """
    try:
        payload = json.loads(message.body)
    except json.JSONDecodeError, UnicodeDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    event_type = payload.get("event_type")
    return event_type if isinstance(event_type, str) and event_type else None


def _forward(
    message: AbstractIncomingMessage,
    event_type: str,
) -> Message:
    """Rebuild the wire message without the broker's own delivery metadata."""
    return Message(
        body=message.body,
        content_type=message.content_type or "application/json",
        message_id=message.message_id,
        type=event_type,
        delivery_mode=DeliveryMode.PERSISTENT,
    )


def _label(message: AbstractIncomingMessage) -> str:
    return message.message_id or "<unknown>"


async def _settle(call: Awaitable[Any]) -> None:
    """Await an ack/nack; never let a verdict failure kill the run."""
    try:
        await call
    except Exception:
        logger.warning("could not settle a dead letter", exc_info=True)


async def _close(connection: AbstractRobustConnection) -> None:
    try:
        await connection.close()
    except Exception:
        logger.warning(
            "closing the RabbitMQ connection failed; the broker reaps it",
            exc_info=True,
        )


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m app.dlq_replay",
        description="Replay dead-lettered order events back into this consumer.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=100,
        help="maximum number of dead letters to replay (default: 100)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="log verbosity (default: INFO)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging(args.log_level)

    if not settings.rabbitmq_url:
        logger.error("RABBITMQ_URL is required by python -m app.dlq_replay")
        return 2

    report = asyncio.run(
        replay_dead_letters(
            url=settings.rabbitmq_url,
            dead_letter_queue=settings.event_consumer_dlq,
            target_queue=settings.event_consumer_queue,
            limit=args.limit,
        )
    )
    # stdout, not the log: this is the machine-readable answer of an operation,
    # and the per-message lines already went to the log handler.
    print(report)
    if report.failed:
        return 1
    return 2 if report.skipped else 0


if __name__ == "__main__":
    sys.exit(main())
