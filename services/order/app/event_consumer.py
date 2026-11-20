"""Entry point of the event consumer: `python -m app.event_consumer`.

A fourth process from the same image (after the API, the reconciler and the
relay), and for the same reason: consuming must not compete with request
handling, and it must survive an API restart.

What this process guarantees, in order of importance:

* **Commit before ack.** A message is acknowledged only after the journal row is
  committed. The reverse order would lose an event for good if the insert rolled
  back; a crash between the two redelivers it instead, and the journal's primary
  key absorbs the duplicate.
* **A duplicate is a success.** `record()` returns `False` when `event_id` is
  already there, and the message is acknowledged all the same: at-least-once
  delivery makes this a normal outcome, not a reason to retry.
* **Deterministic refusals go to the DLQ, transient failures are requeued.** Bad
  JSON and an unknown event type cannot be fixed by a retry, so they are nacked
  without requeue and the queue's dead-letter policy takes over. A database or
  transport failure is retried — after an interruptible pause, so an outage does
  not become a hot loop that burns the delivery budget in milliseconds.
* **This process never publishes.** It writes one journal table and nothing else;
  an event produced here would loop back through the broker forever.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumers.rabbitmq import ConnectFactory, RabbitMQEventConsumer
from app.core.config import Settings, settings
from app.db.session import SessionLocal, engine
from app.domain.events import OrderEventType
from app.repositories.projection import SqlProjectionRepository
from app.runtime import configure_logging, install_signal_handlers, sleep_or_stop
from app.services.event_projection import (
    EventHandlingError,
    MalformedEventError,
    OrderEventProjector,
)

logger = logging.getLogger("app.event_consumer")

#: The allow-list, derived from the domain enum: subscribing to `order.#` would
#: make every future event type an input of a consumer that cannot understand it,
#: and a literal list would drift from `OrderEventProjector` after the first
#: addition.
ROUTING_KEYS: tuple[str, ...] = tuple(event_type.value for event_type in OrderEventType)

#: RabbitMQ 4 quorum queues report how many times a message has already been
#: acquired. It is the broker's own count, so it survives a consumer restart and
#: cannot be reset from Python — unlike a counter kept in memory. The queue also
#: carries `x-delivery-limit`, but this broker does **not** apply it to an
#: explicit `nack(requeue=True)`: a message can be requeued forever while the
#: limit stays decorative (verified against `rabbitmq:4-management-alpine`).
ACQUIRED_COUNT_HEADER = "x-acquired-count"

#: One journal write inside the caller's session. Injected so the callback's
#: decision table can be tested without a database.
RecordProjection = Callable[[AsyncSession, Mapping[str, Any]], Awaitable[bool]]


@dataclass
class ProjectionReport:
    """What the consumer did since the previous report.

    The relay has `RelayReport`; this is its inbound counterpart. Categories are
    kept apart because they mean different things to an operator: a duplicate is
    normal under at-least-once delivery, a retry is a transient dependency
    problem, and `refused`/`dead_lettered` are messages that will never be
    processed without a human.
    """

    processed: int = 0
    duplicates: int = 0
    retried: int = 0
    refused: int = 0
    dead_lettered: int = 0

    def is_empty(self) -> bool:
        return not (
            self.processed
            or self.duplicates
            or self.retried
            or self.refused
            or self.dead_lettered
        )


async def _record_projection(
    session: AsyncSession,
    payload: Mapping[str, Any],
) -> bool:
    return await OrderEventProjector(SqlProjectionRepository(session)).handle(payload)


class EventJournalConsumer:
    """The `on_message` callback: one delivered message, one journal row.

    The runtime owns the transaction and the verdict; the projector owns parsing
    and writing. Keeping them apart is what makes "commit before ack" a property
    of this class rather than a comment inside a handler.
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        stop: asyncio.Event,
        requeue_delay_seconds: float,
        max_delivery_count: int,
        report_interval_seconds: float,
        record: RecordProjection = _record_projection,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._session_factory = session_factory
        self._stop = stop
        self._requeue_delay_seconds = requeue_delay_seconds
        self._max_delivery_count = max_delivery_count
        self._report_interval_seconds = report_interval_seconds
        self._clock = clock
        self._report = ProjectionReport()
        self._last_report_at = clock()
        self._record = record

    async def __call__(self, message: AbstractIncomingMessage) -> None:
        try:
            payload = _decode_message(message)
            async with self._session_factory() as session:
                recorded = await self._record(session, payload)
                # The ack below happens only if this commit returned: a rollback
                # must leave the message unacknowledged so the broker redelivers
                # it. That ordering is the whole point of this method.
                await session.commit()
        except EventHandlingError as error:
            # Retrying cannot change the message, so it goes straight to the
            # queue's dead-letter policy instead of burning delivery attempts.
            self._report.refused += 1
            logger.error("refusing event %s: %s", _event_label(message), error)
            await self._nack(message, requeue=False)
        except Exception:
            logger.exception(
                "transient failure while journaling event %s",
                _event_label(message),
            )
            if self._delivery_limit_reached(message):
                self._report.dead_lettered += 1
                # The broker's own counter says this is the last allowed
                # delivery. Requeueing again would be an endless loop, so the
                # message is refused and the DLX takes over — the same outcome
                # `x-delivery-limit` promises but does not deliver here.
                logger.error(
                    "dead-lettering event %s after %s deliveries",
                    _event_label(message),
                    self._max_delivery_count,
                )
                await self._nack(message, requeue=False)
                return
            self._report.retried += 1
            await self._nack(message, requeue=True)
            # A database outage would otherwise re-deliver this message in a
            # tight loop and exhaust the delivery budget before an operator can
            # react. `sleep_or_stop` keeps shutdown responsive during the pause.
            await sleep_or_stop(self._stop, self._requeue_delay_seconds)
        else:
            if recorded:
                self._report.processed += 1
                # DEBUG, not INFO: the periodic report is what an operator reads;
                # a line per event makes a busy queue's steady state a log flood.
                logger.debug("journaled event %s", _event_label(message))
            else:
                self._report.duplicates += 1
                logger.debug("event %s was already journaled", _event_label(message))
            await self._ack(message)
        finally:
            self.report()

    def report(self, *, force: bool = False) -> None:
        """Log one line if the interval elapsed (or `force`), then reset.

        Counters are per interval, like `RelayReport` is per pass: a cumulative
        number makes an operator subtract two lines to see the rate, and a quiet
        consumer must not keep repeating its total. `force` is for shutdown, so
        the last interval is not lost.
        """
        now = self._clock()
        if not force and now - self._last_report_at < self._report_interval_seconds:
            return

        report, self._report = self._report, ProjectionReport()
        self._last_report_at = now
        if report.is_empty():
            return

        logger.info(
            "consumer report: processed=%s duplicates=%s retried=%s "
            "refused=%s dead_lettered=%s",
            report.processed,
            report.duplicates,
            report.retried,
            report.refused,
            report.dead_lettered,
        )

    def _delivery_limit_reached(self, message: AbstractIncomingMessage) -> bool:
        """Whether this delivery is the last one this consumer may accept.

        `x-acquired-count` is absent on the first delivery and equals the number
        of previous acquisitions afterwards, so adding one makes it the ordinal
        of the current delivery. A malformed header is treated as zero: a
        missing counter must not turn into an immediate dead letter.
        """
        headers = message.headers or {}
        previous = headers.get(ACQUIRED_COUNT_HEADER, 0)
        if not isinstance(previous, int):
            previous = 0
        return previous + 1 >= self._max_delivery_count

    async def _ack(self, message: AbstractIncomingMessage) -> None:
        try:
            await message.ack()
        except Exception:
            # The row is committed; a lost ack only means redelivery, which the
            # journal deduplicates. Raising here would kill the consumer loop
            # over a recoverable transport failure.
            logger.warning(
                "could not acknowledge event %s",
                _event_label(message),
                exc_info=True,
            )

    async def _nack(
        self,
        message: AbstractIncomingMessage,
        *,
        requeue: bool,
    ) -> None:
        try:
            await message.nack(requeue=requeue)
        except Exception:
            # Same reasoning as in `_ack`: an unsettled message returns to the
            # queue when the channel closes, and the journal tolerates a replay.
            logger.warning(
                "could not reject event %s (requeue=%s)",
                _event_label(message),
                requeue,
                exc_info=True,
            )


def _decode_message(message: AbstractIncomingMessage) -> Mapping[str, Any]:
    """Turn the AMQP body into the JSON object the projector expects.

    Broken JSON is a deterministic refusal, not a crash: the callback must end
    with a verdict, otherwise the message stays unacknowledged until the channel
    closes.
    """
    try:
        payload = json.loads(message.body)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise MalformedEventError(f"the message body is not JSON: {error}") from error

    if not isinstance(payload, Mapping):
        raise MalformedEventError(
            f"the message must be a JSON object, got {type(payload).__name__}"
        )
    return payload


def _event_label(message: AbstractIncomingMessage) -> str:
    """`message_id` is the `event_id`; an AMQP producer may omit it."""
    return message.message_id or "<unknown>"


def build_consumer(
    config: Settings,
    *,
    connect: ConnectFactory = aio_pika.connect_robust,
) -> RabbitMQEventConsumer:
    """Wire the inbound adapter from settings; no I/O happens until `start`.

    Every name comes from configuration, and the routing keys come from the
    domain enum: a hard-coded list here would silently stop subscribing to a new
    event type while the projector would happily accept it.
    """
    return RabbitMQEventConsumer(
        url=config.rabbitmq_url,
        exchange_name=config.outbox_exchange,
        queue_name=config.event_consumer_queue,
        dead_letter_exchange=config.event_consumer_dlx_exchange,
        dead_letter_queue=config.event_consumer_dlq,
        routing_keys=ROUTING_KEYS,
        prefetch_count=config.event_consumer_prefetch_count,
        max_delivery_count=config.event_consumer_max_delivery_count,
        connect=connect,
    )


async def run_forever(stop: asyncio.Event | None = None) -> None:
    """Subscribe, then wait for shutdown; a failed first attempt is retried."""
    stop = stop if stop is not None else asyncio.Event()
    install_signal_handlers(stop, logger=logger)

    consumer = build_consumer(settings)
    handler = EventJournalConsumer(
        session_factory=SessionLocal,
        stop=stop,
        requeue_delay_seconds=settings.event_consumer_requeue_delay_seconds,
        max_delivery_count=settings.event_consumer_max_delivery_count,
        report_interval_seconds=settings.event_consumer_report_interval_seconds,
    )

    try:
        while not stop.is_set():
            try:
                await consumer.start(handler)
            except asyncio.CancelledError:
                raise
            except Exception:
                # A broker that is not up yet (or a queue declared with different
                # arguments) must not turn into a crash loop without a pause.
                logger.exception(
                    "could not subscribe to RabbitMQ; retrying in %.1f s",
                    settings.event_consumer_reconnect_delay_seconds,
                )
                await sleep_or_stop(
                    stop,
                    settings.event_consumer_reconnect_delay_seconds,
                )
                continue

            # `connect_robust` restores a dropped connection and its
            # declarations on its own; the loop only has to survive a *failed
            # first attempt* and to stop on a signal.
            await stop.wait()
    finally:
        # Flush the last interval: a stop must not hide what the process did.
        handler.report(force=True)
        # Same order as the relay: release the transport, then the pool. A
        # failing close must not skip `engine.dispose()`.
        try:
            await consumer.aclose()
        finally:
            await engine.dispose()

    logger.info("event consumer stopped")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m app.event_consumer",
        description="Journal the order events delivered by the broker.",
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

    # The API may run without a broker (the shared `Settings` deliberately has
    # no validator for this); a consumer cannot, and must say so before it tries
    # to connect in a loop.
    if not settings.rabbitmq_url:
        logger.error("RABBITMQ_URL is required by python -m app.event_consumer")
        return 2

    asyncio.run(run_forever())
    return 0


if __name__ == "__main__":
    sys.exit(main())
