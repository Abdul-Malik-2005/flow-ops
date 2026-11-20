"""Entry point of the outbox relay: `python -m app.outbox_relay [--once]`.

A third process built from the same image, for the same reason the reconciler is
one: publishing must not compete with the request handling of the API, and it
must survive an API restart. It deliberately does not import `app.main` (the
API's lifespan loads the Identity public key this process never uses).

Why a relay at all: the saga writes an event row *inside* the business
transaction, so the event exists if and only if the state change committed. This
process is the only place where an event leaves the database, and it is therefore
the only place where a publish can happen outside a transaction.

What it owns:

* the transport — `LogEventPublisher` or `RabbitMQEventPublisher`, chosen by
  `OUTBOX_PUBLISHER` behind the same `EventPublisherPort`;
* the loop: publish a batch, sleep, stop on SIGTERM/SIGINT *after* the current
  batch, so no claimed row is abandoned in flight (a crash is also safe — the
  lease expires — a clean stop is just cheaper);
* its own exit code: `--once` is for smoke tests and cron, `0` on success.

Run it with `python -m app.outbox_relay --once` to publish one batch and exit.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Sequence

from app.core.config import settings
from app.db.session import SessionLocal, engine
from app.publishers.log import LogEventPublisher
from app.publishers.rabbitmq import RabbitMQEventPublisher
from app.runtime import configure_logging, install_signal_handlers, sleep_or_stop
from app.services.outbox_relay import EventPublisherPort, OutboxRelay, RelayReport

logger = logging.getLogger("app.outbox_relay")

#: A pass is drained without sleeping only while at least this share of its
#: events was published. A fully dead transport is caught by the same condition
#: with `published == 0`, and a *partially* degraded one (a throttling broker, a
#: poison event in every batch) is caught too: draining it would spend the
#: attempt budget of the whole backlog in seconds, which is exactly the failure
#: the backoff exists to prevent.
DRAIN_MIN_PUBLISHED_SHARE = 0.5

#: Ceiling for the backoff after consecutive failing passes. A database restart
#: or an unapplied migration is a minutes-long condition, and retrying it every
#: `interval` only turns one problem into a log flood.
MAX_PASS_BACKOFF_SECONDS = 300.0
#: How far the exponential may grow before the ceiling takes over; keeps the
#: exponent from overflowing on a long outage.
_MAX_BACKOFF_DOUBLINGS = 16


def pass_backoff(interval: float, consecutive_failures: int) -> float:
    """Delay before the next pass after that many failed ones in a row.

    The first failure already waits one interval (a single blip is normal); from
    there the delay doubles until the ceiling. Pure and public for the tests: the
    loop's sleep is monkeypatched, so this is where the policy is pinned down.
    """
    doublings = min(max(consecutive_failures - 1, 0), _MAX_BACKOFF_DOUBLINGS)
    return min(interval * 2**doublings, MAX_PASS_BACKOFF_SECONDS)


def build_relay() -> OutboxRelay:
    """Wire the relay from settings.

    Every knob comes from configuration: the batch size, the lease and the
    attempt budget must match the defaults the broker adapter is tested against,
    and a hard-coded value here would silently disagree with them.

    The transport is chosen by `OUTBOX_PUBLISHER` and nothing else: claiming,
    marking and the report are identical whichever adapter is in place, which is
    the point of the port.
    """
    return OutboxRelay(
        session_factory=SessionLocal,
        publisher=build_publisher(),
        batch_size=settings.outbox_batch_size,
        lease_seconds=settings.outbox_lease_seconds,
        max_attempts=settings.outbox_max_attempts,
    )


def build_publisher() -> EventPublisherPort:
    """Pick the adapter for the configured transport.

    Public so the choice itself is testable without a relay, and so a future
    consumer-side tool can reuse it. The settings validator has already refused
    `rabbitmq` without an address and an exchange, so this function never has to
    fall back: a missing value cannot reach it.
    """
    if settings.outbox_publisher == "rabbitmq":
        return RabbitMQEventPublisher(
            url=settings.rabbitmq_url,
            exchange=settings.outbox_exchange,
        )
    return LogEventPublisher()


async def run_once() -> RelayReport:
    """One pass: useful as a smoke test and as a cron job for small installs."""
    relay = build_relay()
    try:
        return await relay.process_batch()
    finally:
        # Transport first, then the engine: stop talking, then stop reading. The
        # nested `try` keeps the engine disposal unconditional — a failed close of
        # a dead broker session must not leak the pool.
        try:
            await relay.aclose()
        finally:
            # The engine is module-level, so the process that owns it closes it on
            # the way out; otherwise the pool's connections are reaped by the server.
            await engine.dispose()


async def run_forever(*, stop: asyncio.Event | None = None) -> None:
    """Publish until a shutdown signal arrives."""
    stop = stop or asyncio.Event()
    install_signal_handlers(stop, logger=logger)
    interval = settings.outbox_relay_interval_seconds
    relay = build_relay()
    failures = 0

    try:
        while not stop.is_set():
            delay = interval
            try:
                report = await relay.process_batch()
            except Exception:
                # PostgreSQL away, migrations not applied, a bug: a relay that
                # dies on the first bad pass is useless. The next pass is safe,
                # because publishing an event twice is allowed and a failed
                # publish leaves the row unpublished. The delay grows so that a
                # condition measured in minutes is retried in minutes.
                failures += 1
                delay = pass_backoff(interval, failures)
                logger.exception(
                    "relay pass failed (%s in a row); retrying in %.1f s",
                    failures,
                    delay,
                )
            else:
                failures = 0
                log_report(report)
                if (
                    report.claimed
                    and report.published >= report.claimed * DRAIN_MIN_PUBLISHED_SHARE
                ):
                    # A backlog and a healthy transport: keep draining.
                    continue
                if report.claimed:
                    # The transport is down, or degraded enough that draining
                    # would spend the whole attempt budget in seconds and turn
                    # every row into a dead letter. Back off instead of looping.
                    logger.warning(
                        "only %s of %s claimed events were published; backing off "
                        "%.1f s instead of draining",
                        report.published,
                        report.claimed,
                        interval,
                    )
            await sleep_or_stop(stop, delay)
    finally:
        # Same order as in `run_once`, and it matters more here: this is the path
        # a container takes on SIGTERM, so it is the one where a broker session
        # left open turns a graceful stop into a SIGKILL.
        try:
            await relay.aclose()
        finally:
            await engine.dispose()

    logger.info("outbox relay stopped")


def log_report(report: RelayReport) -> None:
    """One line per pass, at a level that matches what happened."""
    if report.claimed == 0 and report.exhausted == 0:
        logger.debug("relay pass: nothing to do")
        return

    message = "relay pass: claimed=%s published=%s failed=%s duplicates=%s exhausted=%s"
    arguments = (
        report.claimed,
        report.published,
        report.failed,
        report.duplicates,
        report.exhausted,
    )
    if report.failed or report.exhausted:
        logger.warning(message, *arguments)
    else:
        # Duplicates stay at INFO: they are expected under at-least-once and are
        # visible in the line, but they are not a fault an operator must chase.
        logger.info(message, *arguments)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m app.outbox_relay",
        description="Publish committed outbox events to the event transport.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="publish a single batch, print a report and exit",
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

    if args.once:
        report = asyncio.run(run_once())
        # stdout, not the log: this is the machine-readable answer of a smoke
        # test or a cron invocation, and `app.events` lines already went to the
        # log handler with the payload consumers will read.
        print(report)
        return 0

    asyncio.run(run_forever())
    return 0


if __name__ == "__main__":
    sys.exit(main())
