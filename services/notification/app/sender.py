"""Entry point of the notification sender: `python -m app.sender [--once]`.

The second process of this service (after the consumer), and for the same reason
the consumer is separate: delivering notifications must not compete with
recording events, and it must survive a consumer restart.

What it owns:

* the transport — `LogNotificationSender` today, an email/push adapter later,
  behind the same `NotificationSenderPort`;
* the loop: send a batch, sleep, stop on SIGTERM/SIGINT *after* the current batch,
  so no claimed row is abandoned in flight (a crash is also safe — the lease
  expires — a clean stop is just cheaper);
* its own exit code: `--once` is for smoke tests and cron, `0` on success.

It needs only `DATABASE_URL`: unlike the consumer, it never speaks to the broker.
Run it with `python -m app.sender --once` to deliver one batch and exit.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Sequence

from app.core.config import settings
from app.db.session import SessionLocal, engine
from app.domain.notifications import NotificationComposer
from app.runtime import configure_logging, install_signal_handlers, sleep_or_stop
from app.senders.log import LogNotificationSender
from app.senders.smtp import SmtpNotificationSender
from app.services.notification_sender import (
    NotificationSender,
    NotificationSenderPort,
    SendReport,
)
from app.services.recipient_resolver import LocalRecipientResolver

logger = logging.getLogger("app.sender")

#: A pass is drained without sleeping only while at least this share of its rows
#: was sent. A fully dead provider is caught by the same condition with `sent == 0`,
#: and a *partially* degraded one is caught too: draining it would spend the
#: attempt budget of the whole backlog in seconds, which is exactly the failure the
#: backoff exists to prevent.
DRAIN_MIN_SENT_SHARE = 0.5

#: Ceiling for the backoff after consecutive failing passes. A database restart or
#: an unapplied migration is a minutes-long condition, and retrying it every
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


def build_sender() -> NotificationSender:
    """Wire the sender from settings.

    Every knob comes from configuration: the batch size, the lease and the attempt
    budget must match the defaults the adapter is tested against, and a hard-coded
    value here would silently disagree with them.
    """
    return NotificationSender(
        session_factory=SessionLocal,
        sender=build_notification_sender(),
        composer=NotificationComposer(),
        resolver=LocalRecipientResolver(domain=settings.recipient_domain),
        batch_size=settings.sender_batch_size,
        lease_seconds=settings.sender_lease_seconds,
        max_attempts=settings.sender_max_attempts,
    )


def build_notification_sender() -> NotificationSenderPort:
    """Pick the adapter for the configured transport.

    Public so the choice itself is testable without a sender. `log` is the code
    default (no infrastructure); `smtp` delivers real email. The settings validator
    has already refused `smtp` without a host and a sender, so this function never
    has to fall back.
    """
    if settings.sender_transport == "smtp":
        return SmtpNotificationSender(
            host=settings.smtp_host,
            port=settings.smtp_port,
            username=settings.smtp_username,
            password=settings.smtp_password,
            from_addr=settings.smtp_from,
            timeout=settings.smtp_timeout_seconds,
            use_tls=settings.smtp_use_tls,
        )
    return LogNotificationSender()


async def run_once() -> SendReport:
    """One pass: useful as a smoke test and as a cron job for small installs."""
    sender = build_sender()
    try:
        return await sender.process_batch()
    finally:
        # Transport first, then the engine: stop talking, then stop reading. The
        # nested `try` keeps the engine disposal unconditional — a failed close of
        # a dead transport must not leak the pool.
        try:
            await sender.aclose()
        finally:
            await engine.dispose()


async def run_forever(*, stop: asyncio.Event | None = None) -> None:
    """Deliver until a shutdown signal arrives."""
    stop = stop or asyncio.Event()
    install_signal_handlers(stop, logger=logger)
    interval = settings.sender_interval_seconds
    sender = build_sender()
    failures = 0

    try:
        while not stop.is_set():
            delay = interval
            try:
                report = await sender.process_batch()
            except Exception:
                # PostgreSQL away, migrations not applied, a bug: a sender that
                # dies on the first bad pass is useless. The next pass is safe,
                # because sending twice is allowed (idempotency key) and a failed
                # send leaves the row unsent. The delay grows so that a condition
                # measured in minutes is retried in minutes.
                failures += 1
                delay = pass_backoff(interval, failures)
                logger.exception(
                    "sender pass failed (%s in a row); retrying in %.1f s",
                    failures,
                    delay,
                )
            else:
                failures = 0
                log_report(report)
                if (
                    report.claimed
                    and report.sent >= report.claimed * DRAIN_MIN_SENT_SHARE
                ):
                    # A backlog and a healthy provider: keep draining.
                    continue
                if report.claimed:
                    # The provider is down, or degraded enough that draining
                    # would spend the whole attempt budget in seconds and turn
                    # every row into a dead letter. Back off instead of looping.
                    logger.warning(
                        "only %s of %s claimed notifications were sent; backing off "
                        "%.1f s instead of draining",
                        report.sent,
                        report.claimed,
                        interval,
                    )
            await sleep_or_stop(stop, delay)
    finally:
        # Same order as in `run_once`: the transport outlives the loop, the pool
        # outlives both. A failing close must not skip `engine.dispose()`.
        try:
            await sender.aclose()
        finally:
            await engine.dispose()

    logger.info("notification sender stopped")


def log_report(report: SendReport) -> None:
    """One line per pass, at a level that matches what happened."""
    if report.claimed == 0 and report.exhausted == 0:
        logger.debug("sender pass: nothing to do")
        return

    message = "sender pass: claimed=%s sent=%s failed=%s duplicates=%s exhausted=%s"
    arguments = (
        report.claimed,
        report.sent,
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
        prog="python -m app.sender",
        description="Deliver recorded notifications to the notification transport.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="deliver a single batch, print a report and exit",
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
        # test or a cron invocation, and `app.notifications` lines already went
        # to the log handler with the payload a provider will read.
        print(report)
        return 0

    asyncio.run(run_forever())
    return 0


if __name__ == "__main__":
    sys.exit(main())
