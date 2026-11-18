"""Entry point of the reconciler worker: `python -m app.worker [--once]`.

The worker is a *second process* built from the same image as the API. It
deliberately does not import `app.main`: the API's lifespan eagerly loads the
Identity public key, which this process never uses, so importing it would make
the worker refuse to start for a reason that has nothing to do with its job.

What the worker owns:

* one `httpx.AsyncClient` for the whole process (same reason as the API: a
  per-call client would pay a TCP handshake on every retry);
* the loop: process a batch, sleep for the configured interval, stop on
  SIGTERM/SIGINT *after* the current batch so no claimed order is abandoned
  half-way (a crash is safe — the lease expires — but a clean stop is cheaper);
* its own exit code: `--once` is for smoke tests and cron, `0` on success.

Run it with `python -m app.worker --once` to drain one batch and exit.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Sequence

import httpx

from app.clients.inventory import InventoryClient
from app.core.config import settings
from app.db.session import SessionLocal, engine
from app.services.reconciler import Reconciler, ReconcileReport

logger = logging.getLogger("app.worker")


def build_reconciler(http: httpx.AsyncClient) -> Reconciler:
    """Wire the worker from settings.

    Every knob comes from configuration, including the lease window: the API and
    the worker must agree on it, otherwise one of them acts on a row while the
    other still believes a reserve call is in flight.
    """
    return Reconciler(
        session_factory=SessionLocal,
        inventory=InventoryClient(
            http,
            base_url=settings.inventory_base_url,
            service_token=settings.service_token,
        ),
        batch_size=settings.reconcile_batch_size,
        stale_after_seconds=settings.reservation_stale_after_seconds,
        max_attempts=settings.reservation_max_attempts,
        alert_after_seconds=settings.pending_alert_after_seconds,
    )


async def run_once() -> ReconcileReport:
    """One pass: useful as a smoke test and as a cron job for small installs."""
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(settings.inventory_timeout_seconds),
    ) as http:
        try:
            return await build_reconciler(http).process_batch()
        finally:
            # The engine is module-level (shared with the API in a single-process
            # deployment), so the process that owns it must close it on the way
            # out, otherwise the pool's connections are only reaped by the server.
            await engine.dispose()


async def run_forever(*, stop: asyncio.Event | None = None) -> None:
    """Reconcile until a shutdown signal arrives."""
    stop = stop or asyncio.Event()
    _install_signal_handlers(stop)
    interval = settings.reconcile_interval_seconds

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(settings.inventory_timeout_seconds),
    ) as http:
        reconciler = build_reconciler(http)
        try:
            while not stop.is_set():
                try:
                    report = await reconciler.process_batch()
                except Exception:
                    # PostgreSQL away, migrations not applied, a bug: a worker
                    # that dies on the first bad pass is useless. The next pass
                    # is independent, because every order is settled in its own
                    # transaction and every lease expires on its own.
                    logger.exception(
                        "reconcile pass failed; retrying in %.0f s",
                        interval,
                    )
                else:
                    log_report(report)
                    if report.claimed:
                        # A backlog: keep draining instead of sleeping.
                        continue
                await _sleep_or_stop(stop, interval)
        finally:
            await engine.dispose()

    logger.info("reconciler stopped")


def log_report(report: ReconcileReport) -> None:
    """One line per pass, at a level that matches what happened."""
    if report.claimed == 0 and report.stale_backlog == 0:
        logger.debug("reconcile pass: nothing to do")
        return

    message = (
        "reconcile pass: claimed=%s reserved=%s failed=%s retried=%s "
        "superseded=%s stale_backlog=%s"
    )
    arguments = (
        report.claimed,
        report.reserved,
        report.failed,
        report.retried,
        report.superseded,
        report.stale_backlog,
    )
    if report.failed or report.superseded or report.stale_backlog:
        logger.warning(message, *arguments)
    else:
        logger.info(message, *arguments)


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Sleep, but wake up immediately when a shutdown signal arrives."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """Turn SIGTERM/SIGINT into "finish the batch and exit 0".

    `signal.signal` instead of `loop.add_signal_handler`: the latter does not
    exist on Windows, and the worker must be runnable from a developer machine.
    """
    loop = asyncio.get_running_loop()

    def _request_stop(signum: int, _frame: object) -> None:
        logger.info("received signal %s: finishing the current batch", signum)
        loop.call_soon_threadsafe(stop.set)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _request_stop)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m app.worker",
        description="Retry stock reservations for orders stuck in `pending`.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="process a single batch, print a report and exit",
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
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.once:
        report = asyncio.run(run_once())
        # stdout, not the log: this is the machine-readable answer of a smoke
        # test or a cron invocation.
        print(report)
        return 0

    asyncio.run(run_forever())
    return 0


if __name__ == "__main__":
    sys.exit(main())
