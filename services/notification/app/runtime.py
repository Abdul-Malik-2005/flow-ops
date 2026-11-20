"""Process-level plumbing shared by the long-running entry points.

`app.event_consumer` is one process, but it needs the same three things the Order
service's background processes need: logging configured once, SIGTERM/SIGINT
turned into "finish the current message and exit 0", and a sleep that wakes up
early when that signal arrives.

Nothing here is domain logic: keep it free of settings and of any web framework,
so an entry point can import it without dragging in unrelated state.
"""

from __future__ import annotations

import asyncio
import logging
import signal

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging(level: str) -> None:
    """Configure the root logger for a standalone process.

    `basicConfig` is a no-op when a handler is already installed, so a second
    call inside the same interpreter cannot duplicate log lines — which is what
    makes this safe to call from `main()` of every entry point.
    """
    logging.basicConfig(level=level, format=LOG_FORMAT)


async def sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Sleep, but wake up immediately when a shutdown signal arrives."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


def install_signal_handlers(stop: asyncio.Event, *, logger: logging.Logger) -> None:
    """Turn SIGTERM/SIGINT into "finish the current message and exit 0".

    `signal.signal` instead of `loop.add_signal_handler`: the latter does not
    exist on Windows, and the process must be runnable from a developer machine.
    The handler only flips the event; the loop decides where it is safe to stop,
    so no transaction is ever abandoned mid-flight on purpose.
    """
    loop = asyncio.get_running_loop()

    def _request_stop(signum: int, _frame: object) -> None:
        logger.info("received signal %s: finishing the current message", signum)
        loop.call_soon_threadsafe(stop.set)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _request_stop)
