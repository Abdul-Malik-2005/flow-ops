"""The shared process plumbing: one sleep primitive and one signal hook.

Both are duplicated only logically — this module is the single copy the worker
and the relay use — so the tests pin the two properties the loops rely on: the
sleep returns *early* on a pending stop and still returns after the timeout, and
a signal only flips the event instead of raising out of the handler.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time

import pytest

from app.runtime import install_signal_handlers, sleep_or_stop


async def test_a_set_event_interrupts_the_sleep() -> None:
    """SIGTERM must not wait for a 30-second interval to finish."""
    stop = asyncio.Event()
    stop.set()

    started = time.monotonic()
    await sleep_or_stop(stop, seconds=30.0)

    assert time.monotonic() - started < 1.0


async def test_an_unset_event_sleeps_for_the_whole_interval() -> None:
    """The timeout branch: a normal idle pass must not spin."""
    stop = asyncio.Event()

    started = time.monotonic()
    await sleep_or_stop(stop, seconds=0.05)

    assert time.monotonic() - started >= 0.04


async def test_a_signal_requests_a_stop_and_only_that(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The handler flips the event: the loop, not the handler, decides when."""
    stop = asyncio.Event()
    log = logging.getLogger("app.runtime.test")
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}

    install_signal_handlers(stop, logger=log)
    try:
        with caplog.at_level(logging.INFO, logger=log.name):
            # Call the installed handler directly: the point is its effect, not
            # the kernel's ability to deliver SIGTERM on CI.
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            await asyncio.sleep(0)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)

    assert stop.is_set()
    assert "received signal" in caplog.text
