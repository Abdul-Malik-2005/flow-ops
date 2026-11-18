"""The worker's wiring and its one-line report.

The loop itself is proved against a real database in
`tests/integration/test_reconciler.py`; here only the cheap invariants live: the
worker is configured from settings (never hard-coded), `--once` is opt-in, and
the pass report is logged at a level an operator can filter on.

Importing this module is the smoke test for the entry point: it must not need a
database connection, a JWT key or a running Inventory.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from app.core.config import settings
from app.services.reconciler import ReconcileReport
from app.worker import _parse_args, _sleep_or_stop, build_reconciler, log_report


async def test_build_reconciler_wires_every_setting() -> None:
    async with httpx.AsyncClient() as client:
        reconciler = build_reconciler(client)

    assert reconciler._batch_size == settings.reconcile_batch_size
    assert reconciler._stale_after_seconds == settings.reservation_stale_after_seconds
    assert reconciler._max_attempts == settings.reservation_max_attempts
    assert reconciler._alert_after_seconds == settings.pending_alert_after_seconds


def test_once_flag_is_opt_in() -> None:
    assert _parse_args([]).once is False
    assert _parse_args(["--once"]).once is True


def test_an_unknown_log_level_is_rejected() -> None:
    with pytest.raises(SystemExit):
        _parse_args(["--log-level", "TRACE"])


def test_an_idle_pass_logs_at_debug(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("DEBUG", logger="app.worker"):
        log_report(ReconcileReport())

    assert "nothing to do" in caplog.text


def test_a_quiet_pass_logs_at_info(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("INFO", logger="app.worker"):
        log_report(ReconcileReport(claimed=1, reserved=1))

    assert caplog.records[-1].levelname == "INFO"
    assert "reserved=1" in caplog.text


def test_a_pass_with_failures_logs_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("INFO", logger="app.worker"):
        log_report(ReconcileReport(claimed=2, reserved=1, failed=1))

    assert caplog.records[-1].levelname == "WARNING"
    assert "failed=1" in caplog.text


async def test_a_pending_shutdown_interrupts_the_sleep() -> None:
    """SIGTERM must not wait for a 30-second interval to finish."""
    stop = asyncio.Event()
    stop.set()

    await _sleep_or_stop(stop, seconds=30.0)
