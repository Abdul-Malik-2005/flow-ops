"""The sender entry point: the backoff policy and the wiring.

The loop itself needs a database, so it is covered by
`tests/integration/test_sender.py`; here are the pieces that are pure or need no
I/O: the backoff function, the adapter choice and the argument parser.
"""

from __future__ import annotations

import pytest

from app import sender as entrypoint
from app.senders.log import LogNotificationSender
from app.senders.smtp import SmtpNotificationSender


def test_the_first_failure_waits_one_interval() -> None:
    # A single blip is normal: no reason to slow down yet.
    assert entrypoint.pass_backoff(5.0, 0) == 5.0
    assert entrypoint.pass_backoff(5.0, 1) == 5.0


def test_the_delay_doubles_per_consecutive_failure() -> None:
    assert entrypoint.pass_backoff(5.0, 2) == 10.0
    assert entrypoint.pass_backoff(5.0, 3) == 20.0


def test_the_backoff_is_capped() -> None:
    # A long outage must not turn one problem into a log flood.
    assert entrypoint.pass_backoff(5.0, 100) == entrypoint.MAX_PASS_BACKOFF_SECONDS


def test_the_log_transport_is_the_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(entrypoint.settings, "sender_transport", "log")

    assert isinstance(entrypoint.build_notification_sender(), LogNotificationSender)


def test_the_smtp_transport_builds_the_smtp_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(entrypoint.settings, "sender_transport", "smtp")
    monkeypatch.setattr(entrypoint.settings, "smtp_host", "mailpit")
    monkeypatch.setattr(entrypoint.settings, "smtp_from", "orders@flowops.local")

    # Building the adapter opens no connection: the first `send` does.
    assert isinstance(entrypoint.build_notification_sender(), SmtpNotificationSender)


def test_the_parser_reads_the_once_flag() -> None:
    args = entrypoint._parse_args(["--once"])

    assert args.once is True
    assert args.log_level == "INFO"
