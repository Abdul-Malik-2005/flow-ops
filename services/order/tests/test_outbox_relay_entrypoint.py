"""The relay's wiring and its loop policy.

The relay itself is covered in `tests/test_outbox_relay.py`, and the database side
in `tests/integration/test_outbox_relay.py`; here only the entry point lives: is
every knob taken from settings, does `--once` stay opt-in, and does the loop back
off when the transport is down *or degraded* instead of burning the attempt
budget.

Importing this module is the smoke test for the entry point: it must not need a
database connection or a JWT key.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app import outbox_relay as entrypoint
from app.core.config import settings
from app.publishers.log import LogEventPublisher
from app.publishers.rabbitmq import RabbitMQEventPublisher
from app.services.outbox_relay import RelayReport

INTERVAL = settings.outbox_relay_interval_seconds


def test_build_relay_wires_every_setting() -> None:
    relay = entrypoint.build_relay()

    assert relay._batch_size == settings.outbox_batch_size
    assert relay._lease_seconds == settings.outbox_lease_seconds
    assert relay._max_attempts == settings.outbox_max_attempts


def test_the_log_transport_is_the_shipped_default() -> None:
    """Until a queue is bound, the broker would reject every event as unroutable."""
    assert isinstance(entrypoint.build_publisher(), LogEventPublisher)


def test_the_broker_transport_takes_its_address_and_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(entrypoint.settings, "outbox_publisher", "rabbitmq")
    monkeypatch.setattr(
        entrypoint.settings, "rabbitmq_url", "amqp://flowops:secret@broker:5672/"
    )
    monkeypatch.setattr(entrypoint.settings, "outbox_exchange", "order.events")

    publisher = entrypoint.build_publisher()

    assert isinstance(publisher, RabbitMQEventPublisher)
    assert publisher._url == "amqp://flowops:secret@broker:5672/"
    assert publisher._exchange_name == "order.events"
    # Wiring must stay offline: an unreachable broker is a failed *pass*, not a
    # failed start-up, and `--once` on an empty outbox does no I/O at all.
    assert publisher._connection is None


def test_the_relay_uses_the_chosen_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(entrypoint.settings, "outbox_publisher", "rabbitmq")
    monkeypatch.setattr(
        entrypoint.settings, "rabbitmq_url", "amqp://flowops:secret@broker:5672/"
    )

    assert isinstance(entrypoint.build_relay()._publisher, RabbitMQEventPublisher)


def test_once_flag_is_opt_in() -> None:
    assert entrypoint._parse_args([]).once is False
    assert entrypoint._parse_args(["--once"]).once is True


def test_an_unknown_log_level_is_rejected() -> None:
    with pytest.raises(SystemExit):
        entrypoint._parse_args(["--log-level", "TRACE"])


def test_an_idle_pass_logs_at_debug(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG, logger="app.outbox_relay"):
        entrypoint.log_report(RelayReport())

    assert "nothing to do" in caplog.text


def test_a_clean_pass_logs_at_info(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="app.outbox_relay"):
        entrypoint.log_report(RelayReport(claimed=2, published=2))

    assert caplog.records[-1].levelname == "INFO"
    assert "published=2" in caplog.text


def test_a_dead_letter_raises_the_level_to_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO, logger="app.outbox_relay"):
        entrypoint.log_report(RelayReport(claimed=1, published=1, exhausted=1))

    assert caplog.records[-1].levelname == "WARNING"
    assert "exhausted=1" in caplog.text


# ---------------------------------------------------------------------------
# The loop policy
# ---------------------------------------------------------------------------


class FakeRelay:
    def __init__(self, *reports: RelayReport | Exception) -> None:
        self.queue = list(reports)
        self.calls = 0
        self.closed = False

    async def process_batch(self) -> RelayReport:
        self.calls += 1
        result = self.queue.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def aclose(self) -> None:
        self.closed = True


async def test_a_backlog_keeps_draining_without_sleeping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    sleeps: list[float] = []
    relay = FakeRelay(
        RelayReport(claimed=2, published=2),
        RelayReport(claimed=1, published=1),
        RelayReport(),
    )

    async def fake_sleep(_stop: asyncio.Event, seconds: float) -> None:
        sleeps.append(seconds)
        stop.set()

    monkeypatch.setattr(entrypoint, "build_relay", lambda: relay)
    monkeypatch.setattr(entrypoint, "sleep_or_stop", fake_sleep)

    await entrypoint.run_forever(stop=stop)

    # Two productive passes in a row, then one idle pass that finally sleeps.
    assert relay.calls == 3
    assert sleeps == [INTERVAL]


async def test_a_dead_transport_backs_off_instead_of_draining(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    stop = asyncio.Event()
    sleeps: list[float] = []
    relay = FakeRelay(RelayReport(claimed=3, published=0, failed=3))

    async def fake_sleep(_stop: asyncio.Event, seconds: float) -> None:
        sleeps.append(seconds)
        stop.set()

    monkeypatch.setattr(entrypoint, "build_relay", lambda: relay)
    monkeypatch.setattr(entrypoint, "sleep_or_stop", fake_sleep)

    with caplog.at_level(logging.WARNING, logger="app.outbox_relay"):
        await entrypoint.run_forever(stop=stop)

    # Draining a broken transport would spend the whole attempt budget in one go.
    assert relay.calls == 1
    assert sleeps == [INTERVAL]
    assert "only 0 of 3 claimed events were published" in caplog.text


async def test_a_partially_degraded_transport_backs_off_too(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    stop = asyncio.Event()
    sleeps: list[float] = []
    # 13 of 25 is exactly the threshold and still drains; 12 of 25 is below it,
    # because draining a half-broken transport burns the attempt budget of the
    # whole backlog in seconds instead of backing off.
    relay = FakeRelay(
        RelayReport(claimed=25, published=13, failed=12),
        RelayReport(claimed=25, published=12, failed=13),
    )

    async def fake_sleep(_stop: asyncio.Event, seconds: float) -> None:
        sleeps.append(seconds)
        stop.set()

    monkeypatch.setattr(entrypoint, "build_relay", lambda: relay)
    monkeypatch.setattr(entrypoint, "sleep_or_stop", fake_sleep)

    with caplog.at_level(logging.WARNING, logger="app.outbox_relay"):
        await entrypoint.run_forever(stop=stop)

    # The healthy pass drained, the degraded one slept instead of continuing.
    assert relay.calls == 2
    assert sleeps == [INTERVAL]
    assert "only 12 of 25 claimed events were published" in caplog.text


def test_the_backoff_doubles_and_then_stays_capped() -> None:
    assert entrypoint.pass_backoff(INTERVAL, 1) == INTERVAL
    assert entrypoint.pass_backoff(INTERVAL, 2) == INTERVAL * 2
    assert entrypoint.pass_backoff(INTERVAL, 3) == INTERVAL * 4
    assert (
        entrypoint.pass_backoff(INTERVAL, 1000) == entrypoint.MAX_PASS_BACKOFF_SECONDS
    )


async def test_consecutive_failed_passes_sleep_longer_each_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    sleeps: list[float] = []
    relay = FakeRelay(RuntimeError("down"), RuntimeError("still down"))

    async def fake_sleep(_stop: asyncio.Event, seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 2:
            stop.set()

    monkeypatch.setattr(entrypoint, "build_relay", lambda: relay)
    monkeypatch.setattr(entrypoint, "sleep_or_stop", fake_sleep)

    await entrypoint.run_forever(stop=stop)

    # A database that is down for minutes is not retried every five seconds.
    assert relay.calls == 2
    assert sleeps == [INTERVAL, INTERVAL * 2]


async def test_a_failed_pass_is_logged_and_the_loop_continues(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    stop = asyncio.Event()
    sleeps: list[float] = []
    relay = FakeRelay(RuntimeError("database is down"))

    async def fake_sleep(_stop: asyncio.Event, seconds: float) -> None:
        sleeps.append(seconds)
        stop.set()

    monkeypatch.setattr(entrypoint, "build_relay", lambda: relay)
    monkeypatch.setattr(entrypoint, "sleep_or_stop", fake_sleep)

    with caplog.at_level(logging.ERROR, logger="app.outbox_relay"):
        await entrypoint.run_forever(stop=stop)

    assert relay.calls == 1
    assert sleeps == [INTERVAL]
    assert "relay pass failed" in caplog.text


# ---------------------------------------------------------------------------
# The transport's lifetime
# ---------------------------------------------------------------------------


async def test_a_stopped_loop_closes_the_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broker session left open turns a graceful SIGTERM stop into a SIGKILL."""
    stop = asyncio.Event()
    relay = FakeRelay(RelayReport())

    async def fake_sleep(_stop: asyncio.Event, _seconds: float) -> None:
        stop.set()

    monkeypatch.setattr(entrypoint, "build_relay", lambda: relay)
    monkeypatch.setattr(entrypoint, "sleep_or_stop", fake_sleep)

    await entrypoint.run_forever(stop=stop)

    assert relay.closed


async def test_once_closes_the_transport_even_when_the_pass_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--once` is a cron job: leaking one connection per run exhausts the broker."""
    relay = FakeRelay(RuntimeError("database is down"))

    monkeypatch.setattr(entrypoint, "build_relay", lambda: relay)

    with pytest.raises(RuntimeError, match="database is down"):
        await entrypoint.run_once()

    assert relay.closed
