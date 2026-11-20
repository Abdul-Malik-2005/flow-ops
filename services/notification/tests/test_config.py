"""Settings invariants for the consumer.

The service has no API, so `Settings` is only ever built by the consumer and by
tests. The fields that matter are the consumer's: a queue name that belongs to
this service, its own dead-letter topology, and positive limits.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.core.config import Settings


def build_settings(**overrides: object) -> Settings:
    """Build settings from explicit values only (no `.env`, no surprises)."""
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://test:test@localhost:5432/notification_test_db",
        "environment": "testing",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


@pytest.fixture(autouse=True)
def _no_transport_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the process environment out of these tests.

    `Settings` still reads the real environment (`_env_file=None` only disables
    the dotenv file), so a developer who exported `RABBITMQ_URL` to run the live
    broker tests would otherwise see the explicit values shadowed.
    """
    for name in (
        "RABBITMQ_URL",
        "ORDER_EVENTS_EXCHANGE",
        "EVENT_CONSUMER_QUEUE",
        "EVENT_CONSUMER_DLX_EXCHANGE",
        "EVENT_CONSUMER_DLQ",
        "EVENT_CONSUMER_PREFETCH_COUNT",
        "EVENT_CONSUMER_MAX_DELIVERY_COUNT",
        "EVENT_CONSUMER_REPORT_INTERVAL_SECONDS",
        "EVENT_CONSUMER_REQUEUE_DELAY_SECONDS",
        "EVENT_CONSUMER_RECONNECT_DELAY_SECONDS",
        "SENDER_INTERVAL_SECONDS",
        "SENDER_BATCH_SIZE",
        "SENDER_LEASE_SECONDS",
        "SENDER_MAX_ATTEMPTS",
        "SENDER_TRANSPORT",
        "SMTP_HOST",
        "SMTP_PORT",
        "SMTP_USERNAME",
        "SMTP_PASSWORD",
        "SMTP_FROM",
        "SMTP_TIMEOUT_SECONDS",
        "SMTP_USE_TLS",
        "RECIPIENT_DOMAIN",
    ):
        monkeypatch.delenv(name, raising=False)


def test_the_consumer_defaults_name_its_own_topology() -> None:
    settings = build_settings()

    # The exchange belongs to Order; the queue and the dead-letter topology
    # belong to this consumer and must be distinct from Order's.
    assert settings.order_events_exchange == "order.events"
    assert settings.event_consumer_queue == "notification.order-events"
    assert settings.event_consumer_dlx_exchange == "notification.order-events.dlx"
    assert settings.event_consumer_dlq == "notification.order-events.dlq"


def test_the_broker_address_is_optional_for_settings() -> None:
    """Only the consumer entry point needs a broker; the class must build without.

    There is deliberately no validator demanding `rabbitmq_url`: the entry point
    checks it before connecting and exits with code 2, which keeps `Settings`
    usable anywhere the database URL is all that is known.
    """
    settings = build_settings()

    assert settings.rabbitmq_url == ""
    assert settings.event_consumer_prefetch_count >= 1
    assert settings.event_consumer_max_delivery_count >= 1
    assert settings.event_consumer_requeue_delay_seconds >= 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"event_consumer_prefetch_count": 0},
        {"event_consumer_prefetch_count": 1001},
        {"event_consumer_max_delivery_count": 0},
        {"event_consumer_report_interval_seconds": 0},
        {"event_consumer_requeue_delay_seconds": -0.1},
        {"event_consumer_reconnect_delay_seconds": 0},
    ],
)
def test_out_of_range_consumer_values_are_rejected(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        build_settings(**overrides)


# ---------------------------------------------------------------------------
# Sender
# ---------------------------------------------------------------------------


def test_sender_defaults_are_safe() -> None:
    settings = build_settings()

    assert settings.sender_interval_seconds == 5.0
    assert settings.sender_batch_size == 50
    assert settings.sender_lease_seconds == 30.0
    assert settings.sender_max_attempts == 10


def test_sender_lease_equal_to_twice_the_interval_is_accepted() -> None:
    settings = build_settings(sender_interval_seconds=5.0, sender_lease_seconds=10.0)

    assert settings.sender_lease_seconds == 10.0


def test_sender_lease_shorter_than_twice_the_interval_is_rejected() -> None:
    # A pass has to finish before its own lease expires, otherwise a second
    # sender reclaims rows that are still in flight.
    with pytest.raises(ValidationError, match="sender_lease_seconds"):
        build_settings(sender_interval_seconds=5.0, sender_lease_seconds=9.9)


@pytest.mark.parametrize(
    "overrides",
    [
        {"sender_interval_seconds": 0},
        {"sender_batch_size": 0},
        {"sender_batch_size": 501},
        {"sender_lease_seconds": 0},
        {"sender_max_attempts": 0},
    ],
)
def test_out_of_range_sender_values_are_rejected(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        build_settings(**overrides)


# ---------------------------------------------------------------------------
# Sender transport
# ---------------------------------------------------------------------------


def test_the_log_transport_is_the_default() -> None:
    # No infrastructure must be required to run the sender out of the box.
    settings = build_settings()

    assert settings.sender_transport == "log"
    assert settings.recipient_domain == "flowops.local"


def test_an_unknown_transport_is_rejected() -> None:
    # A typo must not silently fall back to the log: notifications would look sent.
    with pytest.raises(ValidationError, match="sender_transport"):
        build_settings(sender_transport="kafka")


def test_smtp_transport_requires_a_host() -> None:
    # Without a host the first pass would claim a batch and burn its attempts.
    with pytest.raises(ValidationError, match="smtp_host"):
        build_settings(sender_transport="smtp", smtp_from="orders@flowops.local")


def test_smtp_transport_requires_a_sender_address() -> None:
    with pytest.raises(ValidationError, match="smtp_from"):
        build_settings(sender_transport="smtp", smtp_host="mailpit")


def test_smtp_transport_with_a_host_and_sender_is_accepted() -> None:
    settings = build_settings(
        sender_transport="smtp",
        smtp_host="mailpit",
        smtp_from="orders@flowops.local",
    )

    assert settings.sender_transport == "smtp"
    assert settings.smtp_port == 1025
