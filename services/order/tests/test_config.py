"""Settings invariants that protect the reservation lease.

The lease closes the "cancel while the reserve call is in flight" race: a fresh
`reservation_started_at` must *prove* that no HTTP call can still be running,
which only holds if the lease is comfortably longer than the call itself.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.core.config import Settings


def build_settings(**overrides: object) -> Settings:
    """Build settings from explicit values only (no `.env`, no surprises)."""
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://test:test@localhost:5432/order_test_db",
        "environment": "testing",
        "inventory_timeout_seconds": 5.0,
        "reservation_stale_after_seconds": 30.0,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


@pytest.fixture(autouse=True)
def _no_transport_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the process environment out of these tests.

    `Settings` still reads the real environment (`_env_file=None` only disables
    the dotenv file), so a developer who exported `RABBITMQ_URL` to run the live
    broker tests would see `test_rabbitmq_transport_requires_an_address` fail on
    a correct validator. Clearing the transport variables makes every assertion
    here about the explicit values the test passed.
    """
    for name in (
        "OUTBOX_PUBLISHER",
        "RABBITMQ_URL",
        "OUTBOX_EXCHANGE",
        "EVENT_CONSUMER_QUEUE",
        "EVENT_CONSUMER_DLX_EXCHANGE",
        "EVENT_CONSUMER_DLQ",
        "EVENT_CONSUMER_PREFETCH_COUNT",
        "EVENT_CONSUMER_MAX_DELIVERY_COUNT",
        "EVENT_CONSUMER_REQUEUE_DELAY_SECONDS",
        "EVENT_CONSUMER_RECONNECT_DELAY_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)


def test_default_lease_is_longer_than_the_inventory_call() -> None:
    settings = build_settings()

    assert settings.reservation_stale_after_seconds >= (
        2 * settings.inventory_timeout_seconds
    )


def test_lease_equal_to_twice_the_timeout_is_accepted() -> None:
    settings = build_settings(
        inventory_timeout_seconds=5.0,
        reservation_stale_after_seconds=10.0,
    )

    assert settings.reservation_stale_after_seconds == 10.0


def test_lease_shorter_than_twice_the_timeout_is_rejected() -> None:
    with pytest.raises(ValidationError, match="at least twice"):
        build_settings(
            inventory_timeout_seconds=5.0,
            reservation_stale_after_seconds=9.9,
        )


def test_lease_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        build_settings(reservation_stale_after_seconds=0)


# ---------------------------------------------------------------------------
# Reconciler
# ---------------------------------------------------------------------------


def test_alert_threshold_cannot_precede_the_lease() -> None:
    """Otherwise every normal reconcile pass would cry wolf."""
    with pytest.raises(ValidationError, match="pending_alert_after_seconds"):
        build_settings(
            reservation_stale_after_seconds=30.0,
            pending_alert_after_seconds=29.9,
        )


def test_alert_threshold_equal_to_the_lease_is_accepted() -> None:
    settings = build_settings(
        reservation_stale_after_seconds=30.0,
        pending_alert_after_seconds=30.0,
    )

    assert settings.pending_alert_after_seconds == 30.0


def test_reconciler_defaults_leave_room_for_retries() -> None:
    settings = build_settings()

    assert settings.reconcile_batch_size >= 1
    assert settings.reservation_max_attempts >= 1
    assert (
        settings.pending_alert_after_seconds > settings.reservation_stale_after_seconds
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"reconcile_batch_size": 0},
        {"reconcile_batch_size": 101},
        {"reconcile_interval_seconds": 0},
        {"reservation_max_attempts": 0},
        {"pending_alert_after_seconds": 0},
    ],
)
def test_out_of_range_reconciler_values_are_rejected(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        build_settings(**overrides)


# ---------------------------------------------------------------------------
# Outbox relay
# ---------------------------------------------------------------------------


def test_outbox_lease_equal_to_twice_the_interval_is_accepted() -> None:
    settings = build_settings(
        outbox_relay_interval_seconds=5.0,
        outbox_lease_seconds=10.0,
    )

    assert settings.outbox_lease_seconds == 10.0


def test_outbox_lease_shorter_than_twice_the_interval_is_rejected() -> None:
    """A short lease lets a second relay republish rows that are in flight."""
    with pytest.raises(ValidationError, match="at least twice"):
        build_settings(
            outbox_relay_interval_seconds=5.0,
            outbox_lease_seconds=9.9,
        )


def test_outbox_defaults_leave_room_for_a_slow_pass() -> None:
    settings = build_settings()

    assert settings.outbox_lease_seconds >= 2 * settings.outbox_relay_interval_seconds
    assert settings.outbox_batch_size >= 1
    assert settings.outbox_max_attempts >= 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"outbox_relay_interval_seconds": 0},
        {"outbox_batch_size": 0},
        {"outbox_batch_size": 501},
        {"outbox_lease_seconds": 0},
        {"outbox_max_attempts": 0},
    ],
)
def test_out_of_range_relay_values_are_rejected(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        build_settings(**overrides)


# ---------------------------------------------------------------------------
# Event transport
# ---------------------------------------------------------------------------


def test_the_log_transport_is_the_default() -> None:
    """No infrastructure must be required to run the relay out of the box."""
    settings = build_settings()

    assert settings.outbox_publisher == "log"
    assert settings.outbox_exchange == "order.events"


def test_an_unknown_transport_is_rejected() -> None:
    """A typo must not silently fall back to the log: events would look sent."""
    with pytest.raises(ValidationError, match="outbox_publisher"):
        build_settings(outbox_publisher="kafka")


def test_rabbitmq_transport_requires_an_address() -> None:
    """Without an address the first pass would burn a batch of attempts."""
    with pytest.raises(ValidationError, match="rabbitmq_url"):
        build_settings(outbox_publisher="rabbitmq")


def test_rabbitmq_transport_requires_an_exchange() -> None:
    with pytest.raises(ValidationError, match="outbox_exchange"):
        build_settings(
            outbox_publisher="rabbitmq",
            rabbitmq_url="amqp://flowops:secret@localhost:5672/",
            outbox_exchange="",
        )


def test_the_address_is_ignored_by_the_log_transport() -> None:
    """The default must not become mandatory just because a broker is configured."""
    settings = build_settings(rabbitmq_url="amqp://flowops:secret@localhost:5672/")

    assert settings.outbox_publisher == "log"


def test_rabbitmq_transport_with_an_address_is_accepted() -> None:
    settings = build_settings(
        outbox_publisher="rabbitmq",
        rabbitmq_url="amqp://flowops:secret@localhost:5672/",
    )

    assert settings.outbox_publisher == "rabbitmq"
    assert settings.outbox_exchange == "order.events"


# ---------------------------------------------------------------------------
# Event consumer
# ---------------------------------------------------------------------------


def test_consumer_defaults_are_safe_without_a_broker() -> None:
    """The API builds the same `Settings` and must not require a broker.

    Hence there is no model validator demanding `rabbitmq_url` when these fields
    are set: the consumer entry point checks the address itself, before it opens
    a connection, and the API keeps starting on a machine without RabbitMQ.
    """
    settings = build_settings()

    assert settings.event_consumer_queue == "order.event-projections"
    assert settings.event_consumer_dlq == "order.event-projections.dlq"
    assert settings.event_consumer_dlx_exchange.endswith(".dlx")
    assert settings.event_consumer_prefetch_count >= 1
    assert settings.event_consumer_max_delivery_count >= 1
    assert settings.event_consumer_requeue_delay_seconds >= 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"event_consumer_prefetch_count": 0},
        {"event_consumer_prefetch_count": 1001},
        {"event_consumer_max_delivery_count": 0},
        {"event_consumer_requeue_delay_seconds": -0.1},
        {"event_consumer_reconnect_delay_seconds": 0},
    ],
)
def test_out_of_range_consumer_values_are_rejected(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        build_settings(**overrides)
