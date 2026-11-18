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
