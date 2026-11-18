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
