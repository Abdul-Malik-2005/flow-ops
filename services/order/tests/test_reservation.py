"""The shared Inventory-answer mapping.

Both the saga and the reconciler go through these helpers, so the invariants
here are what keep a retried reservation from ending in a different state than
the first attempt would have.
"""

from __future__ import annotations

import pytest

from app.clients.inventory import ReservationOutcome
from app.domain.order import FAILURE_REASONS as DOMAIN_FAILURE_REASONS
from app.services.reservation import FAILURE_REASONS, failure_reason


def test_every_mapped_reason_is_a_domain_failure_reason() -> None:
    assert set(FAILURE_REASONS.values()) <= DOMAIN_FAILURE_REASONS


def test_retryable_and_success_outcomes_have_no_failure_reason() -> None:
    for outcome in (ReservationOutcome.RESERVED, ReservationOutcome.UNAVAILABLE):
        with pytest.raises(KeyError):
            failure_reason(outcome)


def test_terminal_outcomes_map_to_distinct_reasons() -> None:
    # Two outcomes sharing a reason would make the history useless for triage.
    assert len(set(FAILURE_REASONS.values())) == len(FAILURE_REASONS)
