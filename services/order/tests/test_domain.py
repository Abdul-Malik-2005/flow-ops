"""Tests for the pure order rules.

No database, no HTTP: the whole point of `app.domain` is that these rules can be
proved in milliseconds and reused by any caller (API, saga, reconciler).
"""

from __future__ import annotations

import uuid

import pytest

from app.domain.exceptions import (
    InvalidOrderError,
    InvalidReasonError,
    InvalidTransitionError,
)
from app.domain.order import (
    ALLOWED_TRANSITIONS,
    CANCELLATION_REASONS,
    FAILURE_REASONS,
    OrderLine,
    OrderReason,
    can_transition,
    canonical_request_hash,
    canonical_request_payload,
    ensure_transition,
    normalize_lines,
)
from app.models.order import OrderStatus

PRODUCT_A = uuid.UUID("11111111-1111-4111-8111-111111111111")
PRODUCT_B = uuid.UUID("22222222-2222-4222-8222-222222222222")
WAREHOUSE = uuid.UUID("33333333-3333-4333-8333-333333333333")
OTHER_WAREHOUSE = uuid.UUID("44444444-4444-4444-8444-444444444444")


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------


def test_pending_can_be_reserved_cancelled_or_failed() -> None:
    assert can_transition(OrderStatus.PENDING, OrderStatus.RESERVED)
    assert can_transition(OrderStatus.PENDING, OrderStatus.CANCELLED)
    assert can_transition(OrderStatus.PENDING, OrderStatus.FAILED)


def test_pending_cannot_be_paid_without_a_reservation() -> None:
    assert not can_transition(OrderStatus.PENDING, OrderStatus.PAID)


@pytest.mark.parametrize(
    "status",
    [
        OrderStatus.RESERVED,
        OrderStatus.PAID,
        OrderStatus.FULFILLED,
        OrderStatus.CANCELLED,
        OrderStatus.FAILED,
    ],
)
def test_terminal_statuses_have_no_outgoing_edges(status: OrderStatus) -> None:
    """`reserved` included: Inventory cannot release, so it cannot be cancelled."""
    assert ALLOWED_TRANSITIONS[status] == frozenset()


def test_ensure_transition_returns_the_target() -> None:
    assert (
        ensure_transition(OrderStatus.PENDING, OrderStatus.RESERVED)
        is OrderStatus.RESERVED
    )


def test_ensure_transition_rejects_an_illegal_step() -> None:
    with pytest.raises(InvalidTransitionError) as excinfo:
        ensure_transition(OrderStatus.CANCELLED, OrderStatus.RESERVED)

    assert excinfo.value.current is OrderStatus.CANCELLED
    assert excinfo.value.target is OrderStatus.RESERVED


def test_cancellation_requires_a_cancellation_reason() -> None:
    with pytest.raises(InvalidReasonError):
        ensure_transition(OrderStatus.PENDING, OrderStatus.CANCELLED)

    with pytest.raises(InvalidReasonError):
        ensure_transition(
            OrderStatus.PENDING,
            OrderStatus.CANCELLED,
            OrderReason.INSUFFICIENT_STOCK,
        )

    assert (
        ensure_transition(
            OrderStatus.PENDING,
            OrderStatus.CANCELLED,
            OrderReason.USER_REQUESTED,
        )
        is OrderStatus.CANCELLED
    )


def test_failure_requires_a_failure_reason() -> None:
    with pytest.raises(InvalidReasonError):
        ensure_transition(OrderStatus.PENDING, OrderStatus.FAILED)

    # Cancellation and failure reasons are disjoint: a stock rejection must not
    # be recorded as a user cancellation.
    with pytest.raises(InvalidReasonError):
        ensure_transition(
            OrderStatus.PENDING,
            OrderStatus.FAILED,
            OrderReason.USER_REQUESTED,
        )

    assert (
        ensure_transition(
            OrderStatus.PENDING,
            OrderStatus.FAILED,
            OrderReason.INSUFFICIENT_STOCK,
        )
        is OrderStatus.FAILED
    )


def test_non_reason_transitions_must_not_carry_a_reason() -> None:
    with pytest.raises(InvalidReasonError):
        ensure_transition(
            OrderStatus.PENDING,
            OrderStatus.RESERVED,
            OrderReason.USER_REQUESTED,
        )


def test_reason_sets_are_disjoint() -> None:
    assert CANCELLATION_REASONS.isdisjoint(FAILURE_REASONS)


def test_every_failure_reason_terminalises_a_pending_order() -> None:
    """Including the reconciler's give-up reason: same gate as the rest."""
    assert OrderReason.RESERVATION_UNAVAILABLE in FAILURE_REASONS

    for reason in FAILURE_REASONS:
        assert (
            ensure_transition(OrderStatus.PENDING, OrderStatus.FAILED, reason)
            is OrderStatus.FAILED
        )


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def test_duplicate_positions_are_merged() -> None:
    normalized = normalize_lines(
        [
            OrderLine(PRODUCT_A, WAREHOUSE, 2),
            OrderLine(PRODUCT_A, WAREHOUSE, 3),
        ]
    )

    assert normalized == (OrderLine(PRODUCT_A, WAREHOUSE, 5),)


def test_different_warehouses_stay_separate() -> None:
    normalized = normalize_lines(
        [
            OrderLine(PRODUCT_A, WAREHOUSE, 1),
            OrderLine(PRODUCT_A, OTHER_WAREHOUSE, 1),
        ]
    )

    assert len(normalized) == 2


def test_normalisation_is_deterministic_and_does_not_mutate_the_input() -> None:
    lines = [
        OrderLine(PRODUCT_B, WAREHOUSE, 1),
        OrderLine(PRODUCT_A, WAREHOUSE, 2),
    ]

    first = normalize_lines(lines)
    second = normalize_lines(list(reversed(lines)))

    assert first == second
    assert lines == [
        OrderLine(PRODUCT_B, WAREHOUSE, 1),
        OrderLine(PRODUCT_A, WAREHOUSE, 2),
    ]


@pytest.mark.parametrize("quantity", [0, -1])
def test_non_positive_quantity_is_rejected(quantity: int) -> None:
    with pytest.raises(InvalidOrderError):
        normalize_lines([OrderLine(PRODUCT_A, WAREHOUSE, quantity)])


def test_an_order_needs_at_least_one_line() -> None:
    with pytest.raises(InvalidOrderError):
        normalize_lines([])


# ---------------------------------------------------------------------------
# Request identity
# ---------------------------------------------------------------------------


def test_hash_is_stable_across_permutations_and_duplicates() -> None:
    straight = [
        OrderLine(PRODUCT_A, WAREHOUSE, 2),
        OrderLine(PRODUCT_B, OTHER_WAREHOUSE, 1),
    ]
    shuffled = [
        OrderLine(PRODUCT_B, OTHER_WAREHOUSE, 1),
        OrderLine(PRODUCT_A, WAREHOUSE, 2),
    ]
    duplicated = [
        OrderLine(PRODUCT_A, WAREHOUSE, 1),
        OrderLine(PRODUCT_A, WAREHOUSE, 1),
        OrderLine(PRODUCT_B, OTHER_WAREHOUSE, 1),
    ]

    expected = canonical_request_hash(straight, currency="RUB")

    assert canonical_request_hash(shuffled, currency="RUB") == expected
    assert canonical_request_hash(duplicated, currency="RUB") == expected


@pytest.mark.parametrize(
    ("lines", "currency"),
    [
        ([OrderLine(PRODUCT_A, WAREHOUSE, 3)], "RUB"),
        ([OrderLine(PRODUCT_A, OTHER_WAREHOUSE, 2)], "RUB"),
        ([OrderLine(PRODUCT_A, WAREHOUSE, 2)], "USD"),
    ],
)
def test_hash_changes_when_the_request_changes(
    lines: list[OrderLine],
    currency: str,
) -> None:
    baseline = canonical_request_hash(
        [OrderLine(PRODUCT_A, WAREHOUSE, 2)],
        currency="RUB",
    )

    assert canonical_request_hash(lines, currency=currency) != baseline


def test_hash_is_a_sha256_hex_digest() -> None:
    digest = canonical_request_hash(
        [OrderLine(PRODUCT_A, WAREHOUSE, 1)],
        currency="RUB",
    )

    assert len(digest) == 64
    assert int(digest, 16) >= 0


def test_canonical_payload_contains_no_prices_or_owner() -> None:
    payload = canonical_request_payload(
        [OrderLine(PRODUCT_A, WAREHOUSE, 1)],
        currency="RUB",
    )

    assert payload == {
        "currency": "RUB",
        "items": [
            {
                "product_id": str(PRODUCT_A),
                "warehouse_id": str(WAREHOUSE),
                "quantity": 1,
            }
        ],
    }


@pytest.mark.parametrize("currency", ["rub", "RU", "RUBB", ""])
def test_currency_must_be_an_upper_case_iso_code(currency: str) -> None:
    with pytest.raises(InvalidOrderError):
        canonical_request_payload(
            [OrderLine(PRODUCT_A, WAREHOUSE, 1)],
            currency=currency,
        )
