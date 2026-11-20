"""The composer: an order fact becomes the text a customer reads.

Pure and total over the three published types: no broker, no database, no clock.
What matters is that each type reads differently, that optional fields (`total_amount`
is null until Catalog prices the order) never crash a notification, and that an
unknown type is a wiring bug, not a blank email.
"""

from __future__ import annotations

from typing import Any

import pytest

from app.domain.events import OrderEventType
from app.domain.notifications import NotificationComposer, NotificationDraft

ORDER_ID = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"


def payload(**overrides: Any) -> dict[str, Any]:
    """A snapshot as Order publishes it, plus overrides."""
    message: dict[str, Any] = {
        "order_id": ORDER_ID,
        "user_id": "cccccccc-3333-4333-8333-cccccccccccc",
        "currency": "RUB",
        "total_amount": "1990.00",
        "items": [
            {
                "product_id": "dddddddd-4444-4444-8444-dddddddddddd",
                "warehouse_id": "eeeeeeee-5555-4555-8555-eeeeeeeeeeee",
                "quantity": 2,
            }
        ],
    }
    message.update(overrides)
    return message


@pytest.fixture
def composer() -> NotificationComposer:
    return NotificationComposer()


def test_a_reserved_order_says_it_is_confirmed(
    composer: NotificationComposer,
) -> None:
    draft = composer.compose(OrderEventType.RESERVED.value, payload())

    assert isinstance(draft, NotificationDraft)
    assert "confirmed" in draft.subject
    assert ORDER_ID in draft.body
    assert "Items: 2" in draft.body
    assert "1990.00 RUB" in draft.body


def test_a_failed_order_carries_the_reason(composer: NotificationComposer) -> None:
    draft = composer.compose(
        OrderEventType.FAILED.value,
        payload(reason="insufficient_stock"),
    )

    assert "could not be placed" in draft.subject
    assert "insufficient_stock" in draft.body


def test_a_cancelled_order_carries_the_reason(composer: NotificationComposer) -> None:
    draft = composer.compose(
        OrderEventType.CANCELLED.value,
        payload(reason="user_requested"),
    )

    assert "cancelled" in draft.subject
    assert "user_requested" in draft.body


@pytest.mark.parametrize("event_type", list(OrderEventType))
def test_every_published_type_composes(
    composer: NotificationComposer,
    event_type: OrderEventType,
) -> None:
    draft = composer.compose(event_type.value, payload(reason="any"))

    assert draft.subject
    assert draft.body


def test_an_unknown_type_is_a_wiring_error(composer: NotificationComposer) -> None:
    with pytest.raises(ValueError, match="unsupported"):
        composer.compose("order.paid", payload())


def test_a_null_total_and_missing_items_still_compose(
    composer: NotificationComposer,
) -> None:
    # Catalog has not priced the order yet: `null` means "unknown", and the
    # notification must still go out instead of failing the row.
    draft = composer.compose(
        OrderEventType.RESERVED.value,
        {"order_id": ORDER_ID, "total_amount": None},
    )

    assert "Items: 0" in draft.body
    assert "amount to be calculated" in draft.body
