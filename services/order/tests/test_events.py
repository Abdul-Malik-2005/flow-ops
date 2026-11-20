"""The payload is the public contract: a consumer parses it, not our ORM.

These tests pin the shape, not the implementation: exact key set, ids as
strings, money as a JSON *string* (never a float), positions in a stable order,
and the reason only where it is allowed. They also prove the event is
JSON-serializable as-is — a leaked `Decimal` or `UUID` would fail here instead of
in the relay's log line.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Sequence
from decimal import Decimal

import pytest

from app.domain.events import (
    OrderEvent,
    OrderEventType,
    order_cancelled,
    order_failed,
    order_reserved,
)
from app.domain.exceptions import InvalidReasonError
from app.domain.order import OrderReason
from app.models.order import Order, OrderStatus
from app.models.order_item import OrderItem
from app.repositories.order import OrderRecord

ORDER_ID = uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa")
USER_ID = uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb")
RESERVATION_ID = uuid.UUID("cccccccc-3333-4333-8333-cccccccccccc")
PRODUCT_A = uuid.UUID("11111111-1111-4111-8111-111111111111")
PRODUCT_B = uuid.UUID("22222222-2222-4222-8222-222222222222")
WAREHOUSE = uuid.UUID("33333333-3333-4333-8333-333333333333")


def build_record(
    *,
    status: OrderStatus = OrderStatus.RESERVED,
    total_amount: Decimal | None = Decimal("1990.00"),
    items: Sequence[OrderItem] | None = None,
) -> OrderRecord:
    order = Order(
        id=ORDER_ID,
        user_id=USER_ID,
        status=status,
        currency="RUB",
        total_amount=total_amount,
        inventory_reservation_id=RESERVATION_ID,
    )
    # Deliberately out of order: the payload must sort positions by itself.
    positions = (
        list(items)
        if items is not None
        else [
            OrderItem(
                order_id=ORDER_ID,
                product_id=PRODUCT_B,
                warehouse_id=WAREHOUSE,
                quantity=1,
            ),
            OrderItem(
                order_id=ORDER_ID,
                product_id=PRODUCT_A,
                warehouse_id=WAREHOUSE,
                quantity=2,
            ),
        ]
    )
    return OrderRecord(order=order, items=positions)


def test_reserved_event_carries_the_order_snapshot() -> None:
    event = order_reserved(record=build_record())

    assert event.event_type is OrderEventType.RESERVED
    assert event.event_type.value == "order.reserved"
    assert event.aggregate_id == ORDER_ID
    assert event.payload == {
        "order_id": str(ORDER_ID),
        "user_id": str(USER_ID),
        "status": "reserved",
        "currency": "RUB",
        "total_amount": "1990.00",
        "items": [
            {
                "product_id": str(PRODUCT_A),
                "warehouse_id": str(WAREHOUSE),
                "quantity": 2,
            },
            {
                "product_id": str(PRODUCT_B),
                "warehouse_id": str(WAREHOUSE),
                "quantity": 1,
            },
        ],
        "inventory_reservation_id": str(RESERVATION_ID),
    }


def test_payload_is_json_ready_and_money_survives_as_a_string() -> None:
    payload = order_reserved(record=build_record()).payload

    # No Decimal and no UUID may leak: `json.dumps` would raise on both.
    decoded = json.loads(json.dumps(payload))

    assert decoded["total_amount"] == "1990.00"
    assert isinstance(decoded["total_amount"], str)
    assert isinstance(decoded["order_id"], str)
    assert decoded["items"][0]["quantity"] == 2


def test_an_unknown_price_stays_null_instead_of_claiming_zero() -> None:
    payload = order_reserved(record=build_record(total_amount=None)).payload

    # `0.00` and "we do not know the price yet" are different statements, and a
    # consumer must be able to tell them apart.
    assert payload["total_amount"] is None


def test_failed_event_carries_the_reason() -> None:
    event = order_failed(
        record=build_record(status=OrderStatus.FAILED),
        reason=OrderReason.INSUFFICIENT_STOCK,
    )

    assert event.event_type is OrderEventType.FAILED
    assert event.payload["status"] == "failed"
    assert event.payload["reason"] == "insufficient_stock"
    assert "inventory_reservation_id" not in event.payload


def test_cancelled_event_carries_the_reason() -> None:
    event = order_cancelled(
        record=build_record(status=OrderStatus.CANCELLED),
        reason=OrderReason.USER_REQUESTED,
    )

    assert event.event_type is OrderEventType.CANCELLED
    assert event.payload["status"] == "cancelled"
    assert event.payload["reason"] == "user_requested"


@pytest.mark.parametrize(
    ("builder", "status", "reason"),
    [
        # A "failed" event must never explain itself with a cancellation reason.
        (order_failed, OrderStatus.FAILED, OrderReason.USER_REQUESTED),
        # ...and a cancellation must never look like a stock rejection.
        (order_cancelled, OrderStatus.CANCELLED, OrderReason.INSUFFICIENT_STOCK),
    ],
)
def test_a_reason_that_contradicts_the_event_is_rejected(
    builder: Callable[..., OrderEvent],
    status: OrderStatus,
    reason: OrderReason,
) -> None:
    with pytest.raises(InvalidReasonError):
        builder(record=build_record(status=status), reason=reason)


def test_every_fact_gets_its_own_event_id() -> None:
    first = order_reserved(record=build_record())
    second = order_reserved(record=build_record())

    # The id is the deduplication key on the consumer side, so reusing it would
    # silently swallow the second fact.
    assert first.event_id != second.event_id


def test_to_outbox_event_maps_the_fact_onto_a_row() -> None:
    event = order_reserved(record=build_record())

    row = event.to_outbox_event()

    assert row.id == event.event_id
    assert row.aggregate_id == ORDER_ID
    # Stored as a plain string, which is what a `varchar` column wants.
    assert row.event_type == "order.reserved"
    assert row.payload == event.payload
    # Delivery state belongs to the relay, not to the producer.
    assert row.published_at is None
    assert row.locked_at is None
