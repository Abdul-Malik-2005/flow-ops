"""Tests for the API schemas: input bounds and response shape."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.domain.order import OrderLine, OrderReason
from app.models.order import Order, OrderStatus
from app.models.order_item import OrderItem
from app.schemas.order import (
    MAX_ORDER_LINES,
    CancelRequest,
    OrderCreate,
    OrderItemCreate,
    OrderListResponse,
    OrderResponse,
)

PRODUCT = uuid.UUID("11111111-1111-4111-8111-111111111111")
WAREHOUSE = uuid.UUID("33333333-3333-4333-8333-333333333333")
NOW = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)


def build_item(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "product_id": str(PRODUCT),
        "warehouse_id": str(WAREHOUSE),
        "quantity": 1,
    }
    values.update(overrides)
    return values


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------


def test_valid_request_is_accepted() -> None:
    payload = OrderCreate(items=[OrderItemCreate.model_validate(build_item())])

    assert payload.items[0].to_line() == OrderLine(PRODUCT, WAREHOUSE, 1)


@pytest.mark.parametrize("quantity", [0, -3])
def test_quantity_must_be_positive(quantity: int) -> None:
    with pytest.raises(ValidationError):
        OrderItemCreate.model_validate(build_item(quantity=quantity))


def test_an_order_needs_at_least_one_item() -> None:
    with pytest.raises(ValidationError):
        OrderCreate(items=[])


def test_position_count_is_capped() -> None:
    items = [
        build_item(product_id=str(uuid.uuid4())) for _ in range(MAX_ORDER_LINES + 1)
    ]

    with pytest.raises(ValidationError):
        OrderCreate(items=items)


def test_unknown_fields_are_rejected() -> None:
    """`user_id` in the body must fail loudly, not be silently ignored."""
    with pytest.raises(ValidationError):
        OrderCreate(items=[build_item()], user_id=str(uuid.uuid4()))

    with pytest.raises(ValidationError):
        OrderItemCreate.model_validate(build_item(unit_price="0.01"))


def test_cancel_reason_defaults_to_user_requested() -> None:
    assert CancelRequest().reason is OrderReason.USER_REQUESTED


def test_failure_reasons_cannot_be_requested_by_a_client() -> None:
    with pytest.raises(ValidationError):
        CancelRequest(reason=OrderReason.INSUFFICIENT_STOCK)


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------


def test_response_hides_internal_fields_and_includes_items() -> None:
    order = Order(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        status=OrderStatus.PENDING,
        currency="RUB",
        total_amount=None,
        created_at=NOW,
        updated_at=NOW,
    )
    item = OrderItem(
        id=uuid.uuid4(),
        order_id=order.id,
        product_id=PRODUCT,
        warehouse_id=WAREHOUSE,
        quantity=2,
        unit_price=None,
        line_total=None,
    )

    response = OrderResponse.from_order(order, [item])
    dumped = response.model_dump()

    assert set(dumped) == {
        "id",
        "status",
        "currency",
        "total_amount",
        "created_at",
        "updated_at",
        "items",
    }
    assert dumped["status"] is OrderStatus.PENDING
    assert dumped["total_amount"] is None
    assert dumped["items"][0]["quantity"] == 2


def test_list_response_has_no_cursor_by_default() -> None:
    assert OrderListResponse(items=[]).next_cursor is None
