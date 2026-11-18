"""API tests: contract, status codes and ownership.

The real service is covered by the integration suite, so here it is replaced by a
fake through `app.dependency_overrides`. That keeps these tests about HTTP:
which code each outcome maps to, which user the service receives, and how the
cursor travels through the URL.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.deps import get_order_service
from app.api.pagination import decode_cursor
from app.domain.order import OrderLine, OrderReason
from app.main import app
from app.models.order import Order, OrderStatus
from app.models.order_item import OrderItem
from app.repositories.order import OrderCursor, OrderRecord
from app.services.exceptions import (
    IdempotencyConflictError,
    InsufficientStockError,
    InventoryItemNotFoundError,
    InventoryRejectedError,
    OrderNotCancellableError,
    OrderNotFoundError,
)
from app.services.order import CreateOrderResult

ORDERS = "/api/v1/orders"
PRODUCT = uuid.UUID("11111111-1111-4111-8111-111111111111")
WAREHOUSE = uuid.UUID("33333333-3333-4333-8333-333333333333")
NOW = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)


def make_record(
    *,
    user_id: uuid.UUID,
    status: OrderStatus = OrderStatus.RESERVED,
    order_id: uuid.UUID | None = None,
) -> OrderRecord:
    order = Order(
        id=order_id or uuid.uuid4(),
        user_id=user_id,
        status=status,
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
    return OrderRecord(order=order, items=[item])


class FakeOrderService:
    """Records how the endpoint called it and replays a canned answer."""

    def __init__(self) -> None:
        self.create_result: CreateOrderResult | None = None
        self.create_error: Exception | None = None
        self.create_calls: list[dict[str, object]] = []

        self.list_result: tuple[list[OrderRecord], OrderCursor | None] = ([], None)
        self.list_calls: list[dict[str, object]] = []

        self.record: OrderRecord | None = None
        self.error: Exception | None = None
        self.cancel_calls: list[dict[str, object]] = []

    async def create_order(
        self,
        *,
        user_id: uuid.UUID,
        lines: list[OrderLine],
        idempotency_key: str | None = None,
    ) -> CreateOrderResult:
        self.create_calls.append(
            {
                "user_id": user_id,
                "lines": lines,
                "idempotency_key": idempotency_key,
            }
        )
        if self.create_error is not None:
            raise self.create_error
        assert self.create_result is not None
        return self.create_result

    async def list_orders(
        self,
        *,
        user_id: uuid.UUID,
        limit: int,
        before: OrderCursor | None = None,
    ) -> tuple[list[OrderRecord], OrderCursor | None]:
        self.list_calls.append({"user_id": user_id, "limit": limit, "before": before})
        return self.list_result

    async def get_order(
        self,
        *,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
    ) -> OrderRecord:
        if self.error is not None:
            raise self.error
        assert self.record is not None
        return self.record

    async def cancel_order(
        self,
        *,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
        reason: OrderReason = OrderReason.USER_REQUESTED,
    ) -> OrderRecord:
        self.cancel_calls.append(
            {"order_id": order_id, "user_id": user_id, "reason": reason}
        )
        if self.error is not None:
            raise self.error
        assert self.record is not None
        return self.record


@pytest.fixture()
def fake_service() -> FakeOrderService:
    return FakeOrderService()


@pytest.fixture()
async def client(
    fake_service: FakeOrderService,
    public_key_file: object,
) -> AsyncIterator[AsyncClient]:
    """ASGI client with the order service replaced by a fake."""
    previous = app.dependency_overrides.copy()
    app.dependency_overrides[get_order_service] = lambda: fake_service
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
        ) as http_client:
            yield http_client
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(previous)


@pytest.fixture()
def auth_headers(make_token: Callable[..., str]) -> dict[str, str]:
    return {"Authorization": f"Bearer {make_token()}"}


BODY = {
    "items": [
        {
            "product_id": str(PRODUCT),
            "warehouse_id": str(WAREHOUSE),
            "quantity": 2,
        }
    ]
}

SOME_ORDER_ID = uuid.UUID("99999999-9999-4999-8999-999999999999")


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


async def test_create_requires_a_token(client: AsyncClient) -> None:
    response = await client.post(ORDERS, json=BODY)

    assert response.status_code == 401


async def test_create_returns_201_and_takes_the_owner_from_the_token(
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
    user_id: uuid.UUID,
) -> None:
    record = make_record(user_id=user_id)
    fake_service.create_result = CreateOrderResult(record=record, created=True)

    response = await client.post(
        ORDERS,
        json=BODY,
        headers={**auth_headers, "Idempotency-Key": "key-1"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["id"] == str(record.order.id)
    assert body["status"] == "reserved"
    assert body["currency"] == "RUB"
    assert body["total_amount"] is None
    assert body["items"][0]["quantity"] == 2

    call = fake_service.create_calls[0]
    # Ownership is never taken from the body: it comes from the verified `sub`.
    assert call["user_id"] == user_id
    assert call["idempotency_key"] == "key-1"
    assert call["lines"] == [OrderLine(PRODUCT, WAREHOUSE, 2)]


@pytest.mark.parametrize(
    ("created", "reservation_pending", "expected"),
    [(False, False, 200), (True, True, 202)],
)
async def test_replay_and_unknown_reservation_have_their_own_codes(
    created: bool,
    reservation_pending: bool,
    expected: int,
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
    user_id: uuid.UUID,
) -> None:
    fake_service.create_result = CreateOrderResult(
        record=make_record(user_id=user_id, status=OrderStatus.PENDING),
        created=created,
        reservation_pending=reservation_pending,
    )

    response = await client.post(ORDERS, json=BODY, headers=auth_headers)

    assert response.status_code == expected


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (InsufficientStockError(SOME_ORDER_ID), 409),
        (IdempotencyConflictError("key-1"), 409),
        (InventoryItemNotFoundError(SOME_ORDER_ID), 422),
        (InventoryRejectedError(SOME_ORDER_ID), 502),
    ],
)
async def test_service_failures_map_to_http_codes(
    error: Exception,
    expected: int,
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
) -> None:
    fake_service.create_error = error

    response = await client.post(ORDERS, json=BODY, headers=auth_headers)

    assert response.status_code == expected
    assert response.json()["detail"]


@pytest.mark.parametrize(
    "body",
    [
        {**BODY, "user_id": str(uuid.uuid4())},
        {"items": []},
        {
            "items": [
                {
                    "product_id": str(PRODUCT),
                    "warehouse_id": str(WAREHOUSE),
                    "quantity": 0,
                }
            ]
        },
        {
            "items": [
                {
                    "product_id": "not-a-uuid",
                    "warehouse_id": str(WAREHOUSE),
                    "quantity": 1,
                }
            ]
        },
    ],
)
async def test_invalid_bodies_are_rejected_before_the_service_runs(
    body: dict[str, object],
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
) -> None:
    response = await client.post(ORDERS, json=body, headers=auth_headers)

    assert response.status_code == 422
    assert fake_service.create_calls == []


async def test_overlong_idempotency_key_is_rejected(
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
) -> None:
    response = await client.post(
        ORDERS,
        json=BODY,
        headers={**auth_headers, "Idempotency-Key": "k" * 256},
    )

    assert response.status_code == 422
    assert fake_service.create_calls == []


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


async def test_list_returns_an_opaque_cursor_that_survives_a_round_trip(
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
    user_id: uuid.UUID,
) -> None:
    position = OrderCursor(created_at=NOW, order_id=uuid.uuid4())
    fake_service.list_result = ([make_record(user_id=user_id)], position)

    first = await client.get(ORDERS, headers=auth_headers)
    assert first.status_code == 200
    payload = first.json()
    assert len(payload["items"]) == 1
    cursor = payload["next_cursor"]
    assert cursor is not None
    assert decode_cursor(cursor) == position

    await client.get(ORDERS, headers=auth_headers, params={"cursor": cursor})
    assert fake_service.list_calls[-1]["before"] == position
    assert fake_service.list_calls[-1]["user_id"] == user_id


async def test_list_passes_the_limit_through(
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
) -> None:
    response = await client.get(ORDERS, headers=auth_headers, params={"limit": 5})

    assert response.status_code == 200
    assert fake_service.list_calls[-1]["limit"] == 5


@pytest.mark.parametrize("limit", [0, 101])
async def test_list_rejects_out_of_range_limits(
    limit: int,
    client: AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    response = await client.get(ORDERS, headers=auth_headers, params={"limit": limit})

    assert response.status_code == 422


@pytest.mark.parametrize("cursor", ["not-a-cursor", "Zm9v"])
async def test_list_rejects_a_malformed_cursor(
    cursor: str,
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
) -> None:
    response = await client.get(ORDERS, headers=auth_headers, params={"cursor": cursor})

    # An unreadable cursor is a client error, never a silent "first page".
    assert response.status_code == 400
    assert fake_service.list_calls == []


# ---------------------------------------------------------------------------
# Get one
# ---------------------------------------------------------------------------


async def test_get_returns_the_same_owner_the_token_carries(
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
    user_id: uuid.UUID,
) -> None:
    record = make_record(user_id=user_id)
    fake_service.record = record

    response = await client.get(f"{ORDERS}/{record.order.id}", headers=auth_headers)

    assert response.status_code == 200
    assert response.json()["id"] == str(record.order.id)


async def test_get_hides_foreign_orders(
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
) -> None:
    fake_service.error = OrderNotFoundError(SOME_ORDER_ID)

    response = await client.get(f"{ORDERS}/{SOME_ORDER_ID}", headers=auth_headers)

    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------


async def test_cancel_defaults_to_the_user_requested_reason(
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
    user_id: uuid.UUID,
) -> None:
    record = make_record(user_id=user_id, status=OrderStatus.CANCELLED)
    fake_service.record = record

    response = await client.post(
        f"{ORDERS}/{record.order.id}/cancel",
        json={},
        headers=auth_headers,
    )

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert fake_service.cancel_calls[0]["reason"] is OrderReason.USER_REQUESTED


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (OrderNotFoundError(SOME_ORDER_ID), 404),
        (OrderNotCancellableError(SOME_ORDER_ID), 409),
    ],
)
async def test_cancel_failures_map_to_http_codes(
    error: Exception,
    expected: int,
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
) -> None:
    fake_service.error = error

    response = await client.post(
        f"{ORDERS}/{SOME_ORDER_ID}/cancel",
        json={},
        headers=auth_headers,
    )

    assert response.status_code == expected


async def test_client_cannot_write_a_failure_reason(
    client: AsyncClient,
    fake_service: FakeOrderService,
    auth_headers: dict[str, str],
) -> None:
    response = await client.post(
        f"{ORDERS}/{SOME_ORDER_ID}/cancel",
        json={"reason": "insufficient_stock"},
        headers=auth_headers,
    )

    assert response.status_code == 422
    assert fake_service.cancel_calls == []
