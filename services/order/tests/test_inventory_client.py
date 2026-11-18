"""Tests for the Inventory client.

`httpx.MockTransport` exercises the real request/response handling without a
socket, so the mapping from HTTP status to saga outcome is pinned down here
instead of being discovered in production.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable

import httpx
import pytest

from app.clients.inventory import (
    RESERVE_PATH,
    SERVICE_TOKEN_HEADER,
    InventoryClient,
    ReservationOutcome,
)
from app.domain.order import OrderLine

ORDER_ID = uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa")
RESERVATION_ID = uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb")
PRODUCT = uuid.UUID("11111111-1111-4111-8111-111111111111")
WAREHOUSE = uuid.UUID("33333333-3333-4333-8333-333333333333")

LINES = [OrderLine(PRODUCT, WAREHOUSE, 2)]

Handler = Callable[[httpx.Request], httpx.Response]


def build_client(handler: Handler) -> InventoryClient:
    return InventoryClient(
        httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="http://inventory.test",
        ),
        base_url="http://inventory.test",
        service_token="s3cret",
    )


async def test_reserved_response_returns_the_reservation_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        # The trailing slash is required: Inventory redirects the slashless path
        # and httpx does not follow redirects by default.
        assert request.url.path == RESERVE_PATH == "/api/v1/reservations/"
        assert request.headers[SERVICE_TOKEN_HEADER] == "s3cret"
        assert json.loads(request.content) == {
            "order_id": str(ORDER_ID),
            "items": [
                {
                    "product_id": str(PRODUCT),
                    "warehouse_id": str(WAREHOUSE),
                    "quantity": 2,
                }
            ],
        }
        return httpx.Response(
            201,
            json={"id": str(RESERVATION_ID), "order_id": str(ORDER_ID)},
        )

    result = await build_client(handler).reserve(order_id=ORDER_ID, lines=LINES)

    assert result.outcome is ReservationOutcome.RESERVED
    assert result.is_reserved
    assert not result.is_retryable
    assert result.reservation_id == RESERVATION_ID
    assert result.status_code == 201


async def test_replay_of_an_existing_reservation_is_still_a_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": str(RESERVATION_ID)})

    result = await build_client(handler).reserve(order_id=ORDER_ID, lines=LINES)

    assert result.outcome is ReservationOutcome.RESERVED
    assert result.status_code == 200


@pytest.mark.parametrize(
    ("status_code", "outcome", "retryable"),
    [
        (409, ReservationOutcome.REJECTED, False),
        (404, ReservationOutcome.ITEM_NOT_FOUND, False),
        (400, ReservationOutcome.INVALID_REQUEST, False),
        (422, ReservationOutcome.INVALID_REQUEST, False),
        (429, ReservationOutcome.UNAVAILABLE, True),
        (500, ReservationOutcome.UNAVAILABLE, True),
        (503, ReservationOutcome.UNAVAILABLE, True),
        (418, ReservationOutcome.UNAVAILABLE, True),
    ],
)
async def test_status_mapping(
    status_code: int,
    outcome: ReservationOutcome,
    retryable: bool,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"detail": "nope"})

    result = await build_client(handler).reserve(order_id=ORDER_ID, lines=LINES)

    assert result.outcome is outcome
    assert result.is_retryable is retryable
    assert result.status_code == status_code
    assert result.reservation_id is None


async def test_a_2xx_without_an_id_is_not_treated_as_reserved() -> None:
    """The reservation may exist but cannot be linked: retry, stay pending."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, json={"detail": "created"})

    result = await build_client(handler).reserve(order_id=ORDER_ID, lines=LINES)

    assert result.outcome is ReservationOutcome.UNAVAILABLE
    assert result.status_code == 201


async def test_non_json_body_on_success_is_not_treated_as_reserved() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, content=b"<html>gateway</html>")

    result = await build_client(handler).reserve(order_id=ORDER_ID, lines=LINES)

    assert result.outcome is ReservationOutcome.UNAVAILABLE


async def test_timeouts_become_a_retryable_outcome() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("inventory is slow", request=request)

    result = await build_client(handler).reserve(order_id=ORDER_ID, lines=LINES)

    assert result.outcome is ReservationOutcome.UNAVAILABLE
    assert result.is_retryable
    assert result.status_code is None


async def test_connection_errors_become_a_retryable_outcome() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    result = await build_client(handler).reserve(order_id=ORDER_ID, lines=LINES)

    assert result.outcome is ReservationOutcome.UNAVAILABLE
    assert result.is_retryable


async def test_empty_service_token_sends_no_header() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert SERVICE_TOKEN_HEADER not in request.headers
        return httpx.Response(201, json={"id": str(RESERVATION_ID)})

    client = InventoryClient(
        httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="http://inventory.test",
        ),
        base_url="http://inventory.test",
        service_token="",
    )

    result = await client.reserve(order_id=ORDER_ID, lines=LINES)

    assert result.outcome is ReservationOutcome.RESERVED


async def test_trailing_slash_in_base_url_does_not_double_the_path() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(201, json={"id": str(RESERVATION_ID)})

    client = InventoryClient(
        httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="http://inventory.test",
        ),
        base_url="http://inventory.test/",
        service_token="s3cret",
    )

    await client.reserve(order_id=ORDER_ID, lines=LINES)

    assert seen == ["/api/v1/reservations/"]
