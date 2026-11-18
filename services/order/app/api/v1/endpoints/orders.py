"""Order endpoints: create, list, read and cancel.

Every error the service raises is translated here, once. `detail` strings stay
generic on purpose: `404` is used for both "does not exist" and "belongs to
someone else" so the API never confirms that a foreign order id is real.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Query, Response, status

from app.api.deps import CurrentUserId, OrderServiceDep
from app.api.pagination import InvalidCursorError, decode_cursor, encode_cursor
from app.domain.exceptions import InvalidOrderError
from app.schemas.order import (
    CancelRequest,
    OrderCreate,
    OrderListResponse,
    OrderResponse,
)
from app.services.exceptions import (
    IdempotencyConflictError,
    InsufficientStockError,
    InventoryItemNotFoundError,
    InventoryRejectedError,
    OrderNotCancellableError,
    OrderNotFoundError,
    OrderStateChangedError,
)

router = APIRouter(prefix="/orders", tags=["orders"])

#: Repeating a request with the same key must return the same order instead of
#: creating a second one. `max_length` matches the column, so a key that could
#: never be stored is rejected with 422 instead of a database error.
IdempotencyKeyHeader = Annotated[
    str | None,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description=(
            "Client-generated key that makes POST /orders replay-safe. "
            "The same key with a different body is rejected with 409."
        ),
    ),
]

_ERROR_HTTP_STATUS: dict[type[Exception], tuple[int, str]] = {
    OrderNotFoundError: (status.HTTP_404_NOT_FOUND, "Order not found"),
    OrderNotCancellableError: (
        status.HTTP_409_CONFLICT,
        "Order cannot be cancelled in its current state",
    ),
    OrderStateChangedError: (
        status.HTTP_409_CONFLICT,
        "Order state changed while the reservation was in flight",
    ),
    IdempotencyConflictError: (
        status.HTTP_409_CONFLICT,
        "Idempotency-Key was already used with a different payload",
    ),
    InsufficientStockError: (status.HTTP_409_CONFLICT, "Insufficient stock"),
    InventoryItemNotFoundError: (
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        "Unknown product or warehouse",
    ),
    InventoryRejectedError: (
        status.HTTP_502_BAD_GATEWAY,
        "Inventory rejected the reservation request",
    ),
    InvalidOrderError: (
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        "Order request is invalid",
    ),
    InvalidCursorError: (
        status.HTTP_400_BAD_REQUEST,
        "Malformed pagination cursor",
    ),
}


def _http_error(exc: Exception) -> HTTPException:
    status_code, detail = _ERROR_HTTP_STATUS.get(
        type(exc),
        (status.HTTP_500_INTERNAL_SERVER_ERROR, "Unexpected error"),
    )
    return HTTPException(status_code=status_code, detail=detail)


@router.post(
    "",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_200_OK: {"description": "Idempotent replay: order already exists"},
        status.HTTP_202_ACCEPTED: {
            "description": "Reservation outcome unknown; the order stays `pending`",
        },
        status.HTTP_409_CONFLICT: {"description": "Stock refusal or key reuse"},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid order"},
        status.HTTP_502_BAD_GATEWAY: {"description": "Inventory rejected the request"},
    },
)
async def create_order(
    payload: OrderCreate,
    response: Response,
    user_id: CurrentUserId,
    service: OrderServiceDep,
    idempotency_key: IdempotencyKeyHeader = None,
) -> OrderResponse:
    """Create an order and reserve stock through the saga.

    `user_id` comes from the verified token only; the schema forbids it in the
    body. Status codes: 201 new, 200 replay, 202 accepted-but-unconfirmed.
    """
    try:
        result = await service.create_order(
            user_id=user_id,
            lines=[item.to_line() for item in payload.items],
            idempotency_key=idempotency_key,
        )
    except (
        IdempotencyConflictError,
        InsufficientStockError,
        InventoryItemNotFoundError,
        InventoryRejectedError,
        InvalidOrderError,
    ) as exc:
        raise _http_error(exc) from exc

    if not result.created:
        response.status_code = status.HTTP_200_OK
    elif result.reservation_pending:
        response.status_code = status.HTTP_202_ACCEPTED

    return OrderResponse.from_order(result.record.order, result.record.items)


@router.get(
    "",
    response_model=OrderListResponse,
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Malformed pagination cursor"},
    },
)
async def list_orders(
    user_id: CurrentUserId,
    service: OrderServiceDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> OrderListResponse:
    """Newest-first feed with keyset pagination.

    No `offset`: a page offset shifts when a new order arrives, which makes the
    client skip or duplicate rows. The cursor is an opaque position in this
    caller's own feed.
    """
    before = None
    if cursor is not None:
        try:
            before = decode_cursor(cursor)
        except InvalidCursorError as exc:
            raise _http_error(exc) from exc

    records, next_cursor = await service.list_orders(
        user_id=user_id,
        limit=limit,
        before=before,
    )

    return OrderListResponse(
        items=[
            OrderResponse.from_order(record.order, record.items) for record in records
        ],
        next_cursor=encode_cursor(next_cursor) if next_cursor is not None else None,
    )


@router.get(
    "/{order_id}",
    response_model=OrderResponse,
    responses={status.HTTP_404_NOT_FOUND: {"description": "Order not found"}},
)
async def get_order(
    order_id: uuid.UUID,
    user_id: CurrentUserId,
    service: OrderServiceDep,
) -> OrderResponse:
    try:
        record = await service.get_order(order_id=order_id, user_id=user_id)
    except OrderNotFoundError as exc:
        raise _http_error(exc) from exc

    return OrderResponse.from_order(record.order, record.items)


@router.post(
    "/{order_id}/cancel",
    response_model=OrderResponse,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Order not found"},
        status.HTTP_409_CONFLICT: {
            "description": "Order is not pending, or the reservation lease is fresh",
        },
    },
)
async def cancel_order(
    order_id: uuid.UUID,
    payload: CancelRequest,
    user_id: CurrentUserId,
    service: OrderServiceDep,
) -> OrderResponse:
    """Cancel a `pending` order.

    Refused while the reservation lease is fresh: the reserve call may still land
    and Inventory cannot release stock, so an early cancel would strand it.
    """
    try:
        record = await service.cancel_order(
            order_id=order_id,
            user_id=user_id,
            reason=payload.reason,
        )
    except (OrderNotFoundError, OrderNotCancellableError) as exc:
        raise _http_error(exc) from exc

    return OrderResponse.from_order(record.order, record.items)
