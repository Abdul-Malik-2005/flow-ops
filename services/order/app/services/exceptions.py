"""Service-level failures and the HTTP meaning they carry.

The exceptions live here, not in the API layer, so the saga can fail without
knowing about status codes; the endpoint maps each one exactly once.
"""

from __future__ import annotations

import uuid


class OrderServiceError(Exception):
    """Base class for a failed order command."""


class OrderNotFoundError(OrderServiceError):
    """Unknown order id or an order owned by someone else (indistinguishable)."""

    def __init__(self, order_id: uuid.UUID) -> None:
        super().__init__(f"order {order_id} does not exist")
        self.order_id = order_id


class OrderNotCancellableError(OrderServiceError):
    """The order is not `pending`, or the reservation lease is still fresh."""

    def __init__(self, order_id: uuid.UUID) -> None:
        super().__init__(f"order {order_id} cannot be cancelled right now")
        self.order_id = order_id


class IdempotencyConflictError(OrderServiceError):
    """The same `Idempotency-Key` was already used with a different body."""

    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            f"idempotency key {idempotency_key!r} was used with a different payload"
        )
        self.idempotency_key = idempotency_key


class OrderStateChangedError(OrderServiceError):
    """The order left `pending` while the reservation call was in flight."""

    def __init__(self, order_id: uuid.UUID) -> None:
        super().__init__(
            f"order {order_id} changed state during reservation; fetch it again"
        )
        self.order_id = order_id


class InsufficientStockError(OrderServiceError):
    """Inventory refused the reservation; the order is now `failed`."""

    def __init__(self, order_id: uuid.UUID) -> None:
        super().__init__(f"order {order_id} failed: insufficient stock")
        self.order_id = order_id


class InventoryItemNotFoundError(OrderServiceError):
    """Inventory does not know one of the products or warehouses."""

    def __init__(self, order_id: uuid.UUID) -> None:
        super().__init__(f"order {order_id} failed: inventory item not found")
        self.order_id = order_id


class InventoryRejectedError(OrderServiceError):
    """Inventory rejected our payload: a bug on the Order side, not the client's."""

    def __init__(self, order_id: uuid.UUID) -> None:
        super().__init__(f"order {order_id} failed: inventory rejected the request")
        self.order_id = order_id
