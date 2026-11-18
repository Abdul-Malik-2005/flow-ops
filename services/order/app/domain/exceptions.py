"""Rule violations raised by `app.domain`.

They carry no HTTP knowledge: the API layer maps them to status codes in one
place, and the messages describe the rule (never token or request internals that
could leak across an ownership boundary).
"""

from __future__ import annotations

from app.models.order import OrderStatus


class OrderDomainError(Exception):
    """Base class for an order command that violates a business rule."""


class InvalidOrderError(OrderDomainError):
    """The command itself is unusable: no lines, bad quantity, bad currency."""


class InvalidTransitionError(OrderDomainError):
    """The order cannot move to the requested status from its current one."""

    def __init__(self, current: OrderStatus, target: OrderStatus) -> None:
        super().__init__(f"cannot move an order from '{current}' to '{target}'")
        self.current = current
        self.target = target


class InvalidReasonError(OrderDomainError):
    """The audit reason does not belong to the destination status."""
