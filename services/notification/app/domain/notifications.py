"""The notification domain: what the customer is told, and how it reads.

A notification is composed from the fact Order published, not from a template
stored elsewhere: the payload already carries the snapshot a consumer needs, so a
composer is a pure function of `(event_type, payload)`. Rendering it into an email
is a separate step (the transport adapter), which keeps the wording testable
without a mail server.

The composer never raises on a missing optional field: `total_amount` can be
`null` until Catalog provides pricing, and a notification must still go out.
`recipient` is not the composer's business — it needs Identity, so it is resolved
separately by `RecipientResolverPort`.

Localization is deliberately out of scope: the templates are one language. A real
product would key them by locale; the seam for that is this module, not the
transport.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from app.domain.events import OrderEventType


@dataclass(frozen=True, slots=True)
class NotificationDraft:
    """The wording of one notification, before a recipient is attached."""

    subject: str
    body: str


@dataclass(frozen=True, slots=True)
class NotificationMessage:
    """A draft plus the address it goes to: what a transport receives."""

    recipient: str
    subject: str
    body: str


class NotificationComposer:
    """Maps an order fact onto the text a customer will read."""

    def compose(
        self,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> NotificationDraft:
        """Compose one draft; an unknown type is a programming error.

        The consumer's allow-list means only the three published types reach the
        inbox, so an unknown type here is a broken wiring, not a business case: it
        raises instead of silently sending a blank email.
        """
        try:
            known = OrderEventType(event_type)
        except ValueError as error:
            raise ValueError(f"unsupported event type {event_type!r}") from error
        return _BUILDERS[known](payload)


def _reserved(payload: Mapping[str, Any]) -> NotificationDraft:
    return NotificationDraft(
        subject="Your FlowOps order is confirmed",
        body=(
            f"Order {_order_id(payload)} is confirmed. "
            f"Items: {_item_count(payload)}. Total: {_total(payload)}."
        ),
    )


def _failed(payload: Mapping[str, Any]) -> NotificationDraft:
    return NotificationDraft(
        subject="Your FlowOps order could not be placed",
        body=f"Order {_order_id(payload)} failed: {_reason(payload)}.",
    )


def _cancelled(payload: Mapping[str, Any]) -> NotificationDraft:
    return NotificationDraft(
        subject="Your FlowOps order was cancelled",
        body=f"Order {_order_id(payload)} was cancelled: {_reason(payload)}.",
    )


def _order_id(payload: Mapping[str, Any]) -> str:
    order_id = payload.get("order_id")
    return order_id if isinstance(order_id, str) else "unknown"


def _reason(payload: Mapping[str, Any]) -> str:
    reason = payload.get("reason")
    return reason if isinstance(reason, str) else "no reason given"


def _item_count(payload: Mapping[str, Any]) -> int:
    """Sum of quantities; a missing or malformed `items` counts as zero."""
    items = payload.get("items")
    if not isinstance(items, list):
        return 0
    total = 0
    for item in items:
        if isinstance(item, Mapping):
            quantity = item.get("quantity")
            if isinstance(quantity, int):
                total += quantity
    return total


def _total(payload: Mapping[str, Any]) -> str:
    """Money is a decimal string; `null` is "unknown", not "free"."""
    amount = payload.get("total_amount")
    if not isinstance(amount, str):
        return "amount to be calculated"
    currency = payload.get("currency")
    if isinstance(currency, str) and currency:
        return f"{amount} {currency}"
    return amount


#: One builder per published event type; `NotificationComposer` owns the lookup.
_BUILDERS: dict[OrderEventType, Callable[[Mapping[str, Any]], NotificationDraft]] = {
    OrderEventType.RESERVED: _reserved,
    OrderEventType.FAILED: _failed,
    OrderEventType.CANCELLED: _cancelled,
}
