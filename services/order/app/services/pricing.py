"""Pricing port: prices come from Catalog, which does not exist yet.

`NullPricing` deliberately answers "unknown" rather than zero. A `0.00` total is
a lie the Payment service could act on, while `NULL` forces every consumer to
handle the missing price explicitly — the same choice the `orders.total_amount`
column makes.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from app.domain.order import OrderLine


@dataclass(frozen=True, slots=True)
class PricedLine:
    """A line plus its snapshot price. `None` means "not known yet"."""

    line: OrderLine
    unit_price: Decimal | None
    line_total: Decimal | None


class PricingPort(Protocol):
    async def price(
        self,
        *,
        lines: Sequence[OrderLine],
        currency: str,
    ) -> Sequence[PricedLine]: ...


class NullPricing:
    """Placeholder until Catalog exposes batch pricing.

    An async signature from day one: the real implementation will be an HTTP
    call, and changing the port later would ripple through the saga.
    """

    async def price(
        self,
        *,
        lines: Sequence[OrderLine],
        currency: str,
    ) -> Sequence[PricedLine]:
        return tuple(
            PricedLine(line=line, unit_price=None, line_total=None) for line in lines
        )


def total_amount(priced_lines: Sequence[PricedLine]) -> Decimal | None:
    """Sum the snapshots, or `None` if any line is still unpriced.

    Partial totals are worse than no total: they look authoritative and would
    let Payment charge an incomplete amount.
    """
    if not priced_lines:
        return None

    total = Decimal(0)
    for priced in priced_lines:
        if priced.line_total is None:
            return None
        total += priced.line_total

    return total
