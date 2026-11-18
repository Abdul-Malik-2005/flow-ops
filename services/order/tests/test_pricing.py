"""Tests for the pricing placeholder.

The contract worth pinning down is "unknown is not zero": as long as Catalog is
absent, no order may carry a numeric total.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

from app.domain.order import OrderLine
from app.services.pricing import NullPricing, PricedLine, total_amount

PRODUCT = uuid.UUID("11111111-1111-4111-8111-111111111111")
WAREHOUSE = uuid.UUID("33333333-3333-4333-8333-333333333333")

LINE = OrderLine(PRODUCT, WAREHOUSE, 2)


async def test_null_pricing_keeps_every_amount_unknown() -> None:
    priced = await NullPricing().price(lines=[LINE], currency="RUB")

    assert priced == (PricedLine(line=LINE, unit_price=None, line_total=None),)


async def test_null_pricing_preserves_the_lines_it_was_given() -> None:
    lines = [LINE, OrderLine(PRODUCT, WAREHOUSE, 3)]

    priced = await NullPricing().price(lines=lines, currency="RUB")

    assert [item.line for item in priced] == lines


async def test_total_is_unknown_while_any_line_is_unpriced() -> None:
    priced = await NullPricing().price(lines=[LINE], currency="RUB")

    assert total_amount(priced) is None


def test_total_is_the_sum_when_every_line_is_priced() -> None:
    priced = [
        PricedLine(LINE, Decimal("10.00"), Decimal("20.00")),
        PricedLine(LINE, Decimal("5.50"), Decimal("5.50")),
    ]

    assert total_amount(priced) == Decimal("25.50")


def test_total_of_an_empty_order_is_unknown() -> None:
    assert total_amount([]) is None
