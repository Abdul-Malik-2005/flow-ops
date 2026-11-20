"""The log adapter is the reference shape a broker adapter will reuse.

These tests pin the wire message, not the logging plumbing: one JSON line per
event, ids as strings, `occurred_at` in ISO with a timezone, and the payload
untouched (money is already a string up the stack — a transport must not
reformat it).
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from app.models.outbox_event import OutboxEvent
from app.publishers.log import LogEventPublisher

EVENT_ID = uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa")
ORDER_ID = uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb")
RESERVATION_ID = uuid.UUID("cccccccc-3333-4333-8333-cccccccccccc")
OCCURRED_AT = datetime(2026, 11, 19, 12, 30, tzinfo=UTC)
LOGGER_NAME = "tests.publisher"

PAYLOAD: dict[str, Any] = {
    "order_id": str(ORDER_ID),
    "user_id": "11111111-1111-4111-8111-111111111111",
    "status": "reserved",
    "currency": "RUB",
    "total_amount": "1990.00",
    "items": [
        {
            "product_id": "22222222-2222-4222-8222-222222222222",
            "warehouse_id": "33333333-3333-4333-8333-333333333333",
            "quantity": 2,
        }
    ],
    "inventory_reservation_id": str(RESERVATION_ID),
}


def build_event(**overrides: Any) -> OutboxEvent:
    values: dict[str, Any] = {
        "id": EVENT_ID,
        "aggregate_id": ORDER_ID,
        "event_type": "order.reserved",
        "payload": dict(PAYLOAD),
        "occurred_at": OCCURRED_AT,
    }
    values.update(overrides)
    return OutboxEvent(**values)


async def publish_and_decode(
    caplog: pytest.LogCaptureFixture,
    *,
    event: OutboxEvent | None = None,
    log: logging.Logger | None = None,
) -> dict[str, Any]:
    """Publish one event through an explicit logger and decode the single line."""
    logger = log or logging.getLogger(LOGGER_NAME)
    caplog.set_level(logging.INFO, logger=logger.name)

    await LogEventPublisher(log=logger).publish(event=event or build_event())

    [record] = [record for record in caplog.records if record.name == logger.name]
    return json.loads(record.getMessage())


async def test_a_publish_emits_one_json_line(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = await publish_and_decode(caplog)

    assert message["event_id"] == str(EVENT_ID)
    assert message["schema_version"] == 1
    assert message["event_type"] == "order.reserved"
    assert message["aggregate_id"] == str(ORDER_ID)
    assert message["occurred_at"] == OCCURRED_AT.isoformat()
    assert message["payload"] == PAYLOAD


async def test_the_payload_is_not_reshaped_by_the_transport(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Money stays a JSON string: a float would already be a broken contract."""
    message = await publish_and_decode(caplog)

    assert message["payload"]["total_amount"] == "1990.00"
    assert message["payload"]["items"] == PAYLOAD["items"]


async def test_an_uncommitted_event_still_formats(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """`occurred_at` is NULL until the row commits; formatting must not raise."""
    message = await publish_and_decode(caplog, event=build_event(occurred_at=None))

    assert message["occurred_at"] is None
    assert message["event_id"] == str(EVENT_ID)


async def test_the_module_logger_is_the_default(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Without injection the line lands in `app.events`, not in a random logger."""
    caplog.set_level(logging.INFO, logger="app.events")

    await LogEventPublisher().publish(event=build_event())

    [record] = caplog.records
    assert record.name == "app.events"
    assert record.levelno == logging.INFO


async def test_a_broken_transport_propagates_instead_of_returning() -> None:
    """The relay marks a row only after `publish()` returned: silence is fatal."""

    class BrokenLog:
        def info(self, message: str) -> None:
            raise RuntimeError("transport is down")

    publisher = LogEventPublisher(log=BrokenLog())  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="transport is down"):
        await publisher.publish(event=build_event())
