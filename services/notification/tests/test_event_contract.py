"""The shared contract in `contracts/` is what Notification must understand.

Order already proves that what it publishes satisfies the schema. This module
proves the other half of the hand-off, from the consumer's side and without
importing a line of Order's code:

* the schema is still a valid draft 2020-12 schema;
* the local `OrderEventType` mirror matches the schema's `enum`, so a new event
  type fails here before the queue silently stops binding it;
* every representative envelope this consumer will see validates against the
  schema, with `format` enforced (an id that is not a UUID must fail);
* the schema is *closed* for the producer (an unexpected field is rejected),
  while `tests/test_event_handler.py` proves the consumer stays lenient — the two
  responsibilities are deliberately different.

The contract lives outside the service: it is a cross-service artifact, and a
consumer must be able to read it without installing the producer's dependencies.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from app.domain.events import SCHEMA_VERSION, OrderEventType

#: `tests` -> service root -> `services` -> repository root.
SCHEMA_PATH = (
    Path(__file__).resolve().parents[3] / "contracts" / "order-events.schema.json"
)

FORMAT_CHECKER = FormatChecker()

EVENT_ID = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
ORDER_ID = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
USER_ID = "cccccccc-3333-4333-8333-cccccccccccc"
PRODUCT_ID = "dddddddd-4444-4444-8444-dddddddddddd"
WAREHOUSE_ID = "eeeeeeee-5555-4555-8555-eeeeeeeeeeee"


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    """Load the published schema, failing loudly if it was moved or deleted."""
    if not SCHEMA_PATH.is_file():
        pytest.fail(f"the published event contract is missing: {SCHEMA_PATH}")
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def snapshot(*, status: str) -> dict[str, Any]:
    """The fields every order event carries, so a consumer parses one shape."""
    return {
        "order_id": ORDER_ID,
        "user_id": USER_ID,
        "status": status,
        "currency": "RUB",
        "total_amount": "1990.00",
        "items": [
            {
                "product_id": PRODUCT_ID,
                "warehouse_id": WAREHOUSE_ID,
                "quantity": 2,
            }
        ],
    }


def envelope(event_type: OrderEventType) -> dict[str, Any]:
    """A representative wire message for one event type."""
    if event_type is OrderEventType.RESERVED:
        payload = {**snapshot(status="reserved"), "inventory_reservation_id": USER_ID}
    else:
        payload = {
            **snapshot(status=event_type.value.removeprefix("order.")),
            "reason": "insufficient_stock",
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": EVENT_ID,
        "event_type": event_type.value,
        "aggregate_id": ORDER_ID,
        "occurred_at": "2026-11-19T12:30:00+00:00",
        "payload": payload,
    }


def validate(message: dict[str, Any], contract: dict[str, Any]) -> None:
    Draft202012Validator(
        contract,
        format_checker=FORMAT_CHECKER,
    ).validate(message)


def test_the_contract_is_a_valid_schema(contract: dict[str, Any]) -> None:
    # A typo in the artifact fails a test instead of failing a consumer.
    Draft202012Validator.check_schema(contract)


def test_the_local_event_types_match_the_published_contract(
    contract: dict[str, Any],
) -> None:
    published = set(contract["properties"]["event_type"]["enum"])

    # The only cheap guard against "Order added a type and Notification never
    # learned it": the local enum and the published enum must be equal.
    assert published == {event_type.value for event_type in OrderEventType}


@pytest.mark.parametrize("event_type", list(OrderEventType), ids=lambda e: e.value)
def test_every_accepted_event_type_validates_against_the_contract(
    contract: dict[str, Any],
    event_type: OrderEventType,
) -> None:
    message = envelope(event_type)

    validate(message, contract)


def test_an_id_that_is_not_a_uuid_is_rejected(contract: dict[str, Any]) -> None:
    message = envelope(OrderEventType.RESERVED)
    message["event_id"] = uuid.uuid4().hex

    # Proves the format checker is wired in: this is the id the inbox
    # deduplicates by, so a malformed one must fail at the contract.
    with pytest.raises(ValidationError):
        validate(message, contract)


def test_the_schema_is_closed_for_the_producer(contract: dict[str, Any]) -> None:
    message = envelope(OrderEventType.RESERVED)
    message["debug"] = True

    # The producer's schema rejects an unexpected field; the consumer ignores it
    # (see `test_event_handler.py`). Closing the contract is the producer's job;
    # tolerating the future is the consumer's.
    with pytest.raises(ValidationError):
        validate(message, contract)


def test_the_schema_requires_the_schema_version(contract: dict[str, Any]) -> None:
    message = envelope(OrderEventType.RESERVED)
    del message["schema_version"]

    with pytest.raises(ValidationError):
        validate(message, contract)


def test_the_local_version_matches_the_published_contract(
    contract: dict[str, Any],
) -> None:
    # A copied constant drifts silently; this pins it to the shared schema, so a
    # contract bump that this service never learned fails here before a broker.
    assert SCHEMA_VERSION == contract["properties"]["schema_version"]["const"]
