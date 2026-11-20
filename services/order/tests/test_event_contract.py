"""The published contract in `contracts/` is checked against the real wire bytes.

`tests/test_events.py` pins the payload with exact literals: it answers "did we
change the message?". This module answers a different question — "did the
message and the schema we published drift apart?" — because the schema is the
only thing a consumer in another service ever sees, and nothing in the Python
code would fail if it went stale.

Three things are proven here:

* the schema itself is a valid draft 2020-12 schema (`check_schema`), so a typo
  in it fails a test instead of failing a consumer;
* every event the domain can build is accepted by it, including an event that
  was never committed and therefore has no `occurred_at`;
* it *rejects* what it must reject — a `reserved` event without the reservation
  id, and an unexpected payload field. Without them `oneOf` and
  `unevaluatedProperties` could be decoration, and a schema that accepts
  anything still shows up green.

Formats are enforced explicitly: by default `jsonschema` validates `format` only
when asked, so an id that is not a UUID would slip through.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from app.domain.events import (
    OrderEvent,
    OrderEventType,
    order_cancelled,
    order_failed,
    order_reserved,
)
from app.domain.order import OrderReason
from app.models.order import OrderStatus
from app.publishers.envelope import encode_event
from tests.test_events import build_record

#: The contract lives outside the service: it is a cross-service artifact, and a
#: consumer must be able to read it without installing our dependencies.
SCHEMA_PATH = (
    Path(__file__).resolve().parents[3] / "contracts" / "order-events.schema.json"
)

FORMAT_CHECKER = FormatChecker()

EVENTS: dict[OrderEventType, Callable[[], OrderEvent]] = {
    OrderEventType.RESERVED: lambda: order_reserved(record=build_record()),
    OrderEventType.FAILED: lambda: order_failed(
        record=build_record(status=OrderStatus.FAILED),
        reason=OrderReason.INSUFFICIENT_STOCK,
    ),
    OrderEventType.CANCELLED: lambda: order_cancelled(
        record=build_record(status=OrderStatus.CANCELLED),
        reason=OrderReason.USER_REQUESTED,
    ),
}


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    """Load the published schema, failing loudly if it was moved or deleted."""
    if not SCHEMA_PATH.is_file():
        pytest.fail(f"the published event contract is missing: {SCHEMA_PATH}")
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def wire_message(
    event: OrderEvent,
    *,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    """The message as it lands in the broker, decoded back into Python.

    Goes through `encode_event` on purpose: validating the payload alone would
    miss the envelope, and the envelope is where `event_id` and `occurred_at`
    live — the two fields a consumer relies on most.
    """
    row = event.to_outbox_event()
    row.occurred_at = occurred_at
    return json.loads(encode_event(row))


def validate(message: dict[str, Any], contract: dict[str, Any]) -> None:
    Draft202012Validator(contract, format_checker=FORMAT_CHECKER).validate(message)


def test_the_schema_is_a_valid_2020_12_schema(contract: dict[str, Any]) -> None:
    Draft202012Validator.check_schema(contract)


def test_the_envelope_keys_are_exactly_the_published_ones(
    contract: dict[str, Any],
) -> None:
    message = wire_message(
        EVENTS[OrderEventType.RESERVED](), occurred_at=datetime.now(UTC)
    )

    # Equality, not a subset: a new envelope field is a breaking change for a
    # strict consumer, so it has to be a deliberate edit of the schema too.
    assert set(message) == set(contract["required"])
    assert set(message) == set(contract["properties"])


def test_the_wire_message_carries_the_current_schema_version(
    contract: dict[str, Any],
) -> None:
    message = wire_message(
        EVENTS[OrderEventType.RESERVED](), occurred_at=datetime.now(UTC)
    )

    # The producer emits exactly the version the contract pins; drift would be a
    # silent "we changed the message but not the version" bug.
    assert message["schema_version"] == 1
    assert contract["properties"]["schema_version"]["const"] == 1


def test_the_schema_requires_the_schema_version(contract: dict[str, Any]) -> None:
    message = wire_message(
        EVENTS[OrderEventType.RESERVED](), occurred_at=datetime.now(UTC)
    )
    del message["schema_version"]

    # A message without a version is refused at the contract, so a consumer never
    # has to guess the shape of what it received.
    with pytest.raises(ValidationError):
        validate(message, contract)


@pytest.mark.parametrize(
    ("event_type", "build_event"),
    list(EVENTS.items()),
    ids=[event_type.value for event_type in EVENTS],
)
def test_every_event_the_domain_can_build_satisfies_the_contract(
    contract: dict[str, Any],
    event_type: OrderEventType,
    build_event: Callable[[], OrderEvent],
) -> None:
    message = wire_message(build_event(), occurred_at=datetime.now(UTC))

    assert message["event_type"] == event_type.value
    validate(message, contract)


def test_an_event_that_was_never_committed_is_still_valid(
    contract: dict[str, Any],
) -> None:
    message = wire_message(EVENTS[OrderEventType.RESERVED](), occurred_at=None)

    # The column has a `server_default`, so a row can reach the encoder without a
    # timestamp. The contract allows `null` because a message must never be
    # rejected over a field the transport cannot fix.
    assert message["occurred_at"] is None
    validate(message, contract)


def test_the_schema_rejects_a_reserved_event_without_its_reservation_id(
    contract: dict[str, Any],
) -> None:
    message = wire_message(
        EVENTS[OrderEventType.RESERVED](), occurred_at=datetime.now(UTC)
    )
    del message["payload"]["inventory_reservation_id"]

    # The whole point of `oneOf`: `reserved` has a required field the other types
    # must not carry, so a payload cannot be "almost" reserved.
    with pytest.raises(ValidationError):
        validate(message, contract)


def test_the_schema_rejects_an_unexpected_payload_field(
    contract: dict[str, Any],
) -> None:
    message = wire_message(
        EVENTS[OrderEventType.RESERVED](), occurred_at=datetime.now(UTC)
    )
    message["payload"]["debug"] = True

    # `unevaluatedProperties: false` sits above `allOf`, which is the only form
    # of "closed" that works for a composed payload: `additionalProperties` in
    # the base would reject the fields the second branch adds.
    with pytest.raises(ValidationError):
        validate(message, contract)


def test_the_schema_rejects_an_id_that_is_not_a_uuid(contract: dict[str, Any]) -> None:
    message = wire_message(
        EVENTS[OrderEventType.RESERVED](), occurred_at=datetime.now(UTC)
    )
    message["event_id"] = uuid.uuid4().hex

    # Proves the format checker is wired in: this is also the id consumers
    # deduplicate by, so a malformed one must fail at the contract, not later.
    with pytest.raises(ValidationError):
        validate(message, contract)


def test_the_schema_describes_every_event_type_the_domain_has(
    contract: dict[str, Any],
) -> None:
    published = set(contract["properties"]["event_type"]["enum"])

    # The only cheap guard against "we added a type and forgot the contract":
    # a new member of `OrderEventType` fails here before it reaches a broker.
    assert published == {event_type.value for event_type in OrderEventType}
