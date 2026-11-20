"""Unit tests for the projector: how a message maps onto one journal row.

No broker and no database here: the repository is a fake that records its
arguments, which is the point of keeping the handler pure. What matters is that
every field is parsed exactly once, that a malformed message never reaches
storage, and that the two kinds of refusal are distinguishable (an unknown type
is not the same incident as a broken envelope).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from app.domain.events import OrderEventType
from app.services.event_projection import (
    EventHandlingError,
    MalformedEventError,
    OrderEventProjector,
    UnsupportedEventTypeError,
)

EVENT_ID = uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa")
ORDER_ID = uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb")
OCCURRED_AT = datetime(2026, 11, 19, 12, 30, tzinfo=UTC)


class FakeProjection:
    """Storage that remembers exactly what the projector asked it to write."""

    def __init__(self, *, recorded: bool = True) -> None:
        self.recorded = recorded
        self.calls: list[dict[str, Any]] = []

    async def record(self, **kwargs: Any) -> bool:
        self.calls.append(kwargs)
        return self.recorded


def envelope(**overrides: Any) -> dict[str, Any]:
    """A valid message as `encode_event` would produce it, plus overrides."""
    message: dict[str, Any] = {
        "schema_version": 1,
        "event_id": str(EVENT_ID),
        "event_type": OrderEventType.RESERVED.value,
        "aggregate_id": str(ORDER_ID),
        "occurred_at": OCCURRED_AT.isoformat(),
        "payload": {"order_id": str(ORDER_ID), "status": "reserved"},
    }
    message.update(overrides)
    return message


def without(field: str) -> dict[str, Any]:
    """A valid message with one mandatory key removed."""
    message = envelope()
    del message[field]
    return message


def build(
    repository: FakeProjection | None = None,
) -> tuple[OrderEventProjector, FakeProjection]:
    repository = repository or FakeProjection()
    return OrderEventProjector(repository), repository


async def test_a_message_becomes_exactly_one_journal_row() -> None:
    projector, repository = build()

    recorded = await projector.handle(envelope())

    assert recorded is True
    # Exact arguments: the journal row is what the next reader of `order.events`
    # will trust, so a silently renamed or re-typed field would be a bug.
    assert repository.calls == [
        {
            "event_id": EVENT_ID,
            "aggregate_id": ORDER_ID,
            "event_type": "order.reserved",
            "payload": {"order_id": str(ORDER_ID), "status": "reserved"},
            "occurred_at": OCCURRED_AT,
        }
    ]


async def test_a_duplicate_delivery_is_not_an_error() -> None:
    projector, repository = build(FakeProjection(recorded=False))

    # At-least-once delivery makes this the normal case after a redelivery: the
    # message must still be acknowledged, so `False` is a result, not a failure.
    assert await projector.handle(envelope()) is False
    assert len(repository.calls) == 1


@pytest.mark.parametrize("event_type", list(OrderEventType))
async def test_every_published_event_type_is_accepted(
    event_type: OrderEventType,
) -> None:
    projector, repository = build()

    # The allow-list *is* `OrderEventType`: a new member is accepted here
    # automatically, and the subscription and the handler cannot drift apart.
    assert await projector.handle(envelope(event_type=event_type.value)) is True
    assert repository.calls[0]["event_type"] == event_type.value


async def test_an_unknown_event_type_is_refused_as_deterministic() -> None:
    projector, repository = build()

    with pytest.raises(UnsupportedEventTypeError) as raised:
        await projector.handle(envelope(event_type="order.created"))

    # The runtime catches the base class to choose the dead-letter path; a
    # requeue would replay a message this consumer will never understand.
    assert isinstance(raised.value, EventHandlingError)
    assert repository.calls == []


async def test_a_higher_schema_version_is_tolerated() -> None:
    projector, repository = build()

    # A bumped version is a *compatible* change by contract
    # (docs/architecture.md §5): an old consumer keeps accepting it instead of
    # dead-lettering a message that is still valid. A breaking change would
    # arrive as a new event type, not as a version bump here.
    assert await projector.handle(envelope(schema_version=2)) is True
    assert repository.calls[0]["event_type"] == "order.reserved"


async def test_an_unknown_envelope_field_is_ignored() -> None:
    projector, repository = build()

    # Forward compatibility: an additive field must not dead-letter a message
    # that was already in flight when it was deployed.
    assert await projector.handle(envelope(debug=True)) is True
    assert repository.calls[0]["event_type"] == "order.reserved"


async def test_a_missing_schema_version_is_refused() -> None:
    projector, repository = build()

    with pytest.raises(MalformedEventError):
        await projector.handle(without("schema_version"))

    assert repository.calls == []


@pytest.mark.parametrize("value", ["1", True, 0, -1])
async def test_a_malformed_schema_version_is_refused(value: Any) -> None:
    projector, repository = build()

    # Present but not a positive integer: the envelope is broken, not merely
    # newer, so it is refused without a retry.
    with pytest.raises(MalformedEventError):
        await projector.handle(envelope(schema_version=value))

    assert repository.calls == []


async def test_an_uncommitted_event_still_has_a_journal_row() -> None:
    projector, repository = build()

    # The contract allows `null`: the row existed in the outbox before `now()`
    # was assigned. Rejecting it would dead-letter a legitimate message.
    assert await projector.handle(envelope(occurred_at=None)) is True
    assert repository.calls[0]["occurred_at"] is None


async def test_a_z_suffixed_timestamp_is_parsed_as_aware_utc() -> None:
    projector, repository = build()

    assert await projector.handle(envelope(occurred_at="2026-11-19T12:30:00Z")) is True

    parsed = repository.calls[0]["occurred_at"]
    assert parsed == OCCURRED_AT
    assert parsed.tzinfo is not None


@pytest.mark.parametrize(
    ("message", "error"),
    [
        # Mandatory keys: absent is different from `null` for `occurred_at`, and
        # either way the journal must not be written with invented values.
        (without("event_id"), MalformedEventError),
        (without("aggregate_id"), MalformedEventError),
        (without("event_type"), MalformedEventError),
        (without("payload"), MalformedEventError),
        (without("occurred_at"), MalformedEventError),
        # Ids are parsed, not trusted: the journal indexes and deduplicates by them.
        (envelope(event_id="not-a-uuid"), MalformedEventError),
        (envelope(event_id=42), MalformedEventError),
        (envelope(aggregate_id="0000-0000"), MalformedEventError),
        # A JSON array or string is not an envelope.
        (["order.reserved"], MalformedEventError),
        # The payload is a snapshot object, never a scalar.
        (envelope(payload=["order.reserved"]), MalformedEventError),
        (envelope(event_type=42), MalformedEventError),
        # A timestamp the database cannot place in time is not a timestamp.
        (envelope(occurred_at="2026-11-19T12:30:00"), MalformedEventError),
        (envelope(occurred_at="yesterday"), MalformedEventError),
        (envelope(occurred_at=1732019400), MalformedEventError),
    ],
    ids=[
        "missing-event-id",
        "missing-aggregate-id",
        "missing-event-type",
        "missing-payload",
        "missing-occurred-at",
        "event-id-not-a-uuid",
        "event-id-not-a-string",
        "aggregate-id-not-a-uuid",
        "message-is-not-an-object",
        "payload-is-not-an-object",
        "event-type-is-not-a-string",
        "occurred-at-without-timezone",
        "occurred-at-not-a-timestamp",
        "occurred-at-not-a-string",
    ],
)
async def test_a_broken_envelope_is_refused_before_storage(
    message: Any,
    error: type[EventHandlingError],
) -> None:
    projector, repository = build()

    with pytest.raises(error):
        await projector.handle(message)

    # Nothing was written: a partially parsed event must never become a row.
    assert repository.calls == []
