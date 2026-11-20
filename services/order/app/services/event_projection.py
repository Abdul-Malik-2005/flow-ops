"""Consumer-side projection: one wire message becomes one journal row.

The handler is deliberately pure: it receives the already-decoded JSON object,
parses it and calls the repository. It opens no session, commits nothing, touches
no broker and never acknowledges anything. Those decisions belong to the runtime,
because only it knows whether the row was committed before the `ack` — and an
`ack` before a commit would lose the event for good.

Parsing is strict where the contract is strict and lenient where it is allowed to
grow:

* **Unknown envelope fields are ignored.** The published schema is closed for the
  producer, but a consumer that rejected extra keys would dead-letter messages
  that are already in flight the day a compatible field is added. The consumer
  must tolerate what it does not understand.
* **A missing required field is fatal.** `occurred_at: null` is valid (a row
  may have been encoded before it committed), but an absent `occurred_at` key is
  not: writing the row without it would silently lose the moment the fact
  happened.
* **A naive timestamp is refused.** `occurred_at` is a `timestamptz`; a value
  without a timezone would force the database to guess the offset, and guessing
  the time of an audit fact is not acceptable. The producer always sends an
  aware timestamp.
* **An unknown `event_type` is refused without a retry.** It is a deterministic
  mismatch between producer and consumer, so requeueing it would only spin.

The exceptions carry no HTTP meaning: the runtime maps every `EventHandlingError`
to a dead letter, not to a status code, and a transient database error is left to
propagate so the message can be requeued.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from app.domain.events import OrderEventType
from app.repositories.projection import ProjectionRepositoryPort


class EventHandlingError(Exception):
    """A message this consumer refuses permanently: retrying it cannot help."""


class MalformedEventError(EventHandlingError):
    """The message is not an envelope the published contract describes."""


class UnsupportedEventTypeError(EventHandlingError):
    """The envelope is well-formed, but its type is outside the allow-list."""


class OrderEventProjector:
    """Journals one delivered event; `True` means this delivery inserted the row.

    `False` is the expected, harmless outcome of an at-least-once replay: the
    repository's `ON CONFLICT DO NOTHING` saw the `event_id` already there. The
    caller still has to commit and acknowledge — a duplicate is a success, not a
    reason to retry.
    """

    def __init__(self, repository: ProjectionRepositoryPort) -> None:
        self._repository = repository

    async def handle(self, message: Mapping[str, Any]) -> bool:
        """Validate one decoded message and record it in the journal.

        Validation happens before the write, so a malformed message never reaches
        the database and never leaves a half-parsed row behind. The repository
        call is a single statement, which keeps the runtime's transaction short
        and its commit-before-ack ordering simple.
        """
        if not isinstance(message, Mapping):
            raise MalformedEventError(
                f"the message must be a JSON object, got {type(message).__name__}"
            )

        event_type = _event_type(_require(message, "event_type"))
        _schema_version(_require(message, "schema_version"))

        return await self._repository.record(
            event_id=_uuid(_require(message, "event_id"), "event_id"),
            aggregate_id=_uuid(_require(message, "aggregate_id"), "aggregate_id"),
            event_type=event_type.value,
            payload=_payload(_require(message, "payload")),
            occurred_at=_occurred_at(message),
        )


def _require(message: Mapping[str, Any], field: str) -> Any:
    """Read a mandatory field, distinguishing "absent" from "null" in the log."""
    if field not in message:
        raise MalformedEventError(f"the envelope is missing {field!r}")
    return message[field]


def _uuid(value: Any, field: str) -> uuid.UUID:
    """Parse an id the consumer will deduplicate and index by.

    Accepts only a string, so a JSON number cannot silently become an id through
    `UUID(int=...)`.
    """
    if not isinstance(value, str):
        raise MalformedEventError(f"{field!r} must be a uuid string")
    try:
        return uuid.UUID(value)
    except ValueError as error:
        raise MalformedEventError(f"{field!r} is not a valid uuid") from error


def _event_type(value: Any) -> OrderEventType:
    """Map the routing key onto the allow-list this consumer subscribes to."""
    if not isinstance(value, str):
        raise MalformedEventError("'event_type' must be a string")
    try:
        return OrderEventType(value)
    except ValueError as error:
        raise UnsupportedEventTypeError(f"unsupported event type {value!r}") from error


def _schema_version(value: Any) -> int:
    """Require a versioned envelope; tolerate a higher, compatible one.

    A bumped `schema_version` denotes an additive change by contract, so an old
    consumer keeps accepting it (docs/architecture.md §5); a breaking change is a
    new event type it never subscribed to. A missing or malformed version is still
    a broken envelope, so it is refused without a retry.
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise MalformedEventError("'schema_version' must be an integer")
    if value < 1:
        raise MalformedEventError(f"'schema_version' must be >= 1, got {value}")
    return value


def _payload(value: Any) -> dict[str, Any]:
    """The payload is stored as-is: it is a snapshot, not a command to replay."""
    if not isinstance(value, dict):
        raise MalformedEventError("'payload' must be a JSON object")
    return value


def _occurred_at(message: Mapping[str, Any]) -> datetime | None:
    """Parse the commit timestamp; `null` is allowed, a naive value is not."""
    value = _require(message, "occurred_at")
    if value is None:
        return None
    if not isinstance(value, str):
        raise MalformedEventError("'occurred_at' must be an ISO 8601 string or null")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise MalformedEventError(
            "'occurred_at' is not a valid ISO 8601 timestamp"
        ) from error
    if parsed.tzinfo is None:
        raise MalformedEventError("'occurred_at' must carry a timezone")
    return parsed
