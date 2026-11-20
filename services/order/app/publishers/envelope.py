"""The wire message of the event transport, shared by every adapter.

The envelope is defined once, here, not inside an adapter: `log` and `rabbitmq`
must publish the *same* bytes, otherwise the contract would be whatever the
adapter in use happens to produce, and a consumer written against the log line
would break the day the broker is switched on.

The line is built by hand rather than by `logging`'s `%s` formatting: the payload
is JSON, and half of it being formatted by a logging handler is how malformed
messages are born. `ensure_ascii=False` keeps Cyrillic product names readable;
`separators` keeps the line compact, since a log file pays for every space.

The helper reshapes **nothing**: money is already a string and ids are already
strings in the payload (`app.domain.events`), and a transport that reformats them
would make the contract depend on the transport.
"""

from __future__ import annotations

import json
from typing import Any

from app.domain.events import SCHEMA_VERSION
from app.models.outbox_event import OutboxEvent


def encode_event(event: OutboxEvent) -> str:
    """Render the message of the transport.

    `occurred_at` may still be `None` on an event that was never committed (the
    column has a `server_default`). The relay only ever sees committed rows, but
    a formatting helper must not raise: an exception here would burn a publish
    attempt over a cosmetic problem.
    """
    message: dict[str, Any] = {
        # Part of the envelope, not the payload: it versions the whole message
        # and is the first thing a consumer reads about its shape.
        "schema_version": SCHEMA_VERSION,
        "event_id": str(event.id),
        "event_type": event.event_type,
        "aggregate_id": str(event.aggregate_id),
        "occurred_at": (
            event.occurred_at.isoformat() if event.occurred_at is not None else None
        ),
        "payload": event.payload,
    }
    return json.dumps(message, ensure_ascii=False, separators=(",", ":"))
