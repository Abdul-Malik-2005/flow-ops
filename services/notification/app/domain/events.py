"""The event types this service can understand: a local mirror of the contract.

Notification cannot import Order's code — a service that reaches into another
service's package is not isolated, and the import would also drag in Order's
dependencies. What is shared is the *artifact*: `contracts/order-events.schema.json`
at the repository root. These names are copied from it, not derived from it, and
`tests/test_event_contract.py` fails the moment the two drift apart.

A local enum (rather than bare strings) keeps the allow-list in one place: the
queue bindings, the runtime's subscription and the handler all iterate it, so a
value that is missing from the contract cannot silently start being consumed.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final

#: Version of the wire envelope this service understands. A compatible (additive)
#: change bumps it and an old consumer must tolerate the bump; a breaking change
#: is a new event type (`order.reserved.v2`), not a bump (docs/architecture.md §5).
SCHEMA_VERSION: Final = 1


class OrderEventType(StrEnum):
    """Values of the published `event_type` (also the AMQP routing key)."""

    RESERVED = "order.reserved"
    FAILED = "order.failed"
    CANCELLED = "order.cancelled"
