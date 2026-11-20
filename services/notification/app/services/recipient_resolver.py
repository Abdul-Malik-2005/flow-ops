"""Recipient resolution: a `user_id` in the snapshot, an address in the world.

Order publishes a `user_id`, never an email: the payload is Order's snapshot, and
Order must not know Identity's data. The email therefore has to be resolved by
Notification, and that resolution is a port so the real lookup (an HTTP call to
Identity once it exists) can replace the local stand-in without touching the
sender loop.

Nothing here is a placeholder to be forgotten: a wrong address is the one failure
a customer notices immediately, so the dev resolver deliberately builds an
obviously fake address and never guesses a real mailbox.
"""

from __future__ import annotations

import uuid
from typing import Protocol


class RecipientResolverPort(Protocol):
    """How a sender turns the `user_id` it has into the address it needs."""

    async def resolve(self, *, user_id: uuid.UUID) -> str:
        """Return the address to deliver to for this user."""
        ...


class LocalRecipientResolver:
    """Dev stand-in: a stable, obviously fake address derived from the id.

    Mailpit accepts any address, so the whole pipeline — compose, resolve, send —
    is testable without Identity. The moment Identity exposes a lookup, this class
    is replaced by an HTTP adapter; neither the composer nor the transport changes.
    """

    def __init__(self, *, domain: str = "flowops.local") -> None:
        self._domain = domain

    async def resolve(self, *, user_id: uuid.UUID) -> str:
        return f"{user_id}@{self._domain}"
