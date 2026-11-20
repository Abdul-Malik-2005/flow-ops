"""Log transport for order events: one JSON line per fact on stdout.

There is no consumer service yet (`Notification` and `Payment` come later), so
publishing has nowhere to go but the log. That is not a stub: a container's
stdout *is* a durable-enough transport for a portfolio project, it is
observable, and it fixes the wire shape that the broker adapter reuses — both
encode through `app.publishers.envelope`, so switching transports cannot change
the message.
"""

from __future__ import annotations

import logging

from app.models.outbox_event import OutboxEvent
from app.publishers.envelope import encode_event

#: Events are facts, so they are logged at INFO: an operator filtering warnings
#: should not see them, but `docker compose logs` must show them by default.
logger = logging.getLogger("app.events")


class LogEventPublisher:
    """`EventPublisherPort` adapter that writes each event to the log."""

    def __init__(self, *, log: logging.Logger | None = None) -> None:
        # Injectable for tests: `caplog` works with the module logger too, but an
        # explicit logger keeps the assertion independent of logger setup.
        self._logger = log or logger

    async def publish(self, *, event: OutboxEvent) -> None:
        self._logger.info(encode_event(event))

    async def aclose(self) -> None:
        """Nothing to release: the log adapter opens no resources.

        It exists because the port promises it — that is what lets the relay stop
        the transport without asking which adapter it holds.
        """
