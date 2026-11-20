"""Log transport for notifications: one JSON line per delivered notification.

There is no mail server in a run without infrastructure, so delivery has nowhere
to go but the log. That is not a stub: the port (`NotificationSenderPort`) defines
the seam, and this adapter fixes the shape a real provider receives — `event_id`
(the idempotency key), the recipient, and the composed subject and body. Swapping
in SMTP changes which adapter is built, not what the sender computes.
"""

from __future__ import annotations

import json
import logging
import uuid

from app.domain.notifications import NotificationMessage

#: Notifications are facts, so they are logged at INFO: an operator filtering
#: warnings should not see them, but `docker compose logs` must show them.
logger = logging.getLogger("app.notifications")


class LogNotificationSender:
    """`NotificationSenderPort` adapter that writes each notification to the log."""

    def __init__(self, *, log: logging.Logger | None = None) -> None:
        # Injectable for tests: `caplog` works with the module logger too, but an
        # explicit logger keeps the assertion independent of logger setup.
        self._logger = log or logger

    async def send(
        self,
        *,
        event_id: uuid.UUID,
        message: NotificationMessage,
    ) -> None:
        line = {
            "event_id": str(event_id),
            "recipient": message.recipient,
            "subject": message.subject,
            "body": message.body,
        }
        self._logger.info(
            json.dumps(line, ensure_ascii=False, separators=(",", ":")),
        )

    async def aclose(self) -> None:
        """Nothing to release: the log adapter opens no resources.

        It exists because the port promises it — that is what lets the sender stop
        the transport without asking which adapter it holds.
        """
