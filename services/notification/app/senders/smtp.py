"""SMTP transport for notifications: one email per recorded fact.

The adapter owns a single lazy `aiosmtplib.SMTP` connection and reuses it across
sends; a failed connection is dropped so the next attempt reconnects. A broken
session is never reused: an SMTP conversation is stateful, and a half-dead one
would turn every later send into a failure until the process restarts.

`Message-ID` is the `event_id`, so a replay of the same at-least-once row produces
the same id — a receiving server (and Mailpit) can deduplicate it, and an operator
can correlate an email with an inbox row by id. That is the email analogue of the
idempotency key the publisher hands a broker.

The connection is opened on the first `send`, not in `__init__`, so building the
sender (and therefore `python -m app.sender --once` with a bad config) reports the
problem where the row's lease protects the retry.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from email.message import EmailMessage
from typing import Any, Final, Protocol

import aiosmtplib

from app.domain.notifications import NotificationMessage

logger = logging.getLogger(__name__)

#: Bound on one connection attempt, handshake included. A broker that stalls must
#: fail within the pass, not after another sender already took the row over.
CONNECT_TIMEOUT_SECONDS: Final = 10.0


class SmtpClient(Protocol):
    """The subset of `aiosmtplib.SMTP` this adapter uses.

    Injected so the adapter is testable without a mail server: a fake that records
    `send_message` proves the envelope without an SMTP conversation.
    """

    @property
    def is_connected(self) -> bool: ...

    async def connect(self) -> Any: ...

    async def login(self, username: str, password: str) -> Any: ...

    async def send_message(self, message: EmailMessage) -> Any: ...

    async def quit(self) -> Any: ...


#: Builds (but does not connect) a fresh client. The default is the real class.
ClientFactory = Callable[[], SmtpClient]


class SmtpNotificationSender:
    """`NotificationSenderPort` adapter that delivers each notification as email."""

    def __init__(
        self,
        *,
        host: str,
        from_addr: str,
        port: int = 1025,
        username: str = "",
        password: str = "",
        timeout: float = CONNECT_TIMEOUT_SECONDS,
        use_tls: bool = False,
        message_id_domain: str = "flowops",
        client_factory: ClientFactory | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._from_addr = from_addr
        self._timeout = timeout
        self._use_tls = use_tls
        self._message_id_domain = message_id_domain
        self._client_factory = client_factory or self._default_factory
        self._client: SmtpClient | None = None

    def _default_factory(self) -> SmtpClient:
        return aiosmtplib.SMTP(
            hostname=self._host,
            port=self._port,
            timeout=self._timeout,
            use_tls=self._use_tls,
        )

    async def send(
        self,
        *,
        event_id: uuid.UUID,
        message: NotificationMessage,
    ) -> None:
        client = await self._ready_client()
        envelope = self._build(event_id, message)
        try:
            await client.send_message(envelope)
        except Exception:
            # A broken session is not reused: drop it so the next attempt
            # reconnects, and re-raise so the row keeps `sent_at IS NULL`.
            await self._drop()
            raise

    async def aclose(self) -> None:
        """Close the SMTP session; never raise.

        Shutdown is not the place to raise, and the server reaps a half-closed
        conversation on its own.
        """
        await self._drop()

    async def _ready_client(self) -> SmtpClient:
        client = self._client
        if client is not None and client.is_connected:
            return client
        client = self._client_factory()
        await client.connect()
        if self._username:
            await client.login(self._username, self._password)
        self._client = client
        return client

    async def _drop(self) -> None:
        client, self._client = self._client, None
        if client is None:
            return
        try:
            await client.quit()
        except Exception:
            logger.warning(
                "closing the SMTP connection failed; the server will reap it",
                exc_info=True,
            )

    def _build(
        self,
        event_id: uuid.UUID,
        message: NotificationMessage,
    ) -> EmailMessage:
        envelope = EmailMessage()
        envelope["From"] = self._from_addr
        envelope["To"] = message.recipient
        envelope["Subject"] = message.subject
        # Stable across a replay: the id a mail server can deduplicate by.
        envelope["Message-ID"] = f"<{event_id}@{self._message_id_domain}>"
        envelope.set_content(message.body)
        return envelope
