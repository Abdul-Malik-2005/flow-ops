"""The SMTP adapter, pinned down without a mail server.

A fake client records `connect`/`login`/`send_message`/`quit`, which is enough to
prove what the adapter owes the sender: a reused connection, a stable `Message-ID`,
a reconnect after a failed send, and a close that never raises.

The real wire path (a message actually stored by a server) is covered by
`tests/integration/test_smtp_sender.py` against Mailpit.
"""

from __future__ import annotations

import uuid
from email.message import EmailMessage

import pytest

from app.domain.notifications import NotificationMessage
from app.senders.smtp import SmtpNotificationSender

EVENT_ID = uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa")
OTHER_EVENT_ID = uuid.UUID("aaaaaaaa-2222-4222-8222-aaaaaaaaaaaa")
MESSAGE = NotificationMessage(
    recipient="customer@flowops.local",
    subject="Order confirmed",
    body="Order X is confirmed.",
)


class FakeSmtpClient:
    def __init__(self, *, fail_send: bool = False) -> None:
        self.is_connected = False
        self.connected = 0
        self.logins: list[tuple[str, str]] = []
        self.sent: list[EmailMessage] = []
        self.quits = 0
        self.fail_send = fail_send

    async def connect(self) -> None:
        self.is_connected = True
        self.connected += 1

    async def login(self, username: str, password: str) -> None:
        self.logins.append((username, password))

    async def send_message(self, message: EmailMessage) -> None:
        if self.fail_send:
            self.is_connected = False
            raise RuntimeError("smtp is down")
        self.sent.append(message)

    async def quit(self) -> None:
        self.is_connected = False
        self.quits += 1


def build_sender(
    *clients: FakeSmtpClient,
    username: str = "",
) -> SmtpNotificationSender:
    queue = list(clients)
    return SmtpNotificationSender(
        host="mailpit",
        port=1025,
        from_addr="orders@flowops.local",
        username=username,
        password="secret" if username else "",
        client_factory=lambda: queue.pop(0),
    )


def envelope_body(envelope: EmailMessage) -> str:
    content = envelope.get_content()
    return content.strip() if isinstance(content, str) else ""


async def test_a_message_becomes_an_email() -> None:
    client = FakeSmtpClient()
    sender = build_sender(client)

    await sender.send(event_id=EVENT_ID, message=MESSAGE)

    assert client.connected == 1
    [envelope] = client.sent
    assert envelope["To"] == MESSAGE.recipient
    assert envelope["From"] == "orders@flowops.local"
    assert envelope["Subject"] == MESSAGE.subject
    # The idempotency key as the mail server sees it: stable across a replay.
    assert envelope["Message-ID"] == f"<{EVENT_ID}@flowops>"
    assert envelope_body(envelope) == MESSAGE.body.strip()


async def test_the_connection_is_reused_across_sends() -> None:
    client = FakeSmtpClient()
    sender = build_sender(client)

    await sender.send(event_id=EVENT_ID, message=MESSAGE)
    await sender.send(event_id=OTHER_EVENT_ID, message=MESSAGE)

    # One handshake for the whole pass: the adapter keeps the session open.
    assert client.connected == 1
    assert len(client.sent) == 2


async def test_a_configured_username_logs_in() -> None:
    client = FakeSmtpClient()
    sender = build_sender(client, username="flowops")

    await sender.send(event_id=EVENT_ID, message=MESSAGE)

    assert client.logins == [("flowops", "secret")]


async def test_a_failed_send_drops_the_client_and_re_raises() -> None:
    failing = FakeSmtpClient(fail_send=True)
    sender = build_sender(failing)

    with pytest.raises(RuntimeError):
        await sender.send(event_id=EVENT_ID, message=MESSAGE)

    # A half-dead session is never reused: it is closed so the next attempt
    # reconnects, and the re-raise keeps `sent_at` NULL for the retry.
    assert failing.quits == 1


async def test_the_next_send_reconnects_after_a_failure() -> None:
    failing = FakeSmtpClient(fail_send=True)
    healthy = FakeSmtpClient()
    sender = build_sender(failing, healthy)

    with pytest.raises(RuntimeError):
        await sender.send(event_id=EVENT_ID, message=MESSAGE)

    await sender.send(event_id=EVENT_ID, message=MESSAGE)

    assert healthy.connected == 1
    assert len(healthy.sent) == 1


async def test_aclose_quits_once_and_never_raises() -> None:
    client = FakeSmtpClient()
    sender = build_sender(client)
    await sender.send(event_id=EVENT_ID, message=MESSAGE)

    await sender.aclose()
    await sender.aclose()

    assert client.quits == 1


async def test_aclose_without_a_connection_is_a_no_op() -> None:
    sender = build_sender(FakeSmtpClient())

    await sender.aclose()
