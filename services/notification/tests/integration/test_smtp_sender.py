"""The SMTP adapter against a real Mailpit: send → the server has the message.

The unit test uses a fake client, so it proves the envelope but not the wire. This
one sends over real SMTP and asks Mailpit's HTTP API whether a message with our
subject arrived. Mailpit the *receiver* is the point: it shows what a real mail
server would accept, not just that `send_message` was called.

Gated like the other infra tests: CI has no Mailpit. Run it against compose:

    $env:NOTIFICATION_SMTP_IT = "1"
    $env:SMTP_HOST = "localhost"; $env:SMTP_PORT = "1025"
    poetry run pytest tests/integration/test_smtp_sender.py -q
"""

from __future__ import annotations

import asyncio
import json
import os
import urllib.request
import uuid
from typing import Any

import pytest

from app.domain.notifications import NotificationMessage
from app.senders.smtp import SmtpNotificationSender

pytestmark = pytest.mark.integration

SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
MAILPIT_API_URL = os.getenv(
    "MAILPIT_API_URL",
    "http://localhost:8025/api/v1/messages",
)

if os.getenv("NOTIFICATION_SMTP_IT") != "1":
    pytest.skip(
        "end-to-end SMTP test: set NOTIFICATION_SMTP_IT=1 (and a Mailpit at "
        "SMTP_HOST:SMTP_PORT) to run it",
        allow_module_level=True,
    )


def fetch_messages() -> dict[str, Any]:
    # Blocking stdlib call run in a thread: Mailpit is a test dependency, and
    # pulling httpx into this service just for a test is not worth it.
    with urllib.request.urlopen(MAILPIT_API_URL, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


async def test_a_composed_message_reaches_the_mail_server() -> None:
    event_id = uuid.uuid4()
    subject = f"FlowOps test {event_id}"
    message = NotificationMessage(
        recipient="customer@flowops.local",
        subject=subject,
        body="Order X is confirmed.",
    )
    sender = SmtpNotificationSender(
        host=SMTP_HOST,
        port=SMTP_PORT,
        from_addr="orders@flowops.local",
    )
    try:
        await sender.send(event_id=event_id, message=message)
    finally:
        await sender.aclose()

    # Mailpit ingests asynchronously; poll briefly for the subject we sent.
    found = False
    for _ in range(50):
        payload = await asyncio.to_thread(fetch_messages)
        if any(
            entry.get("Subject") == subject for entry in payload.get("messages", [])
        ):
            found = True
            break
        await asyncio.sleep(0.1)

    assert found, "the email did not reach Mailpit"
