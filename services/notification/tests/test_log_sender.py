"""The log transport: one JSON line per notification, nothing to close."""

from __future__ import annotations

import json
import logging
import uuid

import pytest

from app.domain.notifications import NotificationMessage
from app.senders.log import LogNotificationSender

EVENT_ID = uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa")


MESSAGE = NotificationMessage(
    recipient="customer@flowops.local",
    subject="Order confirmed",
    body="Order X is confirmed.",
)


async def test_a_notification_is_logged_with_its_content(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sender = LogNotificationSender()

    with caplog.at_level(logging.INFO, logger="app.notifications"):
        await sender.send(event_id=EVENT_ID, message=MESSAGE)

    line = json.loads(caplog.records[-1].getMessage())
    # The wire shape a real provider receives: the idempotency key and the
    # composed message, not the raw order snapshot.
    assert line == {
        "event_id": str(EVENT_ID),
        "recipient": "customer@flowops.local",
        "subject": "Order confirmed",
        "body": "Order X is confirmed.",
    }


async def test_aclose_is_a_no_op() -> None:
    # The log adapter opens no resources; closing it must not raise.
    await LogNotificationSender().aclose()
