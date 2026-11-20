"""Helpers for integration tests."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.notification_inbox import NotificationInbox

SERVICE_ROOT = Path(__file__).resolve().parents[2]
TEST_DATABASE_SUFFIX = "_test_db"

#: A fixed commit time, so the claim order is deterministic without sleeping.
INBOX_OCCURRED_AT = datetime(2026, 11, 19, 12, 30, tzinfo=UTC)


def run_alembic(database_url: str, *args: str) -> None:
    """Run Alembic in a subprocess.

    `migrations/env.py` calls `asyncio.run()`, which cannot nest inside the
    running pytest event loop. A subprocess also mirrors how the container
    applies migrations on start-up.
    """
    env = {**os.environ, "DATABASE_URL": database_url}
    completed = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=SERVICE_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"alembic {' '.join(args)} failed with exit code "
            f"{completed.returncode}\nstdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )


async def insert_inbox_event(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    event_type: str = "order.reserved",
    event_id: uuid.UUID | None = None,
    aggregate_id: uuid.UUID | None = None,
    payload: dict[str, object] | None = None,
    attempts: int | None = None,
    occurred_at: datetime | None = INBOX_OCCURRED_AT,
    sent_at: datetime | None = None,
) -> uuid.UUID:
    """Write one inbox row outside the consumer, already committed.

    Bypasses the consumer on purpose: these tests are about the sender's storage
    semantics, and a hand-built row proves the sender does not depend on how the
    row was received.
    """
    event_id = event_id or uuid.uuid4()
    aggregate = aggregate_id or uuid.uuid4()
    row = NotificationInbox(
        event_id=event_id,
        aggregate_id=aggregate,
        event_type=event_type,
        payload=(
            payload
            if payload is not None
            else {
                "order_id": str(aggregate),
                "user_id": str(uuid.uuid4()),
                "status": "reserved",
            }
        ),
        occurred_at=occurred_at,
    )
    if attempts is not None:
        row.attempts = attempts
    if sent_at is not None:
        row.sent_at = sent_at

    async with session_factory() as session:
        session.add(row)
        await session.commit()

    return event_id


async def age_inbox_lease(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    event_id: uuid.UUID,
    seconds: float,
) -> None:
    """Move `locked_at` into the past: a lease the sender never came back from.

    Uses the database clock, so the staleness predicate under test is exactly the
    one the claim runs.
    """
    async with session_factory() as session:
        await session.execute(
            update(NotificationInbox)
            .where(NotificationInbox.event_id == event_id)
            .values(
                locked_at=func.now()
                - text("make_interval(secs => :seconds)").bindparams(seconds=seconds)
            )
        )
        await session.commit()


async def inbox_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[NotificationInbox]:
    """Every inbox row, oldest first — the way the sender sees them."""
    async with session_factory() as session:
        rows = await session.scalars(
            select(NotificationInbox).order_by(
                NotificationInbox.occurred_at,
                NotificationInbox.event_id,
            )
        )
        return list(rows)
