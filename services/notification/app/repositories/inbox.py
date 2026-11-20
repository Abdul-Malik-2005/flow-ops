"""Persistence for the inbox: a port plus its SQLAlchemy adapter.

The inbox answers one question: "have I already recorded this event?" Delivery
is at-least-once, so the only safe write is an insert that tolerates its own
duplicate:

    INSERT INTO notification_inbox (...) VALUES (...)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id

`RETURNING` is what makes the outcome observable: a row comes back exactly when
this call inserted it, `None` when somebody else already did. A `SELECT` before
the `INSERT` would be a race between two consumers; an insert plus a caught
`IntegrityError` would poison the caller's transaction for every later statement.

The adapter never commits. The consumer owns the transaction and must commit
**before** acknowledging the message: an ack after a rolled-back insert would
lose the event for good.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import Float, bindparam, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.notification_inbox import NotificationInbox

#: Claim predicate on the database clock: a lease that cannot be compared
#: against `now()` on the same clock is not a lease.
_LEASE_CUTOFF_SQL = "now() - make_interval(secs => :lease_seconds)"


class InboxRepositoryPort(Protocol):
    """What the consumer and the sender need from storage.

    Two halves, one table: `record` is the consumer's idempotent insert, and the
    claim/mark/count trio is the sender's delivery state. They share an adapter
    because they share a row — splitting them would only add a second class with
    the same session.
    """

    async def record(
        self,
        *,
        event_id: uuid.UUID,
        aggregate_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        occurred_at: datetime | None,
    ) -> bool:
        """Record one event; `False` means it was already there.

        The caller commits. `True` is the only case in which a new row appeared,
        so the consumer can log a duplicate without a second query.
        """
        ...

    async def claim_batch(
        self,
        *,
        limit: int,
        lease_seconds: float,
        max_attempts: int,
    ) -> list[NotificationInbox]:
        """Lease up to `limit` unsent rows for one send attempt.

        Rows that already spent `max_attempts` are skipped on purpose: they are
        the dead-letter set, surfaced by `count_exhausted_unsent`. Nothing is
        deleted, so a row that never delivered is never lost — only removed from
        the feed until an operator looks at it.
        """
        ...

    async def mark_sent(self, *, event_id: uuid.UUID) -> bool:
        """Mark a successfully delivered notification.

        Returns `False` when somebody else already marked it (or the row is
        gone), which is how at-least-once delivery stays observable.
        """
        ...

    async def count_exhausted_unsent(self, *, max_attempts: int) -> int:
        """Rows that will never be retried automatically (log only)."""
        ...


class SqlInboxRepository:
    """SQLAlchemy adapter. It never commits: the consumer owns transactions."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(
        self,
        *,
        event_id: uuid.UUID,
        aggregate_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        occurred_at: datetime | None,
    ) -> bool:
        statement = (
            pg_insert(NotificationInbox)
            .values(
                event_id=event_id,
                aggregate_id=aggregate_id,
                event_type=event_type,
                payload=payload,
                occurred_at=occurred_at,
            )
            # The primary key is the dedup key: a replay of the same delivery
            # must be a no-op, not an `IntegrityError` that kills the channel.
            .on_conflict_do_nothing(
                index_elements=[NotificationInbox.event_id],
            )
            .returning(NotificationInbox.event_id)
        )

        recorded = await self._session.scalar(statement)
        return recorded is not None

    async def claim_batch(
        self,
        *,
        limit: int,
        lease_seconds: float,
        max_attempts: int,
    ) -> list[NotificationInbox]:
        lease_cutoff = text(_LEASE_CUTOFF_SQL).bindparams(
            bindparam("lease_seconds", value=lease_seconds, type_=Float)
        )

        candidates = (
            select(NotificationInbox.event_id)
            .where(
                NotificationInbox.sent_at.is_(None),
                # Strict `<`: the claim increments the counter *before* the send
                # call, so `attempts == max` already means "budget spent".
                NotificationInbox.attempts < max_attempts,
                # A fresh lease means another sender is delivering this row now.
                or_(
                    NotificationInbox.locked_at.is_(None),
                    NotificationInbox.locked_at < lease_cutoff,
                ),
            )
            # Oldest fact first, `event_id` breaking ties deterministically. An
            # uncommitted event has a NULL `occurred_at`; Postgres sorts NULLS
            # LAST on ASC, which the Python re-sort below reproduces.
            .order_by(NotificationInbox.occurred_at, NotificationInbox.event_id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

        statement = (
            update(NotificationInbox)
            .where(NotificationInbox.event_id.in_(candidates))
            .values(
                locked_at=func.now(),
                attempts=NotificationInbox.attempts + 1,
            )
            .returning(NotificationInbox)
            .execution_options(synchronize_session=False)
        )

        rows = list((await self._session.scalars(statement)).all())
        if not rows:
            return []

        # `RETURNING` has no defined order; restore the claim order explicitly.
        # The `is None` key keeps a NULL `occurred_at` (uncommitted event) last
        # without comparing it to a datetime.
        rows.sort(
            key=lambda row: (row.occurred_at is None, row.occurred_at, row.event_id)
        )
        return rows

    async def mark_sent(self, *, event_id: uuid.UUID) -> bool:
        statement = (
            update(NotificationInbox)
            .where(
                NotificationInbox.event_id == event_id,
                # Compare-and-set: a row is marked exactly once, even if two
                # senders waited out the same expired lease.
                NotificationInbox.sent_at.is_(None),
            )
            .values(sent_at=func.now())
            .returning(NotificationInbox.event_id)
        )

        marked = await self._session.scalar(statement)
        return marked is not None

    async def count_exhausted_unsent(self, *, max_attempts: int) -> int:
        statement = (
            select(func.count())
            .select_from(NotificationInbox)
            .where(
                NotificationInbox.sent_at.is_(None),
                NotificationInbox.attempts >= max_attempts,
            )
        )
        return await self._session.scalar(statement) or 0
