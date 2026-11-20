"""Persistence for the transactional outbox: a port plus its SQLAlchemy adapter.

The repository follows the same two rules as `app.repositories.order`:

* **No commits.** The outbox row must be written in the transaction that records
  the fact it describes, so the transaction boundary belongs to the caller. The
  relay, in turn, needs its own short claim transaction and a separate mark, so
  it commits on its own — outside this adapter.
* **Every state change is conditional.** Claim and mark are `UPDATE … WHERE
  <expected state>` statements: two relay processes must converge instead of
  publishing the same row twice, and a row already marked cannot be marked again
  with a fresh timestamp.

The claim mirrors the reconciler (`app.repositories.order.claim_stale_pending`):
one `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)` moves the lease and
spends an attempt, the caller commits, and only then publishes. A network call
never happens while a row lock is held. The lease doubles as the backoff: a
failed publish writes nothing, so the row simply becomes claimable again when
the lease expires.

Ordering note: `occurred_at` is the transaction timestamp (`now()`), so events
written in one transaction share it. That does not break per-order order: a
transition is a compare-and-set, so a single order cannot change status twice in
one transaction, and its later events always come from a later transaction.
"""

from __future__ import annotations

import uuid
from typing import Protocol

from sqlalchemy import Float, bindparam, func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox_event import OutboxEvent

#: Claim predicate on the database clock: a lease that cannot be compared
#: against `now()` on the same clock is not a lease.
_LEASE_CUTOFF_SQL = "now() - make_interval(secs => :lease_seconds)"


class OutboxRepositoryPort(Protocol):
    """Everything the relay and the write path need from storage."""

    async def enqueue(self, *, event: OutboxEvent) -> None:
        """Stage an event in the caller's transaction.

        No commit and no flush: the row becomes visible exactly when the status
        change it describes becomes visible.
        """
        ...

    async def claim_batch(
        self,
        *,
        limit: int,
        lease_seconds: float,
        max_attempts: int,
    ) -> list[OutboxEvent]:
        """Lease up to `limit` unpublished events for one publish attempt.

        Rows that already spent `max_attempts` are skipped on purpose: they are
        the dead-letter set, surfaced by `count_exhausted_unpublished`. Nothing
        is deleted, so such an event is never lost — only removed from the feed
        until an operator looks at it.
        """
        ...

    async def mark_published(self, *, event_id: uuid.UUID) -> bool:
        """Mark a successfully published event.

        Returns `False` when somebody else already marked it (or the row is
        gone), which is how at-least-once delivery stays observable.
        """
        ...

    async def count_exhausted_unpublished(self, *, max_attempts: int) -> int:
        """Events that will never be retried automatically (log only)."""
        ...


class SqlOutboxRepository:
    """SQLAlchemy adapter. It never commits: the caller owns transactions."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def enqueue(self, *, event: OutboxEvent) -> None:
        self._session.add(event)

    async def claim_batch(
        self,
        *,
        limit: int,
        lease_seconds: float,
        max_attempts: int,
    ) -> list[OutboxEvent]:
        lease_cutoff = text(_LEASE_CUTOFF_SQL).bindparams(
            bindparam("lease_seconds", value=lease_seconds, type_=Float)
        )

        candidates = (
            select(OutboxEvent.id)
            .where(
                OutboxEvent.published_at.is_(None),
                # Strict `<`: the claim increments the counter *before* the
                # publish call, so `attempts == max` already means "budget
                # spent". A `<=` here would turn the cap into max + 1 real
                # publish attempts.
                OutboxEvent.attempts < max_attempts,
                # A fresh lease means another relay process is publishing this
                # row right now.
                or_(
                    OutboxEvent.locked_at.is_(None),
                    OutboxEvent.locked_at < lease_cutoff,
                ),
            )
            # Oldest first: the published stream follows the order of facts.
            # `id` makes the order deterministic for equal timestamps.
            .order_by(OutboxEvent.occurred_at, OutboxEvent.id)
            .limit(limit)
            # A second relay busy with the same batch skips the rows being
            # updated instead of blocking behind them.
            .with_for_update(skip_locked=True)
        )

        statement = (
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(candidates))
            .values(
                locked_at=func.now(),
                attempts=OutboxEvent.attempts + 1,
            )
            .returning(OutboxEvent)
            # The ORM cannot evaluate `now() - make_interval(...)` in Python, and
            # `RETURNING` already gives the fresh state of every row.
            .execution_options(synchronize_session=False)
        )

        events = list((await self._session.scalars(statement)).all())
        if not events:
            return []

        # `RETURNING` has no defined order; restore the claim order explicitly.
        events.sort(key=lambda event: (event.occurred_at, event.id))
        return events

    async def mark_published(self, *, event_id: uuid.UUID) -> bool:
        statement = (
            update(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                # Compare-and-set: an event is marked exactly once, even if two
                # relay processes waited out the same expired lease.
                OutboxEvent.published_at.is_(None),
            )
            .values(published_at=func.now())
            .returning(OutboxEvent.id)
        )

        marked = await self._session.scalar(statement)
        return marked is not None

    async def count_exhausted_unpublished(self, *, max_attempts: int) -> int:
        statement = (
            select(func.count())
            .select_from(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                OutboxEvent.attempts >= max_attempts,
            )
        )
        return await self._session.scalar(statement) or 0
