"""Persistence for the consumer-side journal: a port plus its SQLAlchemy adapter.

The journal answers one question: "has this event already been processed?" A
message can arrive twice (at-least-once), so the only safe write is an insert
that tolerates its own duplicate::

    INSERT INTO order_event_projections (...) VALUES (...)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id

`RETURNING` is what makes the outcome observable: a row comes back exactly when
this call inserted it, `None` when somebody else already did. A `SELECT` before
the `INSERT` would be a race between two consumers; an insert plus a caught
`IntegrityError` would poison the caller's transaction for every later statement.

Like `SqlOutboxRepository`, the adapter never commits. The consumer owns the
transaction and must commit **before** acknowledging the message: an ack after a
rolled-back insert would lose the event for good.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.order_event_projection import OrderEventProjection


class ProjectionRepositoryPort(Protocol):
    """What the consumer needs from storage: one idempotent write."""

    async def record(
        self,
        *,
        event_id: uuid.UUID,
        aggregate_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        occurred_at: datetime | None,
    ) -> bool:
        """Journal one event; `False` means it was already there.

        The caller commits. `True` is the only case in which a new row
        appeared, so the consumer can log a duplicate without a second query.
        """
        ...


class SqlProjectionRepository:
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
            pg_insert(OrderEventProjection)
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
                index_elements=[OrderEventProjection.event_id],
            )
            .returning(OrderEventProjection.event_id)
        )

        recorded = await self._session.scalar(statement)
        return recorded is not None
