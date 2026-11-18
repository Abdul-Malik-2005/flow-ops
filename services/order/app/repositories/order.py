"""Persistence for the order aggregate: a port plus its SQLAlchemy adapter.

Two rules are baked into this module:

* **No commits.** The saga needs two transaction boundaries with an HTTP call in
  between, so the unit of work belongs to the service; a repository that commits
  on its own would make the saga untestable and hide double-commit bugs.
* **Every state change is conditional.** `UPDATE ... WHERE status = <expected>`
  is what makes concurrent cancel/reserve converge instead of overwriting each
  other. Reading first and writing later would be a lost-update bug.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from sqlalchemy import (
    DateTime,
    Float,
    bindparam,
    func,
    literal,
    or_,
    select,
    text,
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.order import OrderReason
from app.models.order import Order, OrderStatus
from app.models.order_item import OrderItem
from app.models.order_status_history import OrderStatusHistory


@dataclass(frozen=True, slots=True)
class OrderCursor:
    """Keyset position: `(created_at, id)`, matching the DESC feed order."""

    created_at: datetime
    order_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class OrderRecord:
    """An order together with its positions, loaded explicitly.

    No ORM relationship is involved: in async SQLAlchemy a lazy load raises
    `MissingGreenlet`, so children are always fetched on purpose.
    """

    order: Order
    items: Sequence[OrderItem]


class CancelStatus(StrEnum):
    CANCELLED = "cancelled"
    #: The order exists and belongs to the caller, but is not `pending` or the
    #: reservation lease is still fresh.
    NOT_CANCELLABLE = "not_cancellable"
    #: Unknown id *or* someone else's order — indistinguishable on purpose.
    NOT_FOUND = "not_found"


@dataclass(frozen=True, slots=True)
class CancelResult:
    status: CancelStatus
    record: OrderRecord | None = None


class OrderRepositoryPort(Protocol):
    """Everything the saga needs from storage, in one narrow interface."""

    async def insert(
        self,
        *,
        order: Order,
        items: Sequence[OrderItem],
        history: OrderStatusHistory,
    ) -> bool:
        """Insert the order, its positions and the first history row.

        Returns `False` when the idempotency unique index rejected the insert,
        without aborting the surrounding transaction.
        """
        ...

    async def get_by_idempotency_key(
        self,
        *,
        user_id: uuid.UUID,
        idempotency_key: str,
    ) -> OrderRecord | None: ...

    async def get(
        self,
        *,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
    ) -> OrderRecord | None: ...

    async def list_for_user(
        self,
        *,
        user_id: uuid.UUID,
        limit: int,
        before: OrderCursor | None = None,
    ) -> list[OrderRecord]: ...

    async def transition(
        self,
        *,
        order_id: uuid.UUID,
        from_status: OrderStatus,
        to_status: OrderStatus,
        reason: OrderReason | None = None,
        inventory_reservation_id: uuid.UUID | None = None,
        clear_lease: bool = False,
    ) -> OrderRecord | None:
        """Conditionally move the order and append the audit row.

        `None` means the order was no longer in `from_status` — the caller must
        not resurrect it, only log and reconcile.
        """
        ...

    async def cancel(
        self,
        *,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
        stale_after_seconds: float,
        reason: OrderReason,
    ) -> CancelResult: ...


# `make_interval(secs => ...)` keeps the comparison on the database clock. Using
# `datetime.now()` in Python would compare two clocks and could let a cancel slip
# in while the reserve call is still running.
_STALE_CUTOFF_SQL = "now() - make_interval(secs => :stale_after_seconds)"


class SqlOrderRepository:
    """SQLAlchemy adapter. It never commits: the service owns transactions."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(
        self,
        *,
        order: Order,
        items: Sequence[OrderItem],
        history: OrderStatusHistory,
    ) -> bool:
        statement = (
            pg_insert(Order)
            .values(
                id=order.id,
                user_id=order.user_id,
                # A new order is always born `pending`: it is the state that
                # means "the reservation outcome is not known yet".
                status=OrderStatus.PENDING,
                currency=order.currency,
                total_amount=order.total_amount,
                idempotency_key=order.idempotency_key,
                request_hash=order.request_hash,
                # Lease opened in the same transaction that will be committed
                # right before the Inventory call.
                reservation_started_at=func.now(),
            )
            # `DO NOTHING` instead of a try/except around IntegrityError: a
            # failed statement would poison the whole transaction, and the
            # replay path has to keep reading in the same session.
            .on_conflict_do_nothing(
                index_elements=[Order.user_id, Order.idempotency_key],
                index_where=text("idempotency_key IS NOT NULL"),
            )
            .returning(
                Order.created_at,
                Order.updated_at,
                Order.reservation_started_at,
            )
        )

        row = (await self._session.execute(statement)).one_or_none()
        if row is None:
            return False

        # Server-generated values are copied onto the instance handed back to the
        # caller, so the response never shows `None` timestamps.
        order.created_at = row.created_at
        order.updated_at = row.updated_at
        order.reservation_started_at = row.reservation_started_at

        self._session.add_all([*items, history])
        return True

    async def get_by_idempotency_key(
        self,
        *,
        user_id: uuid.UUID,
        idempotency_key: str,
    ) -> OrderRecord | None:
        order = await self._session.scalar(
            select(Order).where(
                Order.user_id == user_id,
                Order.idempotency_key == idempotency_key,
            )
        )
        if order is None:
            return None
        return OrderRecord(order=order, items=await self._items(order.id))

    async def get(
        self,
        *,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
    ) -> OrderRecord | None:
        # Ownership is part of the query, not a post-fetch comparison: a foreign
        # order must be indistinguishable from a missing one.
        order = await self._session.scalar(
            select(Order).where(Order.id == order_id, Order.user_id == user_id)
        )
        if order is None:
            return None
        return OrderRecord(order=order, items=await self._items(order.id))

    async def list_for_user(
        self,
        *,
        user_id: uuid.UUID,
        limit: int,
        before: OrderCursor | None = None,
    ) -> list[OrderRecord]:
        statement = (
            select(Order)
            .where(Order.user_id == user_id)
            .order_by(Order.created_at.desc(), Order.id.desc())
            .limit(limit)
        )

        if before is not None:
            # Row comparison `(created_at, id) < (cursor)` matches the DESC
            # ordering exactly: equal timestamps cannot skip or repeat a row.
            statement = statement.where(
                tuple_(Order.created_at, Order.id)
                < tuple_(
                    literal(before.created_at, DateTime(timezone=True)),
                    literal(before.order_id, PGUUID(as_uuid=True)),
                )
            )

        orders = list((await self._session.scalars(statement)).all())
        if not orders:
            return []

        grouped = await self._items_by_order([order.id for order in orders])
        return [
            OrderRecord(order=order, items=grouped.get(order.id, []))
            for order in orders
        ]

    async def transition(
        self,
        *,
        order_id: uuid.UUID,
        from_status: OrderStatus,
        to_status: OrderStatus,
        reason: OrderReason | None = None,
        inventory_reservation_id: uuid.UUID | None = None,
        clear_lease: bool = False,
    ) -> OrderRecord | None:
        values: dict[str, object] = {
            "status": to_status,
            "updated_at": func.now(),
        }
        if inventory_reservation_id is not None:
            values["inventory_reservation_id"] = inventory_reservation_id
        if clear_lease:
            values["reservation_started_at"] = None

        statement = (
            update(Order)
            .where(Order.id == order_id, Order.status == from_status)
            .values(**values)
            .returning(Order)
        )

        # Compare-and-set: `WHERE status = from_status` is the whole concurrency
        # story. If a cancel won the race, zero rows come back and the saga must
        # not push the order forward again.
        order = (await self._session.execute(statement)).scalars().one_or_none()
        if order is None:
            return None

        self._session.add(
            OrderStatusHistory(
                order_id=order_id,
                from_status=from_status,
                to_status=to_status,
                reason=None if reason is None else reason.value,
            )
        )
        return OrderRecord(order=order, items=await self._items(order_id))

    async def cancel(
        self,
        *,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
        stale_after_seconds: float,
        reason: OrderReason,
    ) -> CancelResult:
        stale_cutoff = text(_STALE_CUTOFF_SQL).bindparams(
            bindparam("stale_after_seconds", value=stale_after_seconds, type_=Float)
        )

        statement = (
            update(Order)
            .where(
                Order.id == order_id,
                Order.user_id == user_id,
                Order.status == OrderStatus.PENDING,
                # A fresh lease means the reserve call may still be in flight.
                # Inventory cannot release stock, so cancelling now would strand
                # the reservation forever.
                or_(
                    Order.reservation_started_at.is_(None),
                    Order.reservation_started_at < stale_cutoff,
                ),
            )
            .values(
                status=OrderStatus.CANCELLED,
                updated_at=func.now(),
                reservation_started_at=None,
            )
            .returning(Order)
        )

        order = (await self._session.execute(statement)).scalars().one_or_none()

        if order is None:
            # Distinguish "not yours / does not exist" from "cannot be
            # cancelled": the API answers 404 and 409 respectively.
            current = await self._session.scalar(
                select(Order.status).where(
                    Order.id == order_id,
                    Order.user_id == user_id,
                )
            )
            if current is None:
                return CancelResult(CancelStatus.NOT_FOUND)
            return CancelResult(CancelStatus.NOT_CANCELLABLE)

        self._session.add(
            OrderStatusHistory(
                order_id=order_id,
                from_status=OrderStatus.PENDING,
                to_status=OrderStatus.CANCELLED,
                reason=reason.value,
            )
        )
        return CancelResult(
            CancelStatus.CANCELLED,
            OrderRecord(order=order, items=await self._items(order_id)),
        )

    async def _items(self, order_id: uuid.UUID) -> list[OrderItem]:
        grouped = await self._items_by_order([order_id])
        return grouped.get(order_id, [])

    async def _items_by_order(
        self,
        order_ids: Sequence[uuid.UUID],
    ) -> dict[uuid.UUID, list[OrderItem]]:
        """Load every position of several orders in one query.

        Grouping in Python avoids the N+1 that per-order loading would cause on
        the list endpoint.
        """
        rows = await self._session.scalars(
            select(OrderItem)
            .where(OrderItem.order_id.in_(order_ids))
            .order_by(OrderItem.product_id, OrderItem.warehouse_id)
        )

        grouped: dict[uuid.UUID, list[OrderItem]] = {}
        for item in rows:
            grouped.setdefault(item.order_id, []).append(item)
        return grouped
