"""Order application service: the order-first saga against Inventory.

The sequence for a new order is fixed:

1. **TX1** inserts `pending` + positions + the first history row and opens the
   reservation lease, then commits. The row exists before any remote call, so a
   crash never leaves stock reserved for an order nobody can see.
2. The Inventory reservation is attempted over HTTP, *outside* any transaction.
   Holding a transaction open across a network call would pin a connection and a
   row lock for the whole timeout.
3. **TX2** applies the answer with a conditional `UPDATE ... WHERE status =
   'pending'`. If a cancel won the race, zero rows change and the order stays
   cancelled; stock may leak, which is logged, but state never regresses.

An unknown answer (timeout, 5xx) keeps the order `pending` on purpose: Inventory
is idempotent by `order_id`, so a retry is safe and `failed` would be a lie.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.inventory import (
    InventoryClientPort,
    ReservationOutcome,
    ReservationResult,
)
from app.domain.order import (
    OrderLine,
    OrderReason,
    canonical_request_hash,
    normalize_lines,
)
from app.models.order import Order, OrderStatus
from app.models.order_item import OrderItem
from app.models.order_status_history import OrderStatusHistory
from app.repositories.order import (
    CancelStatus,
    OrderCursor,
    OrderRecord,
    OrderRepositoryPort,
)
from app.services.exceptions import (
    IdempotencyConflictError,
    InsufficientStockError,
    InventoryItemNotFoundError,
    InventoryRejectedError,
    OrderNotCancellableError,
    OrderNotFoundError,
    OrderStateChangedError,
)
from app.services.pricing import PricingPort, total_amount
from app.services.reservation import apply_terminal_outcome

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CreateOrderResult:
    """Outcome of `POST /orders` before it is turned into a status code."""

    record: OrderRecord
    #: `False` for an idempotent replay: the API answers 200 instead of 201.
    created: bool
    #: `True` when Inventory's answer is unknown and the order stayed `pending`
    #: (202 Accepted: the request was accepted, the stock is not confirmed yet).
    reservation_pending: bool = False


class OrderService:
    def __init__(
        self,
        *,
        session: AsyncSession,
        repository: OrderRepositoryPort,
        inventory: InventoryClientPort,
        pricing: PricingPort,
        currency: str,
        stale_after_seconds: float,
    ) -> None:
        self._session = session
        self._repository = repository
        self._inventory = inventory
        self._pricing = pricing
        self._currency = currency
        self._stale_after_seconds = stale_after_seconds

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    async def create_order(
        self,
        *,
        user_id: uuid.UUID,
        lines: Sequence[OrderLine],
        idempotency_key: str | None = None,
    ) -> CreateOrderResult:
        normalized = normalize_lines(lines)
        request_hash = canonical_request_hash(normalized, currency=self._currency)

        order, items, history = await self._build_aggregate(
            user_id=user_id,
            lines=normalized,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )

        inserted = await self._repository.insert(
            order=order,
            items=items,
            history=history,
        )

        if not inserted:
            # Without a key the partial unique index cannot conflict, so this
            # branch always has a key to look up.
            await self._session.rollback()
            return await self._replay(
                user_id=user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )

        # TX1 ends here: the `pending` order and its lease are durable before the
        # network call starts.
        await self._session.commit()

        reservation = await self._inventory.reserve(
            order_id=order.id,
            lines=normalized,
        )

        return await self._apply_reservation(
            order_id=order.id,
            user_id=user_id,
            reservation=reservation,
        )

    async def _replay(
        self,
        *,
        user_id: uuid.UUID,
        idempotency_key: str | None,
        request_hash: str,
    ) -> CreateOrderResult:
        """Answer a repeated `Idempotency-Key` with the original order."""
        if idempotency_key is None:  # pragma: no cover - defensive
            raise IdempotencyConflictError("")

        existing = await self._repository.get_by_idempotency_key(
            user_id=user_id,
            idempotency_key=idempotency_key,
        )
        if existing is None:
            # The row vanished between the insert and this read (manual cleanup):
            # a conflict is the honest answer, a silent duplicate is not.
            raise IdempotencyConflictError(idempotency_key)

        if existing.order.request_hash != request_hash:
            raise IdempotencyConflictError(idempotency_key)

        return CreateOrderResult(record=existing, created=False)

    async def _build_aggregate(
        self,
        *,
        user_id: uuid.UUID,
        lines: Sequence[OrderLine],
        idempotency_key: str | None,
        request_hash: str,
    ) -> tuple[Order, list[OrderItem], OrderStatusHistory]:
        priced = await self._pricing.price(lines=lines, currency=self._currency)

        order = Order(
            id=uuid.uuid4(),
            user_id=user_id,
            status=OrderStatus.PENDING,
            currency=self._currency,
            total_amount=total_amount(priced),
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )
        items = [
            OrderItem(
                order_id=order.id,
                product_id=line.line.product_id,
                warehouse_id=line.line.warehouse_id,
                quantity=line.line.quantity,
                unit_price=line.unit_price,
                line_total=line.line_total,
            )
            for line in priced
        ]
        history = OrderStatusHistory(
            order_id=order.id,
            from_status=None,
            to_status=OrderStatus.PENDING,
            reason=None,
        )
        return order, items, history

    # ------------------------------------------------------------------
    # Saga step 3: apply Inventory's answer
    # ------------------------------------------------------------------

    async def _apply_reservation(
        self,
        *,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
        reservation: ReservationResult,
    ) -> CreateOrderResult:
        if reservation.is_retryable:
            # Nothing to write: `pending` plus a fresh lease is the accurate
            # description of "we do not know". The reconciler retries later,
            # which is safe because Inventory is idempotent by `order_id`.
            logger.warning(
                "order %s stays pending: inventory outcome is unknown",
                order_id,
            )
            return CreateOrderResult(
                record=await self._require(order_id=order_id, user_id=user_id),
                created=True,
                reservation_pending=True,
            )

        record = await apply_terminal_outcome(
            repository=self._repository,
            order_id=order_id,
            reservation=reservation,
        )

        if record is None:
            await self._session.rollback()
            if reservation.is_reserved:
                # A cancel won the race after Inventory reserved. The order must
                # not be resurrected, and the stock is orphaned: Inventory has no
                # release endpoint, so this is logged loudly for manual cleanup.
                logger.error(
                    "order %s was no longer pending but reservation %s succeeded; "
                    "stock stays blocked until Inventory exposes a release",
                    order_id,
                    reservation.reservation_id,
                )
                return CreateOrderResult(
                    record=await self._require(order_id=order_id, user_id=user_id),
                    created=True,
                )

            logger.error(
                "order %s left pending while its reservation failed with %s",
                order_id,
                reservation.outcome,
            )
            raise OrderStateChangedError(order_id)

        await self._session.commit()
        if reservation.is_reserved:
            return CreateOrderResult(record=record, created=True)

        raise _FAILURE_ERRORS[reservation.outcome](order_id)

    async def _require(
        self,
        *,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
    ) -> OrderRecord:
        record = await self._repository.get(order_id=order_id, user_id=user_id)
        if record is None:  # pragma: no cover - the row was just written
            raise OrderNotFoundError(order_id)
        return record

    # ------------------------------------------------------------------
    # Read and cancel
    # ------------------------------------------------------------------

    async def get_order(
        self,
        *,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
    ) -> OrderRecord:
        return await self._require(order_id=order_id, user_id=user_id)

    async def list_orders(
        self,
        *,
        user_id: uuid.UUID,
        limit: int,
        before: OrderCursor | None = None,
    ) -> tuple[list[OrderRecord], OrderCursor | None]:
        # One extra row answers "is there a next page?" without a second query or
        # a COUNT(*) over the whole feed.
        records = await self._repository.list_for_user(
            user_id=user_id,
            limit=limit + 1,
            before=before,
        )
        page = records[:limit]
        if len(records) <= limit:
            return page, None

        last = page[-1]
        return page, OrderCursor(
            created_at=last.order.created_at,
            order_id=last.order.id,
        )

    async def cancel_order(
        self,
        *,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
        reason: OrderReason = OrderReason.USER_REQUESTED,
    ) -> OrderRecord:
        result = await self._repository.cancel(
            order_id=order_id,
            user_id=user_id,
            stale_after_seconds=self._stale_after_seconds,
            reason=reason,
        )

        if result.status is CancelStatus.NOT_FOUND:
            await self._session.rollback()
            raise OrderNotFoundError(order_id)

        if result.status is CancelStatus.NOT_CANCELLABLE:
            await self._session.rollback()
            raise OrderNotCancellableError(order_id)

        await self._session.commit()
        if result.record is None:  # pragma: no cover - cancelled implies a record
            raise OrderNotFoundError(order_id)
        return result.record


# Terminal Inventory answers -> the error the API raises. The reason that goes
# into the status history lives next to the shared TX2 mapping in
# `app.services.reservation`, so both callers cannot disagree about it.
_FAILURE_ERRORS: dict[ReservationOutcome, type[InsufficientStockError]] = {
    ReservationOutcome.REJECTED: InsufficientStockError,
    ReservationOutcome.ITEM_NOT_FOUND: InventoryItemNotFoundError,
    ReservationOutcome.INVALID_REQUEST: InventoryRejectedError,
}
