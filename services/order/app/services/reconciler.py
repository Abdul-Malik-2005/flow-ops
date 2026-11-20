"""Reconciler: finish a reservation step whose answer never arrived.

The saga commits a `pending` order with a lease, calls Inventory and applies the
answer in a second transaction. A crash, a timeout or a 5xx in between leaves an
order nobody is working on: Inventory may hold the stock, the database does not
know. Inventory's create is idempotent by `order_id`, so the cure is to ask
again — that is this worker's entire job.

One pass of the loop:

1. **Claim** the oldest abandoned `pending` orders in a single
   `UPDATE ... FOR UPDATE SKIP LOCKED`: the lease moves to `now()` and
   `reservation_attempts` grows by one. Commit. The transaction is deliberately
   short, so no row lock is ever held across a network call.
2. **Ask** Inventory once per claimed order, outside any transaction.
3. **Apply** the answer in its own transaction per order, through the same
   mapping the saga uses (`app.services.reservation`), so a retry can never end
   in a state the original attempt would not have produced.

Consequences worth knowing (docs/architecture.md §5):

* a worker that dies right after the claim spends one attempt and the order waits
  for the lease to expire again — self-healing, at the price of one window;
* `attempts > max_attempts` is terminal: the order fails with
  `reservation_unavailable`, because an order nobody will retry must stop
  looking fresh. The claim happens before the HTTP call, so the cap is a hard
  ceiling on real calls too;
* the cap can hide a reservation Inventory really created (there is no release
  endpoint), so the give-up is logged with the order id for manual cleanup.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.inventory import InventoryClientPort, ReservationResult
from app.domain.order import OrderLine
from app.repositories.order import (
    OrderRecord,
    OrderRepositoryPort,
    SqlOrderRepository,
)
from app.repositories.outbox import OutboxRepositoryPort, SqlOutboxRepository
from app.services.reservation import apply_terminal_outcome, fail_exhausted_reservation

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ReconcileReport:
    """What one pass did — the worker logs it and `--once` prints it."""

    claimed: int = 0
    #: `pending -> reserved` with Inventory's reservation id stored.
    reserved: int = 0
    #: Terminal, either a rejection on retry or an exhausted attempt budget.
    failed: int = 0
    #: Inventory still did not answer: the order stays `pending` on purpose.
    retried: int = 0
    #: The order was no longer `pending` (a cancel won) — left untouched.
    superseded: int = 0
    #: `pending` orders older than the alert threshold, regardless of claims.
    stale_backlog: int = 0


class _Step(StrEnum):
    """How one order ended; only used to fill `ReconcileReport`."""

    RESERVED = "reserved"
    FAILED = "failed"
    RETRIED = "retried"
    SUPERSEDED = "superseded"


class Reconciler:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        inventory: InventoryClientPort,
        batch_size: int,
        stale_after_seconds: float,
        max_attempts: int,
        alert_after_seconds: float,
        repository_factory: Callable[
            [AsyncSession], OrderRepositoryPort
        ] = SqlOrderRepository,
        outbox_factory: Callable[
            [AsyncSession], OutboxRepositoryPort
        ] = SqlOutboxRepository,
    ) -> None:
        self._session_factory = session_factory
        self._inventory = inventory
        self._repository_factory = repository_factory
        #: Same session as the order repository in every TX2, for the same
        #: reason the API does it: the event must commit with the transition.
        self._outbox_factory = outbox_factory
        self._batch_size = batch_size
        # Must match the API's value: both processes decide "is this lease
        # stale, i.e. is no call in flight" with the same number, otherwise one
        # of them acts while the other still considers the row busy.
        self._stale_after_seconds = stale_after_seconds
        self._max_attempts = max_attempts
        self._alert_after_seconds = alert_after_seconds

    async def process_batch(self) -> ReconcileReport:
        """Claim and settle one batch. A single bad order cannot abort it."""
        claimed = await self._claim()

        steps = dict.fromkeys(_Step, 0)
        for record in claimed:
            try:
                steps[await self._reconcile(record)] += 1
            except Exception:
                # A worker loop must not die because of one row: the claim is
                # already durable, so the order keeps its lease and comes back
                # on a later pass. Swallowing the exception is safe *because* of
                # that, but it must never be silent.
                logger.exception(
                    "order %s could not be reconciled; it stays pending",
                    record.order.id,
                )
                steps[_Step.RETRIED] += 1

        backlog = await self._stale_backlog()
        if backlog:
            logger.error(
                "%s order(s) have been pending for more than %.0f s; "
                "inventory is probably unavailable",
                backlog,
                self._alert_after_seconds,
            )

        return ReconcileReport(
            claimed=len(claimed),
            reserved=steps[_Step.RESERVED],
            failed=steps[_Step.FAILED],
            retried=steps[_Step.RETRIED],
            superseded=steps[_Step.SUPERSEDED],
            stale_backlog=backlog,
        )

    # ------------------------------------------------------------------
    # Claim (TX1)
    # ------------------------------------------------------------------

    async def _claim(self) -> list[OrderRecord]:
        async with self._session_factory() as session:
            records = await self._repository(session).claim_stale_pending(
                limit=self._batch_size,
                stale_after_seconds=self._stale_after_seconds,
            )
            # Commit even for an empty batch: it closes the transaction the claim
            # statement opened, so the connection goes back to the pool clean.
            await session.commit()

        if records:
            logger.info("claimed %s pending order(s) for reconciliation", len(records))
        return records

    # ------------------------------------------------------------------
    # Per-order work (HTTP outside a transaction, then TX2)
    # ------------------------------------------------------------------

    async def _reconcile(self, record: OrderRecord) -> _Step:
        order = record.order

        if order.reservation_attempts > self._max_attempts:
            logger.error(
                "order %s exhausted its budget of %s reservation attempts: failing it",
                order.id,
                self._max_attempts,
            )
            return await self._fail_exhausted(order.id)

        lines = tuple(
            OrderLine(
                product_id=item.product_id,
                warehouse_id=item.warehouse_id,
                quantity=item.quantity,
            )
            for item in record.items
        )
        reservation = await self._inventory.reserve(order_id=order.id, lines=lines)

        if reservation.is_retryable:
            # Nothing to write: `pending` plus a fresh lease already says "we do
            # not know". The lease expiring again *is* the backoff, so no
            # `next_attempt_at` column is needed.
            logger.warning(
                "order %s stays pending: inventory answer is still unknown "
                "(attempt %s/%s)",
                order.id,
                order.reservation_attempts,
                self._max_attempts,
            )
            return _Step.RETRIED

        if not await self._apply(order.id, reservation):
            if reservation.is_reserved:
                logger.error(
                    "order %s is no longer pending but reservation %s succeeded; "
                    "stock stays blocked until Inventory exposes a release",
                    order.id,
                    reservation.reservation_id,
                )
            else:
                logger.error(
                    "order %s left pending while its retry failed with %s",
                    order.id,
                    reservation.outcome,
                )
            return _Step.SUPERSEDED

        if reservation.is_reserved:
            logger.info("order %s reserved by the reconciler", order.id)
            return _Step.RESERVED

        logger.warning(
            "order %s failed: inventory answered %s on retry",
            order.id,
            reservation.outcome,
        )
        return _Step.FAILED

    async def _apply(
        self,
        order_id: uuid.UUID,
        reservation: ReservationResult,
    ) -> bool:
        """TX2 for one order. `False` means the row is no longer `pending`."""
        async with self._session_factory() as session:
            record = await apply_terminal_outcome(
                repository=self._repository(session),
                outbox=self._outbox(session),
                order_id=order_id,
                reservation=reservation,
            )
            if record is None:
                await session.rollback()
                return False
            await session.commit()
            return True

    async def _fail_exhausted(self, order_id: uuid.UUID) -> _Step:
        async with self._session_factory() as session:
            record = await fail_exhausted_reservation(
                repository=self._repository(session),
                outbox=self._outbox(session),
                order_id=order_id,
            )
            if record is None:
                # A cancel got there first: nothing to fix and no stock to
                # worry about, this attempt never reached Inventory.
                await session.rollback()
                logger.warning(
                    "order %s left pending before its attempt budget ran out",
                    order_id,
                )
                return _Step.SUPERSEDED
            await session.commit()
        return _Step.FAILED

    # ------------------------------------------------------------------
    # Alerting
    # ------------------------------------------------------------------

    async def _stale_backlog(self) -> int:
        """How many `pending` orders nobody moved for too long (log only)."""
        async with self._session_factory() as session:
            return await self._repository(session).count_pending_older_than(
                seconds=self._alert_after_seconds,
            )

    def _repository(self, session: AsyncSession) -> OrderRepositoryPort:
        return self._repository_factory(session)

    def _outbox(self, session: AsyncSession) -> OutboxRepositoryPort:
        return self._outbox_factory(session)
