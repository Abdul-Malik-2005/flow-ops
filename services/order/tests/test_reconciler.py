"""Unit tests for the reconciler: every Inventory answer, no database.

The repository is faked so each scenario stays one short test; the SQL itself is
proved by `tests/integration/test_order_repository.py`, and the end-to-end races
by the integration tests of the worker. What matters here is the decision table:
which answer writes which status, and when nothing at all may be written.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from app.clients.inventory import ReservationOutcome, ReservationResult
from app.domain.order import OrderLine, OrderReason
from app.models.order import Order, OrderStatus
from app.models.order_item import OrderItem
from app.repositories.order import OrderRecord
from app.services.reconciler import Reconciler, ReconcileReport

PRODUCT = uuid.UUID("11111111-1111-4111-8111-111111111111")
WAREHOUSE = uuid.UUID("33333333-3333-4333-8333-333333333333")
RESERVATION_ID = uuid.UUID("cccccccc-3333-4333-8333-cccccccccccc")

BATCH_SIZE = 5
STALE_AFTER = 30.0
MAX_ATTEMPTS = 3
ALERT_AFTER = 900.0


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeSession:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


class _SessionContext:
    """What `async with session_factory()` returns."""

    def __init__(self, session: FakeSession) -> None:
        self._session = session

    async def __aenter__(self) -> FakeSession:
        return self._session

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class FakeSessionFactory:
    def __init__(self) -> None:
        self.sessions: list[FakeSession] = []

    def __call__(self) -> _SessionContext:
        session = FakeSession()
        self.sessions.append(session)
        return _SessionContext(session)


class FakeRepository:
    """Only the three methods the reconciler touches, with observable effects."""

    def __init__(self, records: Sequence[OrderRecord] = ()) -> None:
        self.records = {record.order.id: record for record in records}
        self.queue = list(records)
        self.claims: list[tuple[int, float]] = []
        self.transitions: list[dict[str, object]] = []
        self.backlog_seconds: list[float] = []
        self.stale_backlog = 0
        #: Orders whose conditional UPDATE must lose, simulating a cancel.
        self.cancelled: set[uuid.UUID] = set()

    async def claim_stale_pending(
        self,
        *,
        limit: int,
        stale_after_seconds: float,
    ) -> list[OrderRecord]:
        self.claims.append((limit, stale_after_seconds))
        batch, self.queue = self.queue[:limit], self.queue[limit:]
        for record in batch:
            # Mirrors the real claim: the attempt is spent before the HTTP call.
            record.order.reservation_attempts += 1
        return batch

    async def count_pending_older_than(self, *, seconds: float) -> int:
        self.backlog_seconds.append(seconds)
        return self.stale_backlog

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
        self.transitions.append(
            {
                "order_id": order_id,
                "from_status": from_status,
                "to_status": to_status,
                "reason": reason,
                "reservation_id": inventory_reservation_id,
                "clear_lease": clear_lease,
            }
        )
        if order_id in self.cancelled:
            return None

        record = self.records[order_id]
        record.order.status = to_status
        record.order.inventory_reservation_id = inventory_reservation_id
        if clear_lease:
            record.order.reservation_started_at = None
        return record


class FakeInventory:
    def __init__(
        self,
        result: ReservationResult | None = None,
        *,
        fail_for: set[uuid.UUID] | None = None,
    ) -> None:
        """`fail_for` makes `reserve` raise: the worker must survive that."""
        self.result = result or ReservationResult(
            ReservationOutcome.RESERVED,
            reservation_id=RESERVATION_ID,
        )
        self.fail_for = fail_for or set()
        self.calls: list[tuple[uuid.UUID, tuple[OrderLine, ...]]] = []

    async def reserve(
        self,
        *,
        order_id: uuid.UUID,
        lines: Sequence[OrderLine],
    ) -> ReservationResult:
        if order_id in self.fail_for:
            raise RuntimeError("inventory client exploded")
        self.calls.append((order_id, tuple(lines)))
        return self.result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_record(*, attempts: int = 0, quantity: int = 2) -> OrderRecord:
    order = Order(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        status=OrderStatus.PENDING,
        currency="RUB",
        total_amount=None,
    )
    order.reservation_attempts = attempts
    order.reservation_started_at = datetime.now(UTC)
    item = OrderItem(
        order_id=order.id,
        product_id=PRODUCT,
        warehouse_id=WAREHOUSE,
        quantity=quantity,
    )
    return OrderRecord(order=order, items=[item])


def build_reconciler(
    records: Sequence[OrderRecord],
    inventory: FakeInventory,
    *,
    repository: FakeRepository | None = None,
) -> tuple[Reconciler, FakeRepository, FakeSessionFactory]:
    sessions = FakeSessionFactory()
    repository = repository or FakeRepository(records)
    reconciler = Reconciler(
        session_factory=sessions,
        inventory=inventory,
        repository_factory=lambda _session: repository,
        batch_size=BATCH_SIZE,
        stale_after_seconds=STALE_AFTER,
        max_attempts=MAX_ATTEMPTS,
        alert_after_seconds=ALERT_AFTER,
    )
    return reconciler, repository, sessions


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------


async def test_reserved_answer_stores_the_reservation_and_closes_the_lease() -> None:
    record = make_record()
    reconciler, repository, sessions = build_reconciler([record], FakeInventory())

    report = await reconciler.process_batch()

    assert report == ReconcileReport(claimed=1, reserved=1)
    # The claim uses the configured batch and the *shared* lease window.
    assert repository.claims == [(BATCH_SIZE, STALE_AFTER)]
    assert repository.transitions == [
        {
            "order_id": record.order.id,
            "from_status": OrderStatus.PENDING,
            "to_status": OrderStatus.RESERVED,
            "reason": None,
            "reservation_id": RESERVATION_ID,
            "clear_lease": True,
        }
    ]
    assert record.order.status is OrderStatus.RESERVED
    assert record.order.reservation_started_at is None
    # Claim TX committed, then TX2 committed (the backlog read is read-only).
    assert [session.commits for session in sessions.sessions[:2]] == [1, 1]


async def test_the_batch_sends_the_stored_positions_to_inventory() -> None:
    record = make_record(quantity=4)
    inventory = FakeInventory()
    reconciler, _, _ = build_reconciler([record], inventory)

    await reconciler.process_batch()

    assert inventory.calls == [(record.order.id, (OrderLine(PRODUCT, WAREHOUSE, 4),))]


@pytest.mark.parametrize(
    ("outcome", "reason"),
    [
        (ReservationOutcome.REJECTED, OrderReason.INSUFFICIENT_STOCK),
        (ReservationOutcome.ITEM_NOT_FOUND, OrderReason.INVENTORY_ITEM_NOT_FOUND),
        (ReservationOutcome.INVALID_REQUEST, OrderReason.INVENTORY_REJECTED),
    ],
)
async def test_terminal_rejection_fails_the_order_with_the_mapped_reason(
    outcome: ReservationOutcome,
    reason: OrderReason,
) -> None:
    record = make_record()
    reconciler, repository, _ = build_reconciler(
        [record],
        FakeInventory(ReservationResult(outcome)),
    )

    report = await reconciler.process_batch()

    assert report == ReconcileReport(claimed=1, failed=1)
    assert repository.transitions[0]["to_status"] is OrderStatus.FAILED
    assert repository.transitions[0]["reason"] is reason
    assert record.order.status is OrderStatus.FAILED


async def test_unknown_answer_writes_nothing_and_keeps_the_order_pending() -> None:
    record = make_record()
    reconciler, repository, sessions = build_reconciler(
        [record],
        FakeInventory(ReservationResult(ReservationOutcome.UNAVAILABLE)),
    )

    report = await reconciler.process_batch()

    assert report == ReconcileReport(claimed=1, retried=1)
    assert repository.transitions == []
    assert record.order.status is OrderStatus.PENDING
    # The attempt is still spent: the claim already happened, and only the
    # claim transaction was committed — nothing was written for this order.
    assert record.order.reservation_attempts == 1
    assert [session.commits for session in sessions.sessions] == [1, 0]


# ---------------------------------------------------------------------------
# Attempt budget
# ---------------------------------------------------------------------------


async def test_the_last_allowed_attempt_still_calls_inventory() -> None:
    record = make_record(attempts=MAX_ATTEMPTS - 1)
    inventory = FakeInventory()
    reconciler, _, _ = build_reconciler([record], inventory)

    report = await reconciler.process_batch()

    assert record.order.reservation_attempts == MAX_ATTEMPTS
    assert len(inventory.calls) == 1
    assert report.reserved == 1


async def test_exhausted_budget_fails_the_order_without_calling_inventory() -> None:
    record = make_record(attempts=MAX_ATTEMPTS)
    inventory = FakeInventory()
    reconciler, repository, _ = build_reconciler([record], inventory)

    report = await reconciler.process_batch()

    assert inventory.calls == []
    assert report == ReconcileReport(claimed=1, failed=1)
    assert repository.transitions[0]["reason"] is OrderReason.RESERVATION_UNAVAILABLE
    assert repository.transitions[0]["to_status"] is OrderStatus.FAILED
    assert record.order.status is OrderStatus.FAILED


# ---------------------------------------------------------------------------
# Races and resilience
# ---------------------------------------------------------------------------


async def test_a_cancel_that_wins_the_race_is_not_resurrected() -> None:
    record = make_record()
    repository = FakeRepository([record])
    repository.cancelled.add(record.order.id)
    reconciler, _, sessions = build_reconciler(
        [record],
        FakeInventory(),
        repository=repository,
    )

    report = await reconciler.process_batch()

    assert report == ReconcileReport(claimed=1, superseded=1)
    assert record.order.status is OrderStatus.PENDING
    # TX2 rolled back instead of committing a resurrected order.
    assert sum(session.rollbacks for session in sessions.sessions) == 1


async def test_one_broken_order_does_not_abort_the_batch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    first = make_record()
    second = make_record()
    inventory = FakeInventory(fail_for={first.order.id})
    reconciler, _, _ = build_reconciler([first, second], inventory)

    report = await reconciler.process_batch()

    assert report.claimed == 2
    assert report.retried == 1
    assert report.reserved == 1
    assert second.order.status is OrderStatus.RESERVED
    assert first.order.status is OrderStatus.PENDING
    assert str(first.order.id) in caplog.text


async def test_stale_backlog_is_counted_once_per_pass() -> None:
    repository = FakeRepository()
    repository.stale_backlog = 3
    reconciler, _, _ = build_reconciler([], FakeInventory(), repository=repository)

    report = await reconciler.process_batch()

    assert report == ReconcileReport(stale_backlog=3)
    assert repository.backlog_seconds == [ALERT_AFTER]
