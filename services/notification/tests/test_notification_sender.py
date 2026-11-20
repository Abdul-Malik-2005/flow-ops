"""Unit tests for the sender: the transaction order and the decision table.

The SQL of `claim_batch`/`mark_sent` is proved against a real PostgreSQL in
`tests/integration/test_inbox_repository.py` and `tests/integration/test_sender.py`;
here only the loop is under test, with a fake storage and a fake transport. What
matters is that the batch is committed *before* the first send, that a failed send
is never marked, and that a lost compare-and-set is reported as a duplicate
instead of a success.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

from app.domain.notifications import NotificationMessage
from app.models.notification_inbox import NotificationInbox
from app.services.notification_sender import NotificationSender, SendReport

BATCH_SIZE = 10
LEASE_SECONDS = 30.0
MAX_ATTEMPTS = 3
OCCURRED_AT = datetime(2026, 11, 19, 12, 30, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeSession:
    def __init__(self, journal: list[str] | None = None) -> None:
        self.commits = 0
        self._journal = journal

    async def commit(self) -> None:
        self.commits += 1
        if self._journal is not None:
            self._journal.append("commit")

    async def rollback(self) -> None:
        raise AssertionError("the sender never rolls back explicitly")


class _SessionContext:
    def __init__(self, session: FakeSession) -> None:
        self._session = session

    async def __aenter__(self) -> FakeSession:
        return self._session

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class FakeSessionFactory:
    def __init__(self, journal: list[str] | None = None) -> None:
        self.sessions: list[FakeSession] = []
        self._journal = journal

    def __call__(self) -> _SessionContext:
        session = FakeSession(self._journal)
        self.sessions.append(session)
        return _SessionContext(session)


class FakeInbox:
    """Storage with observable effects, limited to what the sender calls."""

    def __init__(
        self,
        rows: Sequence[NotificationInbox] = (),
        *,
        exhausted: int = 0,
        already_marked: set[uuid.UUID] | None = None,
    ) -> None:
        self.rows = list(rows)
        self.exhausted = exhausted
        self.already_marked = already_marked or set()
        self.claims: list[tuple[int, float, int]] = []
        self.marked: list[uuid.UUID] = []
        self.counted_with: list[int] = []

    async def claim_batch(
        self,
        *,
        limit: int,
        lease_seconds: float,
        max_attempts: int,
    ) -> list[NotificationInbox]:
        self.claims.append((limit, lease_seconds, max_attempts))
        batch, self.rows = self.rows[:limit], self.rows[limit:]
        return batch

    async def mark_sent(self, *, event_id: uuid.UUID) -> bool:
        self.marked.append(event_id)
        return event_id not in self.already_marked

    async def count_exhausted_unsent(self, *, max_attempts: int) -> int:
        self.counted_with.append(max_attempts)
        return self.exhausted


class FakeSender:
    def __init__(
        self,
        *,
        failing: bool = False,
        journal: list[str] | None = None,
    ) -> None:
        self.failing = failing
        self.journal = journal
        self.sent: list[uuid.UUID] = []
        #: The composed message each `send` received.
        self.messages: list[NotificationMessage] = []
        #: Every attempt, including the ones that raised.
        self.attempts: list[uuid.UUID] = []
        self.closed = False

    async def send(
        self,
        *,
        event_id: uuid.UUID,
        message: NotificationMessage,
    ) -> None:
        self.attempts.append(event_id)
        self.messages.append(message)
        if self.journal is not None:
            self.journal.append(f"send:{event_id}")
        if self.failing:
            raise RuntimeError("provider is down")
        self.sent.append(event_id)

    async def aclose(self) -> None:
        self.closed = True


def build_row(index: int, *, attempts: int = 1) -> NotificationInbox:
    order_id = uuid.UUID(int=index)
    return NotificationInbox(
        event_id=uuid.UUID(int=index + 1000),
        aggregate_id=order_id,
        event_type="order.reserved",
        payload={
            "order_id": str(order_id),
            "user_id": str(uuid.UUID(int=index + 2000)),
            "status": "reserved",
        },
        occurred_at=OCCURRED_AT,
        attempts=attempts,
    )


def build_sender(
    *,
    inbox: FakeInbox | None = None,
    sender: FakeSender | None = None,
    journal: list[str] | None = None,
) -> tuple[NotificationSender, FakeInbox, FakeSender, FakeSessionFactory]:
    inbox = inbox or FakeInbox()
    sender = sender or FakeSender(journal=journal)
    factory = FakeSessionFactory(journal)
    instance = NotificationSender(
        session_factory=factory,  # type: ignore[arg-type]
        sender=sender,
        batch_size=BATCH_SIZE,
        lease_seconds=LEASE_SECONDS,
        max_attempts=MAX_ATTEMPTS,
        inbox_factory=lambda _session: inbox,  # type: ignore[arg-type,return-value]
    )
    return instance, inbox, sender, factory


async def test_a_delivered_row_is_sent_and_marked() -> None:
    row = build_row(1)
    sender, inbox, transport, _ = build_sender(inbox=FakeInbox([row]))

    report = await sender.process_batch()

    assert report == SendReport(claimed=1, sent=1)
    assert transport.sent == [row.event_id]
    assert inbox.marked == [row.event_id]
    # The transport received composed text, not the raw order snapshot: the
    # recipient was resolved and the wording was chosen by event type.
    [message] = transport.messages
    assert message.recipient.endswith("@flowops.local")
    assert "confirmed" in message.subject


async def test_the_claim_is_committed_before_the_first_send() -> None:
    journal: list[str] = []
    row = build_row(1)
    sender, _, _, _ = build_sender(
        inbox=FakeInbox([row]),
        sender=FakeSender(journal=journal),
        journal=journal,
    )

    await sender.process_batch()

    # The lease must survive a crash during the provider call: the claim commits
    # first, so a row is never sent while its transaction (and lock) is open.
    assert journal[0] == "commit"
    assert journal[1] == f"send:{row.event_id}"
    assert "commit" in journal[2:]


async def test_an_empty_pass_reports_the_dead_letter_backlog() -> None:
    sender, inbox, transport, _ = build_sender(inbox=FakeInbox(exhausted=2))

    report = await sender.process_batch()

    assert report == SendReport(exhausted=2)
    assert inbox.claims == [(BATCH_SIZE, LEASE_SECONDS, MAX_ATTEMPTS)]
    assert transport.attempts == []


async def test_a_failed_send_is_never_marked() -> None:
    row = build_row(1)
    sender, inbox, transport, _ = build_sender(
        inbox=FakeInbox([row]),
        sender=FakeSender(failing=True),
    )

    report = await sender.process_batch()

    # The attempt is spent, nothing is marked: the lease expiry retries the row.
    assert report == SendReport(claimed=1, failed=1)
    assert transport.attempts == [row.event_id]
    assert inbox.marked == []


async def test_a_lost_compare_and_set_is_reported_as_a_duplicate() -> None:
    row = build_row(1)
    sender, _, transport, _ = build_sender(
        inbox=FakeInbox([row], already_marked={row.event_id}),
    )

    report = await sender.process_batch()

    # Sent twice, marked once: at-least-once, and the sender says so instead of
    # pretending it was the process that completed the row.
    assert report == SendReport(claimed=1, sent=1, duplicates=1)
    assert transport.sent == [row.event_id]


async def test_a_batch_uses_one_transaction_per_phase() -> None:
    sender, _, _, factory = build_sender(inbox=FakeInbox([build_row(1)]))

    await sender.process_batch()

    # claim, mark, count: three short sessions, none held across a send.
    assert len(factory.sessions) == 3
    assert [session.commits for session in factory.sessions] == [1, 1, 0]


async def test_a_batch_without_survivors_skips_the_mark_transaction() -> None:
    sender, _, _, factory = build_sender(
        inbox=FakeInbox([build_row(1)]),
        sender=FakeSender(failing=True),
    )

    await sender.process_batch()

    # Nothing was sent, so there is nothing to mark: the claim and the count only.
    assert len(factory.sessions) == 2


async def test_the_sender_releases_the_transport_it_was_built_with() -> None:
    """Ownership: the adapter went in through the constructor, so it comes out."""
    sender, _, transport, _ = build_sender()

    await sender.aclose()

    assert transport.closed
