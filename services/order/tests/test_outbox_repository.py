"""The outbox adapter must never own the transaction.

Everything else about this repository is SQL semantics and lives in
`tests/integration/test_outbox_repository.py`; here only the contract that a fake
can prove: `enqueue` stages the row and leaves the commit to the caller.
"""

from __future__ import annotations

import uuid

from app.models.outbox_event import OutboxEvent
from app.repositories.outbox import SqlOutboxRepository


class RecordingSession:
    """Duck-typed session: records what the repository did, nothing else."""

    def __init__(self) -> None:
        self.added: list[object] = []
        self.commits = 0
        self.rollbacks = 0

    def add(self, instance: object) -> None:
        self.added.append(instance)

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def build_event() -> OutboxEvent:
    aggregate_id = uuid.uuid4()
    return OutboxEvent(
        id=uuid.uuid4(),
        aggregate_id=aggregate_id,
        event_type="order.reserved",
        payload={"order_id": str(aggregate_id)},
    )


async def test_enqueue_stages_the_event_without_committing() -> None:
    session = RecordingSession()
    event = build_event()

    await SqlOutboxRepository(session).enqueue(event=event)  # type: ignore[arg-type]

    assert session.added == [event]
    # Committing here would publish an event about a fact whose transaction may
    # still roll back — the exact failure this pattern exists to prevent.
    assert session.commits == 0
    assert session.rollbacks == 0
