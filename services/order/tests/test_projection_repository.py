"""The projection adapter must leave commits to the consumer.

SQL semantics (`ON CONFLICT (event_id) DO NOTHING`, `RETURNING`) live in
`tests/integration/test_projection_repository.py`; here only the contract a fake
can prove: `record` reports "inserted / duplicate" and never commits.
"""

from __future__ import annotations

import uuid

from app.repositories.projection import SqlProjectionRepository


class RecordingSession:
    """Duck-typed session: returns a canned `RETURNING` value, counts commits."""

    def __init__(self, returned: uuid.UUID | None) -> None:
        self._returned = returned
        self.statements: list[object] = []
        self.commits = 0

    async def scalar(self, statement: object) -> uuid.UUID | None:
        self.statements.append(statement)
        return self._returned

    async def commit(self) -> None:
        self.commits += 1


async def test_record_reports_a_new_event_without_committing() -> None:
    event_id = uuid.uuid4()
    session = RecordingSession(returned=event_id)

    recorded = await SqlProjectionRepository(session).record(  # type: ignore[arg-type]
        event_id=event_id,
        aggregate_id=uuid.uuid4(),
        event_type="order.reserved",
        payload={"order_id": str(uuid.uuid4())},
        occurred_at=None,
    )

    assert recorded is True
    assert len(session.statements) == 1
    # Committing here would acknowledge the message before the journal row is
    # durable — the one ordering that turns at-least-once into at-most-once.
    assert session.commits == 0


async def test_record_reports_a_duplicate_for_an_empty_returning() -> None:
    session = RecordingSession(returned=None)

    recorded = await SqlProjectionRepository(session).record(  # type: ignore[arg-type]
        event_id=uuid.uuid4(),
        aggregate_id=uuid.uuid4(),
        event_type="order.paid",
        payload={"order_id": str(uuid.uuid4()), "reason": "paid"},
        occurred_at=None,
    )

    assert recorded is False
    assert session.commits == 0
