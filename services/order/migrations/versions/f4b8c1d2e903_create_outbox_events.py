"""create outbox events table

Revision ID: f4b8c1d2e903
Revises: d3f8a1b6c204
Create Date: 2026-11-19 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "f4b8c1d2e903"
down_revision: str | Sequence[str] | None = "d3f8a1b6c204"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the transactional outbox.

    `event_type` is a `varchar` on purpose: a new event type must not require an
    `ALTER TYPE` migration (and `ALTER TYPE ... ADD VALUE` cannot run in the same
    transaction as the change that needs it).

    The partial index covers only unpublished rows, so the relay's feed stays
    small while the table itself is append-only. `(occurred_at, id)` matches the
    claim order and makes it deterministic for events created in the same
    transaction.
    """
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("aggregate_id", sa.UUID(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "attempts",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox_events")),
    )
    op.create_index(
        "ix_outbox_events_unpublished",
        "outbox_events",
        ["occurred_at", "id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    """Drop the outbox.

    Unpublished rows disappear with the table: this is a development-time
    rollback, and the events it removes are exactly the ones nobody has consumed
    yet.
    """
    op.drop_index(
        "ix_outbox_events_unpublished",
        table_name="outbox_events",
    )
    op.drop_table("outbox_events")
