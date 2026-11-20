"""create order event projections table

Revision ID: a7c9e1f4b2d6
Revises: f4b8c1d2e903
Create Date: 2026-11-20 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "a7c9e1f4b2d6"
down_revision: str | Sequence[str] | None = "f4b8c1d2e903"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the consumer-side journal of delivered order events.

    `event_id` is the primary key because it is the AMQP `message_id` and the
    `outbox_events.id` the message was built from: the consumer inserts with
    `ON CONFLICT (event_id) DO NOTHING`, so a repeated delivery of the same
    at-least-once message is a no-op.

    `event_type` is a `varchar` on purpose: a new event type must not require an
    `ALTER TYPE` migration (same reasoning as `outbox_events`).

    The index covers one order's journal in the order the facts happened; it is
    small because the table is append-only and never scanned by the producer.
    """
    op.create_table(
        "order_event_projections",
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column("aggregate_id", sa.UUID(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_order_event_projections")),
    )
    op.create_index(
        "ix_order_event_projections_aggregate_id_occurred_at",
        "order_event_projections",
        ["aggregate_id", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    """Drop the journal.

    The projection is derived data: it can be rebuilt by replaying the outbox,
    so a rollback removes no fact that exists nowhere else.
    """
    op.drop_index(
        "ix_order_event_projections_aggregate_id_occurred_at",
        table_name="order_event_projections",
    )
    op.drop_table("order_event_projections")
