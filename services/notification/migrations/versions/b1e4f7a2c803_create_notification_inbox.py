"""create notification inbox table

Revision ID: b1e4f7a2c803
Revises:
Create Date: 2026-11-20 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "b1e4f7a2c803"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the transactional inbox of delivered Order events.

    `event_id` is the primary key because it is the AMQP `message_id` and
    Order's `outbox_events.id`: the consumer inserts with
    `ON CONFLICT (event_id) DO NOTHING`, so a repeated delivery of the same
    at-least-once message is a no-op instead of a duplicate notification.

    `event_type` is a `varchar` on purpose: a new event type is a contract/code
    change, not an `ALTER TYPE` migration.

    There is no foreign key to Order's tables: they live in another database
    this service cannot read, and everything a notification needs travels in the
    `payload` snapshot.
    """
    op.create_table(
        "notification_inbox",
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
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_notification_inbox")),
    )
    op.create_index(
        "ix_notification_inbox_aggregate_id_occurred_at",
        "notification_inbox",
        ["aggregate_id", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    """Drop the inbox.

    The inbox is derived from the event stream: it can be rebuilt by replaying
    the outbox (at-least-once), so a rollback removes no fact that exists
    nowhere else.
    """
    op.drop_index(
        "ix_notification_inbox_aggregate_id_occurred_at",
        table_name="notification_inbox",
    )
    op.drop_table("notification_inbox")
