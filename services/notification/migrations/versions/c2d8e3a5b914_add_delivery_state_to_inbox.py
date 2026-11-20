"""add delivery state to the notification inbox

Revision ID: c2d8e3a5b914
Revises: b1e4f7a2c803
Create Date: 2026-11-20 14:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c2d8e3a5b914"
down_revision: str | Sequence[str] | None = "b1e4f7a2c803"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the send state a later process needs to deliver each fact once.

    `sent_at IS NULL` means "recorded but not delivered yet"; the sender marks it
    only after the provider returned, which is what keeps delivery at-least-once.
    `locked_at` is the lease of the current send attempt and `attempts` counts
    them (including failed ones), so a dead provider backs off instead of spinning
    and a poison row leaves the feed without being deleted.

    The partial index covers only unsent rows: the feed is the sender's claim
    predicate, and a table of delivered history must not be scanned to find it.
    """
    op.add_column(
        "notification_inbox",
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "notification_inbox",
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "notification_inbox",
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.create_index(
        "ix_notification_inbox_unsent",
        "notification_inbox",
        ["occurred_at", "event_id"],
        unique=False,
        postgresql_where=sa.text("sent_at IS NULL"),
    )


def downgrade() -> None:
    """Remove the send state.

    Safe to roll back: the inbox rows themselves stay, and the columns describe
    only how far delivery got — replaying the outbox rebuilds it.
    """
    op.drop_index(
        "ix_notification_inbox_unsent",
        table_name="notification_inbox",
    )
    op.drop_column("notification_inbox", "attempts")
    op.drop_column("notification_inbox", "sent_at")
    op.drop_column("notification_inbox", "locked_at")
