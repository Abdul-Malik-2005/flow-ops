"""add reservation attempt counter to orders

Revision ID: d3f8a1b6c204
Revises: c7d2e5f13a08
Create Date: 2026-11-18 18:12:44.918273

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d3f8a1b6c204"
down_revision: str | Sequence[str] | None = "c7d2e5f13a08"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the reconciler's attempt budget.

    `server_default` backfills existing rows in the same statement (a metadata
    only change on PostgreSQL 11+), so the column is `NOT NULL` without a
    separate `UPDATE`. The default stays in the schema because `0` is the only
    honest value for "never claimed" and the row is not always inserted by the
    API path.

    No index: the counter is only read through the primary key by the claim
    statement, which already drives off `ix_orders_pending_created_at`.
    """
    op.add_column(
        "orders",
        sa.Column(
            "reservation_attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )


def downgrade() -> None:
    """Drop the counter: it is derived bookkeeping, not business data."""
    op.drop_column("orders", "reservation_attempts")