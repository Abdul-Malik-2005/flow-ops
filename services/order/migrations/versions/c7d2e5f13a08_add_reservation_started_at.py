"""add reservation lease marker to orders

Revision ID: c7d2e5f13a08
Revises: b1a4c7d92e05
Create Date: 2026-11-18 16:40:12.512304

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c7d2e5f13a08"
down_revision: str | Sequence[str] | None = "b1a4c7d92e05"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the saga lease marker.

    Nullable with no default and no backfill: existing `pending` rows have an
    unknown reservation state, and `NULL` means exactly that ("no call in
    flight"), which keeps cancellation legal for them.

    No extra index: the marker is only read through the primary key during
    cancellation, and the reconciler feed already has
    `ix_orders_pending_created_at`.
    """
    op.add_column(
        "orders",
        sa.Column(
            "reservation_started_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    """Drop the marker: the column holds no data that cannot be rebuilt."""
    op.drop_column("orders", "reservation_started_at")
