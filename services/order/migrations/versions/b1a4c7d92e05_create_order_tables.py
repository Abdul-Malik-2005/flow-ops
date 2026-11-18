"""create order tables

Revision ID: b1a4c7d92e05
Revises:
Create Date: 2026-11-18 15:12:41.117904

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "b1a4c7d92e05"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# `create_type=False`: the type is created once explicitly in `upgrade()`, so the
# column definitions must not try to create it again for the second table.
# Keep the value order in sync with `app.models.order.OrderStatus`.
order_status = postgresql.ENUM(
    "pending",
    "reserved",
    "paid",
    "fulfilled",
    "cancelled",
    "failed",
    name="order_status",
    create_type=False,
)


def upgrade() -> None:
    """Upgrade schema."""
    order_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "orders",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column(
            "status",
            order_status,
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("total_amount", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("request_hash", sa.String(length=64), nullable=True),
        sa.Column("inventory_reservation_id", sa.UUID(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "total_amount IS NULL OR total_amount >= 0",
            name=op.f("ck_orders_total_amount_non_negative"),
        ),
        sa.CheckConstraint(
            "currency ~ '^[A-Z]{3}$'",
            name=op.f("ck_orders_currency_iso_code"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_orders")),
    )
    op.create_index(
        op.f("ix_orders_user_id_created_at"),
        "orders",
        ["user_id", sa.text("created_at DESC")],
    )
    op.create_index(
        op.f("ix_orders_pending_created_at"),
        "orders",
        ["created_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        op.f("uq_orders_user_id_idempotency_key"),
        "orders",
        ["user_id", "idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )
    op.create_table(
        "order_items",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("order_id", sa.UUID(), nullable=False),
        sa.Column("product_id", sa.UUID(), nullable=False),
        sa.Column("warehouse_id", sa.UUID(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("unit_price", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("line_total", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.CheckConstraint(
            "quantity > 0",
            name=op.f("ck_order_items_quantity_positive"),
        ),
        sa.CheckConstraint(
            "unit_price IS NULL OR unit_price >= 0",
            name=op.f("ck_order_items_unit_price_non_negative"),
        ),
        sa.CheckConstraint(
            "line_total IS NULL OR line_total >= 0",
            name=op.f("ck_order_items_line_total_non_negative"),
        ),
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.id"],
            name=op.f("fk_order_items_order_id_orders"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_order_items")),
        sa.UniqueConstraint(
            "order_id",
            "product_id",
            "warehouse_id",
            name=op.f("uq_order_items_order_id_product_id_warehouse_id"),
        ),
    )

    op.create_table(
        "order_status_history",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("order_id", sa.UUID(), nullable=False),
        sa.Column("from_status", order_status, nullable=True),
        sa.Column("to_status", order_status, nullable=False),
        sa.Column("reason", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "from_status IS NULL OR from_status <> to_status",
            name=op.f("ck_order_status_history_status_changed"),
        ),
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.id"],
            name=op.f("fk_order_status_history_order_id_orders"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_order_status_history")),
    )
    op.create_index(
        op.f("ix_order_status_history_order_id_created_at"),
        "order_status_history",
        ["order_id", "created_at"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        op.f("ix_order_status_history_order_id_created_at"),
        table_name="order_status_history",
    )
    op.drop_table("order_status_history")
    op.drop_table("order_items")
    op.drop_index(op.f("uq_orders_user_id_idempotency_key"), table_name="orders")
    op.drop_index(op.f("ix_orders_pending_created_at"), table_name="orders")
    op.drop_index(op.f("ix_orders_user_id_created_at"), table_name="orders")
    op.drop_table("orders")
    # Must come last: the enum type is still referenced by the tables above.
    order_status.drop(op.get_bind(), checkfirst=True)
