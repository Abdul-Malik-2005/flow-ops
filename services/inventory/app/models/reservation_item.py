from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    Integer,
    ForeignKey,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class ReservationItem(Base):
    __tablename__ = "reservation_items"

    __table_args__ = (
        CheckConstraint(
            "quantity > 0",
            name="ck_reservation_items_quantity_positive",
        ),
        ForeignKeyConstraint(
            ["product_id", "warehouse_id"],
            ["inventory.product_id", "inventory.warehouse_id"],
            name="fk_reservation_items_inventory",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )

    reservation_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("reservations.id", ondelete="CASCADE"),
        nullable=False,
    )

    product_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=False,
    )

    warehouse_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        nullable=False,
    )

    quantity: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    reservation: Mapped["Reservation"] = relationship(
        back_populates="items",
    )
