from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.reservation import Reservation
from app.models.reservation_item import ReservationItem


class ReservationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def lock_order_id(self, order_id: UUID) -> None:
        """
        Блокирует обработку конкретного order_id
        до завершения текущей транзакции.
        """
        stmt = text(
            """
            SELECT pg_advisory_xact_lock(
                hashtextextended(CAST(:order_id AS text), 0)
            )
            """
        )

        await self.session.execute(
            stmt,
            {"order_id": str(order_id)},
        )

    async def get_by_order_id(self, order_id: UUID) -> Reservation | None:
        stmt = (
            select(Reservation)
            .options(selectinload(Reservation.items))
            .where(Reservation.order_id == order_id)
        )

        result = await self.session.scalars(stmt)
        return result.one_or_none()

    async def create(
        self,
        reservation: Reservation,
    ) -> Reservation:
        self.session.add(reservation)
        await self.session.flush()
        return reservation

    async def create_items(
        self,
        items: list[ReservationItem],
    ) -> None:
        self.session.add_all(items)
        await self.session.flush()
