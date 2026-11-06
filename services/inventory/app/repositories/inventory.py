
from uuid import UUID

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.inventory import Inventory


class InventoryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_product_and_warehouse(
        self,
        product_id: UUID,
        warehouse_id: UUID,
    ) -> Inventory | None:
        stmt = select(Inventory).where(
            Inventory.product_id == product_id,
            Inventory.warehouse_id == warehouse_id,
        )

        result = await self.session.scalars(stmt)

        return result.one_or_none()

    async def get_many_for_update(
        self,
        items: list[tuple[UUID, UUID]],
    ) -> list[Inventory]:
        if not items:
            return []

        stmt = (
            select(Inventory)
            .where(
                tuple_(
                    Inventory.product_id,
                    Inventory.warehouse_id,
                ).in_(items)
            )
            .order_by(
                Inventory.product_id,
                Inventory.warehouse_id,
            )
            .with_for_update()
        )

        result = await self.session.scalars(stmt)

        return list(result.all())
