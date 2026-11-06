
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.reservation import Reservation
from app.models.reservation_item import ReservationItem
from app.repositories.inventory import InventoryRepository
from app.repositories.reservation import ReservationRepository
from app.schemas.reservation import ReservationCreate, ReservationItemCreate


class InventoryItemNotFoundError(Exception):
    """Запрошенный товар отсутствует на складе."""


class InsufficientStockError(Exception):
    """Недостаточно доступного количества товара."""


class ReservationService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.inventory_repo = InventoryRepository(session)
        self.reservation_repo = ReservationRepository(session)

    def normalize_items(
        self,
        items: list[ReservationItemCreate],
    ) -> list[ReservationItemCreate]:
        quantities: dict[tuple[UUID, UUID], int] = {}

        for item in items:
            key = (item.product_id, item.warehouse_id)
            quantities[key] = quantities.get(key, 0) + item.quantity

        result = [
            ReservationItemCreate(
                product_id=product_id,
                warehouse_id=warehouse_id,
                quantity=quantity,
            )
            for (product_id, warehouse_id), quantity in quantities.items()
        ]

        return sorted(
            result,
            key=lambda item: (
                str(item.product_id),
                str(item.warehouse_id),
            ),
        )

    async def create_reservation(
        self,
        data: ReservationCreate,
    ) -> tuple[Reservation, bool]:
        async with self.session.begin():
            existing_reservation = (
                await self.reservation_repo.get_by_order_id(data.order_id)
            )

            if existing_reservation:
                return existing_reservation, False

            items = self.normalize_items(data.items)

            inventory_items = await self.inventory_repo.get_many_for_update(
                [
                    (item.product_id, item.warehouse_id)
                    for item in items
                ]
            )

            existing_reservation = (
                await self.reservation_repo.get_by_order_id(data.order_id)
            )

            if existing_reservation:
                return existing_reservation, False

            inventory_map = {
                (item.product_id, item.warehouse_id): item
                for item in inventory_items
            }

            for item in items:
                key = (item.product_id, item.warehouse_id)
                inventory = inventory_map.get(key)

                if inventory is None:
                    raise InventoryItemNotFoundError(
                        f"Inventory not found for product {item.product_id}"
                    )

                if inventory.available_quantity < item.quantity:
                    raise InsufficientStockError(
                        f"Not enough stock for product {item.product_id}"
                    )

            reservation_items = [
                ReservationItem(
                    product_id=item.product_id,
                    warehouse_id=item.warehouse_id,
                    quantity=item.quantity,
                )
                for item in items
            ]

            reservation = Reservation(
                order_id=data.order_id,
                items=reservation_items,
            )

            await self.reservation_repo.create(reservation)

            for item in items:
                inventory = inventory_map[
                    (item.product_id, item.warehouse_id)
                ]
                inventory.quantity_reserved += item.quantity

            await self.session.flush()

            return reservation, True
