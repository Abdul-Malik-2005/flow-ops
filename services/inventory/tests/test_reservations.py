import asyncio
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload

from app.models.inventory import Inventory
from app.models.reservation import Reservation
from app.repositories.reservation import ReservationRepository
from app.schemas.reservation import ReservationCreate, ReservationItemCreate
from app.services.reservation import (
    InsufficientStockError,
    InventoryItemNotFoundError,
    ReservationService,
)


@pytest.mark.asyncio
async def test_create_reservation_success(test_session_factory):
    product_id = uuid4()
    warehouse_id = uuid4()
    order_id = uuid4()

    # Arrange: создаём товар на складе с остатком 100.
    async with test_session_factory() as session:
        inventory = Inventory(
            product_id=product_id,
            warehouse_id=warehouse_id,
            quantity_on_hand=100,
            quantity_reserved=0,
        )

        session.add(inventory)
        await session.commit()

    # Act: резервируем 2 единицы товара.
    async with test_session_factory() as session:
        service = ReservationService(session)

        data = ReservationCreate(
            order_id=order_id,
            items=[
                ReservationItemCreate(
                    product_id=product_id,
                    warehouse_id=warehouse_id,
                    quantity=2,
                )
            ],
        )

        reservation, created = await service.create_reservation(data)

        assert created is True
        assert reservation.order_id == order_id

    # Assert: проверяем фактическое состояние БД.
    async with test_session_factory() as session:
        inventory = await session.get(
            Inventory,
            (product_id, warehouse_id),
        )

        saved_reservation = await session.scalar(
            select(Reservation)
            .options(selectinload(Reservation.items))
            .where(Reservation.order_id == order_id)
        )

        assert inventory is not None
        assert inventory.quantity_on_hand == 100
        assert inventory.quantity_reserved == 2
        assert inventory.available_quantity == 98

        assert saved_reservation is not None
        assert saved_reservation.order_id == order_id
        assert len(saved_reservation.items) == 1
        assert saved_reservation.items[0].quantity == 2
        assert saved_reservation.items[0].product_id == product_id
        assert saved_reservation.items[0].warehouse_id == warehouse_id


@pytest.mark.asyncio
async def test_create_reservation_insufficient_stock(test_session_factory):
    product_id = uuid4()
    warehouse_id = uuid4()
    order_id = uuid4()

    # Arrange: на складе есть только 100 единиц.
    async with test_session_factory() as session:
        session.add(
            Inventory(
                product_id=product_id,
                warehouse_id=warehouse_id,
                quantity_on_hand=100,
                quantity_reserved=0,
            )
        )
        await session.commit()

    # Act + Assert: пытаемся зарезервировать 101 единицу.
    async with test_session_factory() as session:
        service = ReservationService(session)

        data = ReservationCreate(
            order_id=order_id,
            items=[
                ReservationItemCreate(
                    product_id=product_id,
                    warehouse_id=warehouse_id,
                    quantity=101,
                )
            ],
        )

        with pytest.raises(InsufficientStockError):
            await service.create_reservation(data)

    # Assert: проверяем, что транзакция откатилась.
    async with test_session_factory() as session:
        inventory = await session.get(
            Inventory,
            (product_id, warehouse_id),
        )

        reservation = await session.scalar(
            select(Reservation).where(Reservation.order_id == order_id)
        )

        assert inventory is not None
        assert inventory.quantity_reserved == 0
        assert inventory.available_quantity == 100
        assert reservation is None


@pytest.mark.asyncio
async def test_create_reservation_inventory_not_found(test_session_factory):
    product_id = uuid4()
    warehouse_id = uuid4()
    order_id = uuid4()

    # Arrange: намеренно не создаём запись в inventory.

    # Act + Assert: пытаемся зарезервировать отсутствующий товар.
    async with test_session_factory() as session:
        service = ReservationService(session)

        data = ReservationCreate(
            order_id=order_id,
            items=[
                ReservationItemCreate(
                    product_id=product_id,
                    warehouse_id=warehouse_id,
                    quantity=1,
                )
            ],
        )

        with pytest.raises(InventoryItemNotFoundError):
            await service.create_reservation(data)

    # Assert: проверяем, что резервирование не появилось.
    async with test_session_factory() as session:
        reservation = await session.scalar(
            select(Reservation).where(Reservation.order_id == order_id)
        )

        assert reservation is None


@pytest.mark.asyncio
async def test_create_reservation_idempotency(test_session_factory):
    product_id = uuid4()
    warehouse_id = uuid4()
    order_id = uuid4()

    # Arrange: создаём товар с остатком 100.
    async with test_session_factory() as session:
        session.add(
            Inventory(
                product_id=product_id,
                warehouse_id=warehouse_id,
                quantity_on_hand=100,
                quantity_reserved=0,
            )
        )
        await session.commit()

    data = ReservationCreate(
        order_id=order_id,
        items=[
            ReservationItemCreate(
                product_id=product_id,
                warehouse_id=warehouse_id,
                quantity=5,
            )
        ],
    )

    # Act: дважды отправляем одинаковый запрос.
    async with test_session_factory() as session:
        service = ReservationService(session)
        first_reservation, first_created = await service.create_reservation(data)

    async with test_session_factory() as session:
        service = ReservationService(session)
        second_reservation, second_created = await service.create_reservation(data)

    # Assert: второй запрос вернул существующее резервирование.
    assert first_created is True
    assert second_created is False

    assert first_reservation.id == second_reservation.id

    # Assert: проверяем состояние БД.
    async with test_session_factory() as session:
        inventory = await session.get(
            Inventory,
            (product_id, warehouse_id),
        )

        reservations = (
            await session.scalars(
                select(Reservation).where(Reservation.order_id == order_id)
            )
        ).all()

        assert inventory is not None
        assert inventory.quantity_reserved == 5
        assert len(reservations) == 1


@pytest.mark.asyncio
async def test_create_reservation_duplicate_items(test_session_factory):
    product_id = uuid4()
    warehouse_id = uuid4()
    order_id = uuid4()

    # Arrange: создаём товар с остатком 100.
    async with test_session_factory() as session:
        session.add(
            Inventory(
                product_id=product_id,
                warehouse_id=warehouse_id,
                quantity_on_hand=100,
                quantity_reserved=0,
            )
        )
        await session.commit()

    # Act: отправляем две одинаковые позиции.
    async with test_session_factory() as session:
        service = ReservationService(session)

        data = ReservationCreate(
            order_id=order_id,
            items=[
                ReservationItemCreate(
                    product_id=product_id,
                    warehouse_id=warehouse_id,
                    quantity=2,
                ),
                ReservationItemCreate(
                    product_id=product_id,
                    warehouse_id=warehouse_id,
                    quantity=3,
                ),
            ],
        )

        _reservation, created = await service.create_reservation(data)

        assert created is True

    # Assert: проверяем состояние БД.
    async with test_session_factory() as session:
        inventory = await session.get(
            Inventory,
            (product_id, warehouse_id),
        )

        saved_reservation = await session.scalar(
            select(Reservation)
            .options(selectinload(Reservation.items))
            .where(Reservation.order_id == order_id)
        )

        assert inventory is not None
        assert inventory.quantity_reserved == 5

        assert saved_reservation is not None
        assert len(saved_reservation.items) == 1
        assert saved_reservation.items[0].quantity == 5


@pytest.mark.asyncio
async def test_create_reservation_multiple_items_atomicity(
    test_session_factory,
):
    product_a_id = uuid4()
    product_b_id = uuid4()
    warehouse_id = uuid4()
    order_id = uuid4()

    # Arrange: создаём два товара с разными остатками.
    async with test_session_factory() as session:
        session.add_all(
            [
                Inventory(
                    product_id=product_a_id,
                    warehouse_id=warehouse_id,
                    quantity_on_hand=100,
                    quantity_reserved=0,
                ),
                Inventory(
                    product_id=product_b_id,
                    warehouse_id=warehouse_id,
                    quantity_on_hand=3,
                    quantity_reserved=0,
                ),
            ]
        )
        await session.commit()

    # Act: пытаемся зарезервировать оба товара.
    async with test_session_factory() as session:
        service = ReservationService(session)

        data = ReservationCreate(
            order_id=order_id,
            items=[
                ReservationItemCreate(
                    product_id=product_a_id,
                    warehouse_id=warehouse_id,
                    quantity=5,
                ),
                ReservationItemCreate(
                    product_id=product_b_id,
                    warehouse_id=warehouse_id,
                    quantity=5,
                ),
            ],
        )

        with pytest.raises(InsufficientStockError):
            await service.create_reservation(data)

    # Assert: проверяем, что ни один товар не зарезервирован.
    async with test_session_factory() as session:
        inventory_a = await session.get(
            Inventory,
            (product_a_id, warehouse_id),
        )
        inventory_b = await session.get(
            Inventory,
            (product_b_id, warehouse_id),
        )

        reservation = await session.scalar(
            select(Reservation).where(Reservation.order_id == order_id)
        )

        assert inventory_a is not None
        assert inventory_b is not None

        assert inventory_a.quantity_reserved == 0
        assert inventory_b.quantity_reserved == 0

        assert reservation is None


@pytest.mark.asyncio
async def test_concurrent_reservations_do_not_oversell(
    test_session_factory,
):
    product_id = uuid4()
    warehouse_id = uuid4()
    order_a_id = uuid4()
    order_b_id = uuid4()

    # Arrange: на складе всего 10 единиц.
    async with test_session_factory() as session:
        session.add(
            Inventory(
                product_id=product_id,
                warehouse_id=warehouse_id,
                quantity_on_hand=10,
                quantity_reserved=0,
            )
        )
        await session.commit()

    async def reserve(order_id: UUID):
        async with test_session_factory() as session:
            service = ReservationService(session)

            data = ReservationCreate(
                order_id=order_id,
                items=[
                    ReservationItemCreate(
                        product_id=product_id,
                        warehouse_id=warehouse_id,
                        quantity=7,
                    )
                ],
            )

            return await service.create_reservation(data)

    # Act: запускаем два запроса одновременно.
    results = await asyncio.gather(
        reserve(order_a_id),
        reserve(order_b_id),
        return_exceptions=True,
    )

    # Assert: один запрос успешен, второй получил ошибку.
    successful_results = [
        result for result in results if not isinstance(result, BaseException)
    ]

    errors = [result for result in results if isinstance(result, BaseException)]

    assert len(successful_results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], InsufficientStockError)

    # Assert: проверяем фактическое состояние БД.
    async with test_session_factory() as session:
        inventory = await session.get(
            Inventory,
            (product_id, warehouse_id),
        )

        reservations = (
            await session.scalars(
                select(Reservation).where(
                    Reservation.order_id.in_([order_a_id, order_b_id])
                )
            )
        ).all()

        assert inventory is not None
        assert inventory.quantity_on_hand == 10
        assert inventory.quantity_reserved == 7
        assert inventory.available_quantity == 3

        assert len(reservations) == 1


@pytest.mark.asyncio
async def test_postgres_row_lock_blocks_second_transaction(
    test_session_factory,
    test_engine,
):
    product_id = uuid4()
    warehouse_id = uuid4()

    # Arrange: создаём товар с остатком 10.
    async with test_session_factory() as session:
        session.add(
            Inventory(
                product_id=product_id,
                warehouse_id=warehouse_id,
                quantity_on_hand=10,
                quantity_reserved=0,
            )
        )
        await session.commit()

    # Будем хранить PID второй транзакции.
    second_pid = asyncio.get_running_loop().create_future()

    async def second_transaction():
        async with test_session_factory() as session, session.begin():
            pid = await session.scalar(text("SELECT pg_backend_pid()"))
            second_pid.set_result(pid)

            inventory = await session.scalar(
                select(Inventory)
                .where(
                    Inventory.product_id == product_id,
                    Inventory.warehouse_id == warehouse_id,
                )
                .with_for_update()
            )

            assert inventory is not None

            return inventory.available_quantity

    # Первая транзакция захватывает блокировку.
    async with test_session_factory() as session:
        async with session.begin():
            inventory = await session.scalar(
                select(Inventory)
                .where(
                    Inventory.product_id == product_id,
                    Inventory.warehouse_id == warehouse_id,
                )
                .with_for_update()
            )

            assert inventory is not None

            # Запускаем вторую транзакцию.
            second_task = asyncio.create_task(second_transaction())

            pid = await second_pid

            # Проверяем через PostgreSQL, что вторая
            # транзакция действительно ожидает блокировку.
            lock_detected = False

            async with test_engine.connect() as connection:
                for _ in range(200):
                    wait_event = await connection.scalar(
                        text("""
                            SELECT wait_event_type
                            FROM pg_stat_activity
                            WHERE pid = :pid
                            """),
                        {"pid": pid},
                    )

                    if wait_event == "Lock":
                        lock_detected = True
                        break

                    await asyncio.sleep(0.01)

            # Первая транзакция изменяет остаток.
            inventory.quantity_reserved = 7

        # Здесь первая транзакция выполняет COMMIT.
        # Блокировка освобождается.

        # Вторая транзакция продолжает выполнение.
        second_available = await second_task

    # Assert: вторая транзакция действительно ожидала блокировку.
    assert lock_detected is True

    # Assert: вторая транзакция увидела обновлённые данные.
    assert second_available == 3

    # Assert: проверяем окончательное состояние БД.
    async with test_session_factory() as session:
        inventory = await session.get(
            Inventory,
            (product_id, warehouse_id),
        )

        assert inventory is not None
        assert inventory.quantity_reserved == 7
        assert inventory.available_quantity == 3


@pytest.mark.asyncio
async def test_concurrent_same_order_id_is_idempotent(
    test_session_factory,
    monkeypatch,
):
    order_id = uuid4()
    warehouse_id = uuid4()

    product_a_id = uuid4()
    product_b_id = uuid4()

    # Arrange: создаём два независимых товара.
    async with test_session_factory() as session:
        session.add_all(
            [
                Inventory(
                    product_id=product_a_id,
                    warehouse_id=warehouse_id,
                    quantity_on_hand=100,
                    quantity_reserved=0,
                ),
                Inventory(
                    product_id=product_b_id,
                    warehouse_id=warehouse_id,
                    quantity_on_hand=100,
                    quantity_reserved=0,
                ),
            ]
        )
        await session.commit()

    # Сохраняем оригинальный метод Advisory Lock.
    original_lock = ReservationRepository.lock_order_id

    # Гарантируем, что оба запроса дойдут до получения блокировки
    # до того, как один из них начнёт её получать.
    ready_count = 0
    barrier = asyncio.Event()
    counter_lock = asyncio.Lock()

    async def synchronized_lock(self, current_order_id):
        nonlocal ready_count

        async with counter_lock:
            ready_count += 1

            if ready_count == 2:
                barrier.set()

        await barrier.wait()

        return await original_lock(self, current_order_id)

    monkeypatch.setattr(
        ReservationRepository,
        "lock_order_id",
        synchronized_lock,
    )

    async def reserve(product_id):
        async with test_session_factory() as session:
            service = ReservationService(session)

            data = ReservationCreate(
                order_id=order_id,
                items=[
                    ReservationItemCreate(
                        product_id=product_id,
                        warehouse_id=warehouse_id,
                        quantity=4,
                    )
                ],
            )

            return await service.create_reservation(data)

    # Act: одновременно отправляем два запроса с одинаковым order_id.
    results = await asyncio.gather(
        reserve(product_a_id),
        reserve(product_b_id),
        return_exceptions=True,
    )

    # Assert: оба запроса должны завершиться успешно.
    assert len(results) == 2
    assert all(not isinstance(result, BaseException) for result in results)

    first_reservation, first_created = results[0]
    second_reservation, second_created = results[1]

    # Оба запроса должны получить одну и ту же резервацию.
    assert first_reservation.id == second_reservation.id

    # Только один запрос действительно создал резервацию.
    assert sorted([first_created, second_created]) == [False, True]

    # Проверяем состояние базы данных.
    async with test_session_factory() as session:
        reservations = (
            await session.scalars(
                select(Reservation)
                .options(selectinload(Reservation.items))
                .where(Reservation.order_id == order_id)
            )
        ).all()

        assert len(reservations) == 1
        assert len(reservations[0].items) == 1
        assert reservations[0].items[0].quantity == 4

        inventory_a = await session.get(
            Inventory,
            (product_a_id, warehouse_id),
        )
        inventory_b = await session.get(
            Inventory,
            (product_b_id, warehouse_id),
        )

        assert inventory_a is not None
        assert inventory_b is not None

        # Суммарно зарезервировано ровно 4 единицы,
        # а не 8.
        assert (inventory_a.quantity_reserved + inventory_b.quantity_reserved) == 4
