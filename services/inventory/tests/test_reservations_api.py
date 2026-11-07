from uuid import uuid4

import pytest
from sqlalchemy import select

from app.models.inventory import Inventory
from app.models.reservation import Reservation


@pytest.mark.asyncio
async def test_create_reservation_api_success(
    client,
    test_session_factory,
):
    product_id = uuid4()
    warehouse_id = uuid4()
    order_id = uuid4()

    # Arrange: создаём товар на складе.
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

    # Act: отправляем HTTP-запрос.
    response = await client.post(
        "/api/v1/reservations/",
        json={
            "order_id": str(order_id),
            "items": [
                {
                    "product_id": str(product_id),
                    "warehouse_id": str(warehouse_id),
                    "quantity": 5,
                }
            ],
        },
    )

    # Assert: проверяем HTTP-ответ.
    assert response.status_code == 201

    body = response.json()

    assert body["order_id"] == str(order_id)
    assert body["status"] == "reserved"
    assert len(body["items"]) == 1
    assert body["items"][0]["product_id"] == str(product_id)
    assert body["items"][0]["warehouse_id"] == str(warehouse_id)
    assert body["items"][0]["quantity"] == 5

    # Assert: проверяем фактическое состояние БД.
    async with test_session_factory() as session:
        inventory = await session.get(
            Inventory,
            (product_id, warehouse_id),
        )

        reservation = await session.scalar(
            select(Reservation).where(Reservation.order_id == order_id)
        )

        assert inventory is not None
        assert inventory.quantity_reserved == 5

        assert reservation is not None


@pytest.mark.asyncio
async def test_create_reservation_api_idempotency(
    client,
    test_session_factory,
):
    product_id = uuid4()
    warehouse_id = uuid4()
    order_id = uuid4()

    # Arrange.
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

    payload = {
        "order_id": str(order_id),
        "items": [
            {
                "product_id": str(product_id),
                "warehouse_id": str(warehouse_id),
                "quantity": 5,
            }
        ],
    }

    # Act: отправляем два одинаковых запроса.
    first_response = await client.post(
        "/api/v1/reservations/",
        json=payload,
    )

    second_response = await client.post(
        "/api/v1/reservations/",
        json=payload,
    )

    # Assert.
    assert first_response.status_code == 201
    assert second_response.status_code == 200

    first_body = first_response.json()
    second_body = second_response.json()

    assert first_body["id"] == second_body["id"]

    # Проверяем, что товар зарезервирован только один раз.
    async with test_session_factory() as session:
        inventory = await session.get(
            Inventory,
            (product_id, warehouse_id),
        )

        assert inventory is not None
        assert inventory.quantity_reserved == 5


@pytest.mark.asyncio
async def test_create_reservation_api_insufficient_stock(
    client,
    test_session_factory,
):
    product_id = uuid4()
    warehouse_id = uuid4()

    # Arrange: на складе только 3 единицы.
    async with test_session_factory() as session:
        session.add(
            Inventory(
                product_id=product_id,
                warehouse_id=warehouse_id,
                quantity_on_hand=3,
                quantity_reserved=0,
            )
        )
        await session.commit()

    # Act: пытаемся зарезервировать 5.
    response = await client.post(
        "/api/v1/reservations/",
        json={
            "order_id": str(uuid4()),
            "items": [
                {
                    "product_id": str(product_id),
                    "warehouse_id": str(warehouse_id),
                    "quantity": 5,
                }
            ],
        },
    )

    # Assert.
    assert response.status_code == 409
    assert "Not enough stock for product" in response.json()["detail"]


@pytest.mark.asyncio
async def test_create_reservation_api_inventory_not_found(client):
    response = await client.post(
        "/api/v1/reservations/",
        json={
            "order_id": str(uuid4()),
            "items": [
                {
                    "product_id": str(uuid4()),
                    "warehouse_id": str(uuid4()),
                    "quantity": 1,
                }
            ],
        },
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_create_reservation_api_empty_items(client):
    response = await client.post(
        "/api/v1/reservations/",
        json={
            "order_id": str(uuid4()),
            "items": [],
        },
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_reservation_api_invalid_uuid(client):
    response = await client.post(
        "/api/v1/reservations/",
        json={
            "order_id": "not-a-valid-uuid",
            "items": [
                {
                    "product_id": str(uuid4()),
                    "warehouse_id": str(uuid4()),
                    "quantity": 1,
                }
            ],
        },
    )

    assert response.status_code == 422
