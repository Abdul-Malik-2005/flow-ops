import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import make_url
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import settings
from app.db.base import Base
from app.db.session import get_session
from app.main import app
from app.models.inventory import Inventory  # noqa: F401
from app.models.reservation import Reservation  # noqa: F401
from app.models.reservation_item import ReservationItem  # noqa: F401


@pytest_asyncio.fixture
async def test_engine():
    if not settings.test_database_url:
        raise RuntimeError("TEST DATABASE URL is not configured in .env")

    database_name = make_url(settings.test_database_url).database

    if database_name != "inventory_test_db":
        raise RuntimeError("Tests can only run against inventory_test_db")

    engine = create_async_engine(
        settings.test_database_url,
        pool_pre_ping=True,
    )

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest.fixture
def test_session_factory(test_engine):
    return async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


@pytest_asyncio.fixture
async def db_session(test_session_factory):
    async with test_session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def client(test_session_factory):
    async def override_get_session():
        async with test_session_factory() as session:
            yield session

    previous_overrides = app.dependency_overrides.copy()

    app.dependency_overrides[get_session] = override_get_session

    transport = ASGITransport(app=app)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as test_client:
        yield test_client

    app.dependency_overrides.clear()
    app.dependency_overrides.update(previous_overrides)
