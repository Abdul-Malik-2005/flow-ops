"""Readiness probe: 200 only while PostgreSQL answers.

`/health` is liveness and must keep returning 200 when the database is down;
`/ready` is what a load balancer should gate traffic on.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import OperationalError

from app.db.session import get_session
from app.main import app

pytestmark = pytest.mark.integration

READY = "/ready"


class _BrokenSession:
    """Stands in for `AsyncSession` when the database cannot be reached."""

    async def execute(self, *args: object, **kwargs: object) -> None:
        raise OperationalError("SELECT 1", {}, ConnectionError("db is down"))


async def test_ready_reports_database_ok(api_client) -> None:
    response = await api_client.get(READY)

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "ok"}


async def test_ready_returns_503_when_database_is_unreachable(api_client) -> None:
    async def broken_session():
        yield _BrokenSession()

    previous = app.dependency_overrides[get_session]
    app.dependency_overrides[get_session] = broken_session
    try:
        response = await api_client.get(READY)
    finally:
        app.dependency_overrides[get_session] = previous

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable", "database": "error"}


async def test_health_stays_200_without_a_database(api_client) -> None:
    async def broken_session():
        yield _BrokenSession()

    previous = app.dependency_overrides[get_session]
    app.dependency_overrides[get_session] = broken_session
    try:
        response = await api_client.get("/health")
    finally:
        app.dependency_overrides[get_session] = previous

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}
