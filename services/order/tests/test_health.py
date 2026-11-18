"""Smoke tests for the two probes.

`/health` is liveness (process only) and `/ready` is readiness (database).
Keeping them separate is what stops an orchestrator from restarting a healthy
container when PostgreSQL blips, so both behaviours are asserted here.
"""

from httpx import AsyncClient

from tests.conftest import FakeSession


async def test_health_reports_healthy(api_client: AsyncClient) -> None:
    response = await api_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


async def test_health_never_touches_the_database(
    api_client: AsyncClient,
    fake_session: FakeSession,
) -> None:
    await api_client.get("/health")

    assert fake_session.statements == []


async def test_ready_checks_the_database(
    api_client: AsyncClient,
    fake_session: FakeSession,
) -> None:
    response = await api_client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "ok"}
    assert fake_session.statements == ["SELECT 1"]


async def test_ready_returns_503_when_database_is_down(
    api_client: AsyncClient,
    fake_session: FakeSession,
    sqlalchemy_error: Exception,
) -> None:
    fake_session.error = sqlalchemy_error

    response = await api_client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable", "database": "error"}
