"""Tests for the authentication dependencies.

A throwaway FastAPI app is used instead of the real one: the endpoints that need
authentication do not exist yet, and these tests must stay focused on the
dependency behaviour (status codes, headers, identity propagation).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.deps import CurrentUserId, PrincipalDep


def build_app() -> FastAPI:
    app = FastAPI()

    @app.get("/whoami")
    async def whoami(principal: PrincipalDep) -> dict[str, str]:
        return {"user_id": str(principal.user_id), "role": principal.role}

    @app.get("/user-id")
    async def user_id_endpoint(current_user_id: CurrentUserId) -> dict[str, str]:
        return {"user_id": str(current_user_id)}

    return app


@pytest.fixture()
async def auth_client(public_key_file: Path) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=build_app())
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


async def test_missing_header_returns_401(auth_client: AsyncClient) -> None:
    response = await auth_client.get("/whoami")

    assert response.status_code == 401
    assert response.json()["detail"] == "Not authenticated"
    assert response.headers["www-authenticate"] == "Bearer"


async def test_non_bearer_scheme_returns_401(auth_client: AsyncClient) -> None:
    response = await auth_client.get(
        "/whoami",
        headers={"Authorization": "Basic dXNlcjpwYXNz"},
    )

    assert response.status_code == 401


async def test_malformed_token_returns_401(auth_client: AsyncClient) -> None:
    response = await auth_client.get(
        "/whoami",
        headers={"Authorization": "Bearer not-a-jwt"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or expired access token"


async def test_expired_token_returns_401(
    auth_client: AsyncClient,
    make_token,
) -> None:
    past = datetime.now(UTC) - timedelta(minutes=1)
    token = make_token(
        claims=lambda p: {**p, "iat": past - timedelta(minutes=15), "exp": past}
    )

    response = await auth_client.get(
        "/whoami",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 401


async def test_valid_token_propagates_user_id(
    auth_client: AsyncClient,
    make_token,
    user_id,
) -> None:
    token = make_token()

    whoami = await auth_client.get(
        "/whoami",
        headers={"Authorization": f"Bearer {token}"},
    )
    owner = await auth_client.get(
        "/user-id",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert whoami.status_code == 200
    assert whoami.json() == {"user_id": str(user_id), "role": "user"}
    assert owner.json() == {"user_id": str(user_id)}
