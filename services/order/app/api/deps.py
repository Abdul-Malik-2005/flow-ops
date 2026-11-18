"""Authentication dependencies for the API layer."""

from __future__ import annotations

import uuid
from typing import Annotated

import httpx
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.inventory import InventoryClient, InventoryClientPort
from app.core.config import settings
from app.core.security import InvalidTokenError, Principal, decode_access_token
from app.db.session import get_session
from app.repositories.order import SqlOrderRepository
from app.services.order import OrderService
from app.services.pricing import NullPricing

# `auto_error=False` lets us answer a missing header with our own 401 and a
# `WWW-Authenticate` header instead of FastAPI's generic 403.
bearer_scheme = HTTPBearer(auto_error=False)

_UNAUTHORIZED_HEADERS = {"WWW-Authenticate": "Bearer"}


async def get_principal(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(bearer_scheme),
    ],
) -> Principal:
    """Resolve the caller from a Bearer access token.

    No database access: the token is verified locally and its claims are the
    only source of identity. A role change in Identity therefore applies with
    the next token (access TTL is 15 minutes) — the accepted trade-off for not
    calling Identity on every request.
    """
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers=_UNAUTHORIZED_HEADERS,
        )

    try:
        return decode_access_token(credentials.credentials)
    except InvalidTokenError as exc:
        # The precise reason stays in the exception chain for logs; the client
        # gets a vague message so token internals are never leaked.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token",
            headers=_UNAUTHORIZED_HEADERS,
        ) from exc


PrincipalDep = Annotated[Principal, Depends(get_principal)]


async def get_current_user_id(principal: PrincipalDep) -> uuid.UUID:
    """Order owner: always taken from the token, never from the request body."""
    return principal.user_id


CurrentUserId = Annotated[uuid.UUID, Depends(get_current_user_id)]


async def get_inventory_client(request: Request) -> InventoryClientPort:
    """Inventory client bound to the lifespan-managed connection pool.

    The pool lives on `app.state` because `httpx.AsyncClient` is expensive to
    create and must be closed on shutdown. A missing pool means the lifespan was
    never run (a wiring bug, not a client error), so it fails loudly.
    """
    http_client: httpx.AsyncClient | None = getattr(
        request.app.state,
        "inventory_http",
        None,
    )
    if http_client is None:
        raise RuntimeError(
            "httpx.AsyncClient is not initialised: the application lifespan did "
            "not run, so Inventory calls cannot be made"
        )

    return InventoryClient(
        http_client,
        base_url=settings.inventory_base_url,
        service_token=settings.service_token,
    )


InventoryClientDep = Annotated[InventoryClientPort, Depends(get_inventory_client)]


async def get_order_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    inventory: InventoryClientDep,
) -> OrderService:
    """Assemble the application service for one request.

    Everything here is cheap or request-scoped: the session is per request, the
    HTTP pool is shared, and the currency/lease come from settings. Tests override
    this dependency instead of patching modules.
    """
    return OrderService(
        session=session,
        repository=SqlOrderRepository(session),
        inventory=inventory,
        pricing=NullPricing(),
        currency=settings.default_currency,
        stale_after_seconds=settings.reservation_stale_after_seconds,
    )


OrderServiceDep = Annotated[OrderService, Depends(get_order_service)]
