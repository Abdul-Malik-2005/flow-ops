"""HTTP client for the Inventory reservation API — the only synchronous saga step.

The client never raises on a business answer: every transport or protocol
outcome is folded into a `ReservationResult`, because the caller must choose
between "retryable, keep the order pending" and "write a terminal status".
Raising here would turn a stock rejection into a 500 and lose the audit trail.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from http import HTTPStatus
from typing import Protocol

import httpx

from app.domain.order import OrderLine

logger = logging.getLogger(__name__)

# Agreed inter-service contract (docs/architecture.md §5). Inventory ignores the
# value today; sending it from day one means the receiving side can enforce it
# without a coordinated release.
SERVICE_TOKEN_HEADER = "X-Service-Token"

# The trailing slash is not cosmetic: Inventory exposes `POST .../reservations/`
# and Starlette answers `/reservations` with a 307 redirect. `httpx.AsyncClient`
# does not follow redirects by default, so the wrong path silently degrades to
# "unavailable".
RESERVE_PATH = "/api/v1/reservations/"


class ReservationOutcome(StrEnum):
    """What the reservation attempt actually told us."""

    #: 201 (created) or 200 (idempotent replay for the same `order_id`).
    RESERVED = "reserved"
    #: 409 — not enough stock. Terminal: the order becomes `failed`.
    REJECTED = "rejected"
    #: 404 — unknown product/warehouse. Terminal, and a client error for us.
    ITEM_NOT_FOUND = "item_not_found"
    #: 400/422 — Inventory refused our payload. Terminal, but *our* bug: the API
    #: must surface it as a gateway error, not as the client's fault.
    INVALID_REQUEST = "invalid_request"
    #: Timeout, 5xx, 429, connection error, unparsable 2xx — the outcome is
    #: unknown. The order stays `pending`, which is exactly why Inventory's
    #: create is idempotent by `order_id`.
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class ReservationResult:
    outcome: ReservationOutcome
    reservation_id: uuid.UUID | None = None
    status_code: int | None = None

    @property
    def is_reserved(self) -> bool:
        return self.outcome is ReservationOutcome.RESERVED

    @property
    def is_retryable(self) -> bool:
        """Only an unknown outcome is retryable; the rest are terminal."""
        return self.outcome is ReservationOutcome.UNAVAILABLE


class InventoryClientPort(Protocol):
    """Narrow port: one saga step, so fakes stay trivial."""

    async def reserve(
        self,
        *,
        order_id: uuid.UUID,
        lines: Sequence[OrderLine],
    ) -> ReservationResult: ...


_TERMINAL_OUTCOMES: dict[int, ReservationOutcome] = {
    HTTPStatus.CONFLICT: ReservationOutcome.REJECTED,
    HTTPStatus.NOT_FOUND: ReservationOutcome.ITEM_NOT_FOUND,
    HTTPStatus.BAD_REQUEST: ReservationOutcome.INVALID_REQUEST,
    HTTPStatus.UNPROCESSABLE_ENTITY: ReservationOutcome.INVALID_REQUEST,
}


def _reservation_id(response: httpx.Response) -> uuid.UUID | None:
    """Extract `id` from the reservation body, tolerating a broken payload."""
    try:
        raw = response.json()["id"]
    except ValueError, KeyError, TypeError:
        return None
    try:
        return uuid.UUID(str(raw))
    except ValueError:
        return None


class InventoryClient:
    """Real implementation, sharing one connection pool from `app.state`."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        base_url: str,
        service_token: str,
    ) -> None:
        self._http = http
        self._url = base_url.rstrip("/") + RESERVE_PATH
        self._headers = {SERVICE_TOKEN_HEADER: service_token} if service_token else {}

    async def reserve(
        self,
        *,
        order_id: uuid.UUID,
        lines: Sequence[OrderLine],
    ) -> ReservationResult:
        payload = {
            "order_id": str(order_id),
            "items": [
                {
                    "product_id": str(line.product_id),
                    "warehouse_id": str(line.warehouse_id),
                    "quantity": line.quantity,
                }
                for line in lines
            ],
        }

        try:
            response = await self._http.post(
                self._url,
                json=payload,
                headers=self._headers,
            )
        except httpx.TimeoutException:
            logger.warning("inventory reserve timed out for order %s", order_id)
            return ReservationResult(ReservationOutcome.UNAVAILABLE)
        except httpx.HTTPError as exc:
            logger.warning("inventory reserve failed for order %s: %s", order_id, exc)
            return ReservationResult(ReservationOutcome.UNAVAILABLE)

        return self._interpret(response, order_id=order_id)

    def _interpret(
        self,
        response: httpx.Response,
        *,
        order_id: uuid.UUID,
    ) -> ReservationResult:
        status_code = response.status_code

        if status_code in (HTTPStatus.OK, HTTPStatus.CREATED):
            reservation_id = _reservation_id(response)
            if reservation_id is None:
                # The reservation may well exist, but without its id we cannot
                # link it. Staying `pending` is safe: a retry with the same
                # `order_id` returns the existing reservation.
                logger.warning(
                    "inventory answered %s without a usable id for order %s",
                    status_code,
                    order_id,
                )
                return ReservationResult(
                    ReservationOutcome.UNAVAILABLE,
                    status_code=status_code,
                )
            return ReservationResult(
                ReservationOutcome.RESERVED,
                reservation_id=reservation_id,
                status_code=status_code,
            )

        outcome = _TERMINAL_OUTCOMES.get(status_code)
        if outcome is not None:
            return ReservationResult(outcome, status_code=status_code)

        # 429, 5xx and anything unexpected: never mark the order failed on an
        # answer we do not understand — `failed` cannot be undone.
        logger.warning(
            "unexpected inventory response %s for order %s",
            status_code,
            order_id,
        )
        return ReservationResult(
            ReservationOutcome.UNAVAILABLE, status_code=status_code
        )
