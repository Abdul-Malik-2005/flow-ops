"""Aggregate router for API v1.

Endpoint modules declare their own prefix and tags (same convention as Identity),
so nothing is prefixed twice.
"""

from fastapi import APIRouter

from app.api.v1.endpoints import orders

router = APIRouter()
router.include_router(orders.router)
