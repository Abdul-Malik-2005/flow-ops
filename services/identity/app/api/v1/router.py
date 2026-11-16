"""API v1 aggregate router.

Each endpoint module declares its own prefix and tags, so they are included
as-is — declaring a prefix here as well would double it (`/auth/auth/...`).
"""

from fastapi import APIRouter

from app.api.v1.endpoints import auth, users

router = APIRouter()
router.include_router(auth.router)
router.include_router(users.router)
