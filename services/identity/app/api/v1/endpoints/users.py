"""User administration endpoints (admin only)."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_admin
from app.db.session import get_session
from app.models.user import User
from app.schemas.user import UserResponse
from app.services.user import UserService

router = APIRouter(prefix="/users", tags=["users"])


@router.get("", response_model=list[UserResponse])
async def list_users(
    session: Annotated[AsyncSession, Depends(get_session)],
    _admin: Annotated[User, Depends(require_admin)],
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[UserResponse]:
    """Paginated user list.

    `limit` has a hard ceiling: an unbounded `SELECT *` on `users` is a
    denial-of-service waiting to happen once the table grows.
    """
    users = await UserService(session).list_all(limit=limit, offset=offset)
    return [UserResponse.model_validate(user) for user in users]
