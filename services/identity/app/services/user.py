"""Read-only user operations."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User
from app.repositories.user import UserRepository


class UserService:
    def __init__(self, session: AsyncSession) -> None:
        self.user_repo = UserRepository(session)

    async def get_by_id(self, user_id: UUID) -> User | None:
        return await self.user_repo.get_by_id(user_id)

    async def list_all(self, *, limit: int = 50, offset: int = 0) -> list[User]:
        return await self.user_repo.list_all(limit=limit, offset=offset)
