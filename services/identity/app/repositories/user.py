"""Queries for the `users` table.

Repositories own SQL only: no commits, no business rules. The surrounding
transaction is managed by the service layer (see `app.services.auth`).
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User


class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_email(self, email: str) -> User | None:
        """Look up a user by an already-normalized (lower-case) email."""
        stmt = select(User).where(User.email == email)
        return await self.session.scalar(stmt)

    async def get_by_id(self, user_id: UUID) -> User | None:
        return await self.session.get(User, user_id)

    async def create(self, user: User) -> User:
        self.session.add(user)
        await self.session.flush()
        return user

    async def list_all(self, *, limit: int = 50, offset: int = 0) -> list[User]:
        """Return a page of users in a stable order.

        `id` breaks ties on `created_at` so pagination cannot silently skip or
        repeat rows that share a timestamp.
        """
        stmt = (
            select(User).order_by(User.created_at, User.id).limit(limit).offset(offset)
        )
        result = await self.session.scalars(stmt)
        return list(result)
