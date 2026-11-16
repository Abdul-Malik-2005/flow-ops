"""Queries for the `refresh_tokens` table.

The table stores one row per issued refresh token and doubles as an audit log:
`revoked_at` marks a dead token, `replaced_by_jti` links a rotated token to its
successor. That link is what makes refresh-token reuse detection possible.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.refresh_token import RefreshToken


class RefreshTokenRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_jti(self, jti: UUID) -> RefreshToken | None:
        return await self.session.get(RefreshToken, jti)

    async def get_by_jti_for_update(self, jti: UUID) -> RefreshToken | None:
        """Load a token row with a row-level lock.

        Two concurrent `/auth/refresh` calls with the same token would otherwise
        both pass the "not revoked" check and rotate twice. `SELECT ... FOR
        UPDATE` serializes them: the second caller waits, then sees
        `revoked_at` set and is treated as a reuse attempt.
        """
        stmt = select(RefreshToken).where(RefreshToken.jti == jti).with_for_update()
        return await self.session.scalar(stmt)

    async def create(self, refresh_token: RefreshToken) -> RefreshToken:
        self.session.add(refresh_token)
        await self.session.flush()
        return refresh_token

    async def revoke(self, jti: UUID, replaced_by_jti: UUID | None = None) -> None:
        """Mark a single token as revoked (rotation or logout)."""
        stmt = (
            update(RefreshToken)
            .where(RefreshToken.jti == jti, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=func.now(), replaced_by_jti=replaced_by_jti)
        )
        await self.session.execute(stmt)

    async def revoke_all_for_user(self, user_id: UUID) -> None:
        """Revoke every live token of a user — used when reuse is detected."""
        stmt = (
            update(RefreshToken)
            .where(
                RefreshToken.user_id == user_id,
                RefreshToken.revoked_at.is_(None),
            )
            .values(revoked_at=func.now())
        )
        await self.session.execute(stmt)
