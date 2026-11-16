"""Audit log for login attempts.

Every `/auth/login` call produces one row, successful or not. The rows are
write-only for now; they become the data source for rate limiting once Redis is
introduced.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.login_attempt import LoginAttempt


class LoginAttemptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def record(self, attempt: LoginAttempt) -> None:
        self.session.add(attempt)
        await self.session.flush()
