"""Shared fixtures for notification tests.

Settings are constructed at import time by `app.core.config`, so the required
environment variables have to exist before any `app.*` import. These values only
satisfy pydantic validation; tests that need a real database override them.
"""

from __future__ import annotations

import os

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://test:test@localhost:5432/notification_test_db",
)
os.environ.setdefault("DEBUG", "false")
os.environ.setdefault("ENVIRONMENT", "testing")
