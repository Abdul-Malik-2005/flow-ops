from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import router as api_v1_router
from app.core import security
from app.core.config import settings
from app.db.session import engine


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup/shutdown hooks.

    JWT keys are loaded eagerly so a misconfigured container crashes at start-up
    instead of answering `/health` and failing the first `/auth/login` with 500.
    Tests build the app with `ENVIRONMENT=testing`: their keys come from the
    `jwt_keys` fixture, which is installed per test, not at import time.
    """
    if settings.environment != "testing":
        security.load_keys()

    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(
    title="FlowOps Identity Service",
    description="Authentication, users and tokens service",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(api_v1_router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
