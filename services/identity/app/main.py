from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.router import router as api_v1_router
from app.core import security
from app.core.config import settings
from app.db.session import engine, get_session


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
    """Liveness: the process is up. Never touches the database."""
    return {"status": "healthy"}


@app.get("/ready")
async def readiness_check(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> JSONResponse:
    """Readiness: can this instance actually serve traffic?

    `/health` stays a liveness probe, so an orchestrator does not restart a
    healthy process just because PostgreSQL is briefly away. `/ready` is the
    one to gate traffic on: it reports 503 until `SELECT 1` succeeds.
    """
    try:
        await session.execute(text("SELECT 1"))
    except SQLAlchemyError:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unavailable", "database": "error"},
        )
    return JSONResponse(content={"status": "ready", "database": "ok"})
