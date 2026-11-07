from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.endpoints.reservations import router as reservations_router
from app.db.session import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(
    title="FlowOps Inventory Service",
    description="Inventory management and reservation service",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(
    reservations_router,
    prefix="/api/v1",
)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
