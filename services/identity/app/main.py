from contextlib import asynccontextmanager

from fastapi import FastAPI

# from app.api.v1.router import router as api_v1_router
from app.db.session import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
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

# app.include_router(api_v1_router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
