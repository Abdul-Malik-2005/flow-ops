from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.models.reservation import ReservationStatus


class ReservationItemCreate(BaseModel):
    product_id: UUID
    warehouse_id: UUID
    quantity: int = Field(gt=0)


class ReservationCreate(BaseModel):
    order_id: UUID
    items: list[ReservationItemCreate] = Field(min_length=1)


class ReservationItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    product_id: UUID
    warehouse_id: UUID
    quantity: int


class ReservationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    order_id: UUID
    status: ReservationStatus
    created_at: datetime
    updated_at: datetime
    items: list[ReservationItemResponse]
