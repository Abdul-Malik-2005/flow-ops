from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.schemas.reservation import ReservationCreate, ReservationResponse
from app.services.reservation import (
    InsufficientStockError,
    InventoryItemNotFoundError,
    ReservationService,
)

router = APIRouter(
    prefix="/reservations",
    tags=["Reservations"],
)


@router.post(
    "/",
    response_model=ReservationResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_200_OK: {
            "model": ReservationResponse,
            "description": "Reservation already exists",
        },
    },
)
async def create_reservation(
    data: ReservationCreate,
    response: Response,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> ReservationResponse:
    service = ReservationService(session)

    try:
        reservation, created = await service.create_reservation(data)

    except InventoryItemNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc

    except InsufficientStockError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc

    if not created:
        response.status_code = status.HTTP_200_OK

    return ReservationResponse.model_validate(reservation)
