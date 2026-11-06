
from fastapi import APIRouter, Depends, HTTPException, status
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
)
async def create_reservation(
    data: ReservationCreate,
    session: AsyncSession = Depends(get_session),
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
        from fastapi import Response

        return Response(
            content=ReservationResponse.model_validate(
                reservation
            ).model_dump_json(),
            status_code=status.HTTP_200_OK,
            media_type="application/json",
        )

    return ReservationResponse.model_validate(reservation)
