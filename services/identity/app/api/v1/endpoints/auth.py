"""Authentication endpoints.

The API layer does exactly two things: translate HTTP into service calls and map
domain exceptions to status codes. All rules live in `AuthService`.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.session import get_session
from app.models.user import User
from app.schemas.auth import LoginRequest, RefreshRequest, RegisterRequest, TokenPair
from app.schemas.user import UserResponse
from app.services.auth import AuthService
from app.services.exceptions import (
    InvalidCredentialsError,
    RefreshTokenReusedError,
    RefreshTokenRevokedError,
    UserAlreadyExistsError,
    UserInactiveError,
)

router = APIRouter(prefix="/auth", tags=["auth"])


def _client_ip(request: Request) -> str | None:
    """Best-effort client IP for the login audit log."""
    return request.client.host if request.client else None


@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    responses={status.HTTP_409_CONFLICT: {"description": "Email already registered"}},
)
async def register(
    data: RegisterRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> UserResponse:
    try:
        user = await AuthService(session).register(data.email, data.password)
    except UserAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered",
        ) from exc

    return UserResponse.model_validate(user)


@router.post(
    "/login",
    response_model=TokenPair,
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Invalid email or password"},
        status.HTTP_403_FORBIDDEN: {"description": "User is inactive"},
    },
)
async def login(
    data: LoginRequest,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TokenPair:
    try:
        tokens = await AuthService(session).login(
            data.email,
            data.password,
            _client_ip(request),
        )
    except InvalidCredentialsError as exc:
        # One message for "no such user" and "wrong password" on purpose.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    except UserInactiveError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is inactive",
        ) from exc

    return TokenPair(
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
    )


@router.post(
    "/refresh",
    response_model=TokenPair,
    responses={
        status.HTTP_401_UNAUTHORIZED: {
            "description": "Refresh token is invalid, expired, revoked, or reused"
        },
        status.HTTP_403_FORBIDDEN: {"description": "User is inactive"},
    },
)
async def refresh(
    data: RefreshRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TokenPair:
    try:
        tokens = await AuthService(session).refresh(data.refresh_token)
    except RefreshTokenReusedError as exc:
        # The service has already revoked the whole chain.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token reuse detected, session revoked",
        ) from exc
    except RefreshTokenRevokedError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token is invalid or expired",
        ) from exc
    except UserInactiveError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is inactive",
        ) from exc

    return TokenPair(
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    data: RefreshRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> None:
    """Revoke the presented refresh token. Always 204 — logging out is idempotent."""
    await AuthService(session).logout(data.refresh_token)


@router.get("/me", response_model=UserResponse)
async def me(
    current_user: Annotated[User, Depends(get_current_user)],
) -> UserResponse:
    return UserResponse.model_validate(current_user)
