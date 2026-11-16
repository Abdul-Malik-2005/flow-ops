"""Response schemas for user resources.

Request schemas live in `app.schemas.auth`; this module describes only what the
API returns. A dedicated response model guarantees that internals such as
`password_hash` can never leak into a response body.
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr

from app.models.user import UserRole


class UserResponse(BaseModel):
    """Public representation of a user.

    `password_hash` is deliberately absent: the model is built from an ORM
    object via `model_validate`, and unknown attributes are ignored.
    """

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    email: EmailStr
    role: UserRole
    is_active: bool
    created_at: datetime
    updated_at: datetime
