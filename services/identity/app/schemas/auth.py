"""Request/response schemas for the authentication endpoints."""

from pydantic import BaseModel, EmailStr, Field

# Upper bound protects the server from hashing multi-megabyte passwords
# (Argon2 cost grows with input size). 128 is well above any sane password and
# well below the 255-char `password_hash` column value (a hash is fixed-length).
_PASSWORD_MIN_LENGTH = 8
_PASSWORD_MAX_LENGTH = 128


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(
        min_length=_PASSWORD_MIN_LENGTH,
        max_length=_PASSWORD_MAX_LENGTH,
    )


class LoginRequest(BaseModel):
    email: EmailStr
    # No `min_length` here: an old/short password must yield 401, not 422 —
    # a validation error would reveal that the password format is "wrong",
    # which is a (small) enumeration oracle.
    password: str = Field(max_length=_PASSWORD_MAX_LENGTH)


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=1)


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
