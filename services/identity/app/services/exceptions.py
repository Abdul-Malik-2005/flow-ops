"""Domain errors raised by the service layer.

They deliberately carry no HTTP knowledge: the API layer maps each one to a
status code in a single place, and none of these messages are returned to the
client verbatim (except where we choose to).
"""


class UserAlreadyExistsError(Exception):
    """Email is already registered."""


class InvalidCredentialsError(Exception):
    """Wrong email or password — deliberately indistinguishable to the caller."""


class InvalidAccessTokenError(Exception):
    """Access token is malformed, expired, or its subject no longer exists."""


class UserInactiveError(Exception):
    """The account exists but `is_active` is false."""


class RefreshTokenRevokedError(Exception):
    """Refresh token is unknown, revoked, or expired."""


class RefreshTokenReusedError(Exception):
    """A rotated refresh token was presented again — the chain is compromised."""
