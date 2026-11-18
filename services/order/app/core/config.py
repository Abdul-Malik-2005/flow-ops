from typing import Literal, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the Order service.

    Order owns only its own database. Everything that belongs to another
    service arrives either as a token claim (Identity) or over HTTP
    (Inventory, later Catalog), so no foreign credentials live here.
    """

    database_url: str
    test_database_url: str | None = None

    debug: bool = False
    environment: Literal["development", "testing", "production"] = "development"

    # Access tokens are minted by Identity (RS256 + `kid`). Order needs the
    # public key only: it verifies, never signs.
    jwt_algorithm: Literal["RS256"] = "RS256"
    jwt_kid: str = "identity-1"
    jwt_public_key_path: str = ""

    # Order -> Inventory (synchronous saga step). Inventory ignores the token
    # today, but the header is part of the agreed inter-service contract.
    inventory_base_url: str = "http://localhost:8000"
    service_token: str = ""
    inventory_timeout_seconds: float = 5.0

    # Lease on the saga step. A `pending` order carries
    # `reservation_started_at` while the reserve call may still be in flight;
    # cancellation is refused until the marker is older than this value (see
    # docs/architecture.md §5). Must exceed the HTTP timeout twice: otherwise a
    # still-running call could be mistaken for an abandoned one.
    reservation_stale_after_seconds: float = Field(default=30.0, gt=0)

    # Reconciler: a separate process started from the same image. A `pending`
    # order whose lease has expired means the reserve call was lost (or its
    # outcome never reached the database), so the worker re-drives it. After
    # `reservation_max_attempts` such claims the order is failed instead of
    # being retried forever. The alert threshold only drives an error log and
    # never changes state.
    reconcile_interval_seconds: float = Field(default=30.0, gt=0)
    reconcile_batch_size: int = Field(default=20, ge=1, le=100)
    reservation_max_attempts: int = Field(default=20, ge=1)
    pending_alert_after_seconds: float = Field(default=900.0, gt=0)

    # Currency is stored from the first order onwards, while the amounts stay
    # NULL until Catalog becomes the source of prices.
    default_currency: str = "RUB"

    @model_validator(mode="after")
    def _lease_outlives_the_inventory_call(self) -> Self:
        minimum = 2 * self.inventory_timeout_seconds
        if self.reservation_stale_after_seconds < minimum:
            raise ValueError(
                "reservation_stale_after_seconds must be at least twice "
                f"inventory_timeout_seconds ({minimum}), got "
                f"{self.reservation_stale_after_seconds}"
            )
        return self

    @model_validator(mode="after")
    def _alert_cannot_precede_the_lease(self) -> Self:
        # An alert for an order that is still inside its first lease window
        # would cry wolf on every normal reconcile pass.
        if self.pending_alert_after_seconds < self.reservation_stale_after_seconds:
            raise ValueError(
                "pending_alert_after_seconds must be at least "
                f"reservation_stale_after_seconds "
                f"({self.reservation_stale_after_seconds}), got "
                f"{self.pending_alert_after_seconds}"
            )
        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
