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

    # Outbox relay (`python -m app.outbox_relay`): a second process that publishes
    # committed events. It never holds a transaction across the publish call, so
    # `locked_at` is the only thing that tells a *busy* relay apart from a *dead*
    # one. A lease shorter than a pass lets another relay steal rows the first one
    # is still publishing; hence the `lease >= 2 * interval` rule below.
    outbox_relay_interval_seconds: float = Field(default=5.0, gt=0)
    outbox_batch_size: int = Field(default=50, ge=1, le=500)
    outbox_lease_seconds: float = Field(default=30.0, gt=0)
    # After this many failed publish attempts the row drops out of the claim
    # predicate and surfaces as an error log. It is never deleted: a consumer
    # that missed an event must still be able to be repaired by hand.
    outbox_max_attempts: int = Field(default=10, ge=1)

    # Event transport: the relay publishes through a *port*, so switching an
    # adapter changes nothing about claiming, marking or the report — only the
    # place the event leaves from. `log` is the default because it needs no
    # infrastructure; `rabbitmq` is switched on together with the first consumer
    # queue, because a topic exchange with no bindings returns every message as
    # unroutable and the publisher treats that as a failed publish on purpose.
    outbox_publisher: Literal["log", "rabbitmq"] = "log"
    rabbitmq_url: str = ""
    # One exchange per producer service: `order` owns its stream, so a new event
    # type or a retired exchange does not touch anyone else's routing. The
    # routing key is the event type itself (`order.reserved`), which lets a
    # consumer subscribe to a single fact or to all of them (`order.#`).
    outbox_exchange: str = "order.events"

    # Event consumer (`python -m app.event_consumer`): a separate process that
    # reads the exchange this service publishes to and journals every delivery.
    # The queue belongs to its consumer — the producer declares only the
    # exchange — so its names live here and not next to `outbox_exchange`.
    #
    # The queue is declared as a quorum queue: it replicates through Raft, and
    # `x-delivery-limit` makes the broker move a message to the DLQ after N
    # redeliveries. That is stronger than a hand-rolled counter in the callback:
    # the broker's count survives a consumer crash and counts messages that never
    # reach the code at all.
    event_consumer_queue: str = "order.event-projections"
    event_consumer_dlx_exchange: str = "order.events.dlx"
    event_consumer_dlq: str = "order.event-projections.dlq"
    # Unacknowledged messages the broker may push at once. The callback journals
    # one message at a time; the buffer saves round trips without letting a dead
    # database lose more than one prefetch window of messages.
    event_consumer_prefetch_count: int = Field(default=10, ge=1, le=1000)
    # Deliveries this consumer accepts before refusing a message without requeue,
    # which sends it to the DLQ. The count is the broker's own
    # (`x-acquired-count`): RabbitMQ 4 does not apply `x-delivery-limit` to an
    # explicit requeue, so the runtime checks the header itself. A deterministic
    # refusal (bad JSON, unknown type) is dead-lettered on the first delivery.
    event_consumer_max_delivery_count: int = Field(default=5, ge=1)
    # How often the consumer logs what it has done since the previous report.
    # New rows, duplicates, retries, refusals and dead letters are counted
    # separately: a duplicate is normal under at-least-once, while a growing
    # `refused` or `dead_lettered` is an incident. Per-event lines stay at DEBUG,
    # so a busy queue does not turn its steady state into a log flood.
    event_consumer_report_interval_seconds: float = Field(default=60.0, gt=0)
    # Pause after a transient failure before the message is redelivered. Without
    # it a database outage turns `nack(requeue=True)` into a hot loop that burns
    # the whole delivery budget in milliseconds. The wait is interruptible, so
    # shutdown is not delayed by it.
    event_consumer_requeue_delay_seconds: float = Field(default=2.0, ge=0)
    # How long to wait before retrying the connection when the broker is not
    # reachable at start-up. `connect_robust` restores a *dropped* connection by
    # itself; this covers the first attempt.
    event_consumer_reconnect_delay_seconds: float = Field(default=5.0, gt=0)
    # Deliberately no validator requires `rabbitmq_url` for these: the API builds
    # the same `Settings` and must keep working without a broker. The consumer
    # entry point checks the address itself, before opening a connection.

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

    @model_validator(mode="after")
    def _outbox_lease_outlives_a_pass(self) -> Self:
        # A relay claims a batch, commits, publishes and only then marks. If the
        # lease were shorter than this cycle, a second relay would reclaim rows
        # the first one is still publishing and both would send them. Two
        # intervals give a slow pass room to finish before its lease expires.
        minimum = 2 * self.outbox_relay_interval_seconds
        if self.outbox_lease_seconds < minimum:
            raise ValueError(
                "outbox_lease_seconds must be at least twice "
                f"outbox_relay_interval_seconds ({minimum}), got "
                f"{self.outbox_lease_seconds}"
            )
        return self

    @model_validator(mode="after")
    def _broker_transport_needs_an_address(self) -> Self:
        # Fails at start-up on purpose. A relay that boots without an address
        # would claim a batch first and only then discover the problem, burning
        # one attempt of every row in it — the exact budget the retry logic is
        # built to protect.
        if self.outbox_publisher == "rabbitmq":
            missing = [
                name
                for name, value in (
                    ("rabbitmq_url", self.rabbitmq_url),
                    ("outbox_exchange", self.outbox_exchange),
                )
                if not value
            ]
            if missing:
                raise ValueError(
                    f"{' and '.join(missing)} required when "
                    f"outbox_publisher is 'rabbitmq'"
                )
        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
