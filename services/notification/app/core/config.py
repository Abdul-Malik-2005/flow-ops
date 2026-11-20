from typing import Literal, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the Notification service.

    Notification owns only its own database. Every fact it needs arrives over the
    broker from Order, so there are no foreign credentials or HTTP clients here:
    the service is a pure consumer that records what it was told.
    """

    database_url: str
    test_database_url: str | None = None

    debug: bool = False
    environment: Literal["development", "testing", "production"] = "development"

    # Event transport: the exchange is owned by its producer (Order). A consumer
    # only reads it and re-declares it with identical arguments so it can start
    # before the producer.
    rabbitmq_url: str = ""
    order_events_exchange: str = "order.events"

    # The queue belongs to its consumer, never to the producer. Order declares
    # only the exchange, so a new consumer appears here without a deploy of Order
    # and without Order knowing who listens.
    event_consumer_queue: str = "notification.order-events"
    # Notification's own dead-letter topology, deliberately separate from
    # Order's: a dead letter from this queue must not land in another service's
    # DLQ just because both share a dead-letter exchange.
    event_consumer_dlx_exchange: str = "notification.order-events.dlx"
    event_consumer_dlq: str = "notification.order-events.dlq"
    # Unacknowledged messages the broker may push at once. The callback records
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
    # `refused` or `dead_lettered` is an incident. Per-event lines stay at DEBUG.
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

    # Deliberately no validator requires `rabbitmq_url`: `Settings` is also built
    # by anything that just wants the database URL, and only the consumer entry
    # point needs a broker. It checks the address itself, before connecting.

    # Sender (`python -m app.sender`): the second process of this service. It
    # reads recorded inbox rows, delivers each one, and marks `sent_at` only after
    # the provider returned. Delivery is at-least-once; `event_id` is the
    # idempotency key handed to the provider, so a replay is deduplicated there as
    # well. Like the outbox relay it never holds a transaction across the send, so
    # `locked_at` is the only thing that tells a *busy* sender from a *dead* one.
    sender_interval_seconds: float = Field(default=5.0, gt=0)
    sender_batch_size: int = Field(default=50, ge=1, le=500)
    # A lease shorter than a pass lets another sender steal rows the first one is
    # still sending; hence the `lease >= 2 * interval` rule below.
    sender_lease_seconds: float = Field(default=30.0, gt=0)
    # Attempts per notification; then the row leaves the feed and surfaces as an
    # error log. It is never deleted: a missed notification stays repairable.
    sender_max_attempts: int = Field(default=10, ge=1)
    # Which adapter the sender builds behind `NotificationSenderPort`. `log` needs
    # no infrastructure (the code default, and what pytest/locally uses); `smtp`
    # delivers real email and is switched on together with a mail server.
    sender_transport: Literal["log", "smtp"] = "log"
    # Read only when `sender_transport` is `smtp`. Dev points them at Mailpit
    # (localhost:1025); a real deployment points them at its mail relay.
    smtp_host: str = ""
    smtp_port: int = Field(default=1025, ge=1, le=65535)
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_from: str = ""
    smtp_timeout_seconds: float = Field(default=10.0, gt=0)
    smtp_use_tls: bool = False
    # Dev recipient domain: `LocalRecipientResolver` turns a `user_id` into
    # `<user_id>@<domain>`. The real resolver will call Identity instead.
    recipient_domain: str = "flowops.local"

    @model_validator(mode="after")
    def _sender_lease_outlives_a_pass(self) -> Self:
        # A pass has to finish before its own lease expires, otherwise a second
        # sender reclaims rows the first one is still delivering and both send.
        minimum = 2 * self.sender_interval_seconds
        if self.sender_lease_seconds < minimum:
            raise ValueError(
                "sender_lease_seconds must be at least twice "
                f"sender_interval_seconds ({minimum}), got "
                f"{self.sender_lease_seconds}"
            )
        return self

    @model_validator(mode="after")
    def _smtp_transport_needs_a_host_and_sender(self) -> Self:
        # Fails at start-up on purpose. A sender that boots into `smtp` without an
        # address would claim a batch first and only then fail every row, burning
        # the attempt budget the lease is meant to protect.
        if self.sender_transport == "smtp":
            missing = [
                name
                for name, value in (
                    ("smtp_host", self.smtp_host),
                    ("smtp_from", self.smtp_from),
                )
                if not value
            ]
            if missing:
                raise ValueError(
                    f"{' and '.join(missing)} required when sender_transport is 'smtp'"
                )
        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
