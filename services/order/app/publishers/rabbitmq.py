"""RabbitMQ transport: one durable topic exchange per producer, AMQP confirms.

Why this shape and not a queue per event type:

* **One exchange, `order.events`, owned by this service.** A producer must not
  need to know who consumes its facts. A new consumer binds a new queue with
  `order.#` (or `order.reserved`) and starts receiving — no deploy of `order`.
  Per-consumer queues would put the consumer list inside the producer.
* **Routing key is the event type** (`order.reserved`), so the topic pattern
  *is* the subscription language; there is no mapping table to keep in sync.
* **Durable exchange, persistent messages, publisher confirms.** The outbox row
  is marked published only after `publish()` returned, so a lost message would
  be silently lost data. A confirm is the only thing that makes the return value
  of `publish()` mean "the broker has it".
* **`mandatory=True` together with a confirm.** This is the pair that makes the
  absence of a consumer a *failure* instead of a shrug: an unroutable message is
  returned by the broker, and the adapter raises, so the row keeps
  `published_at IS NULL` and retries after the lease. Without it, a topic
  exchange with no bindings would swallow events and the outbox would lie.

That last point is why the relay defaults to the `log` transport: until the
first queue is bound, `rabbitmq` would turn every event into a failed publish.

`connect_robust` restores the connection and the channel in the background after
a broker restart, but it does **not** replay publications that failed while it
was down (aio-pika's own docstring says so). That is fine here: the failed call
raises, the row stays unpublished, and the lease brings it back — at-least-once,
which is the guarantee the whole outbox is built around. A message that *was*
delivered but whose confirm was lost is published again, which is why consumers
deduplicate by `event_id`.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Final

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractRobustConnection
from aio_pika.exceptions import DeliveryError, PublishError

from app.models.outbox_event import OutboxEvent
from app.publishers.envelope import encode_event

logger = logging.getLogger(__name__)

#: Bound on one connection attempt, handshake included. Without it a broker that
#: accepts the TCP connection and then stalls would block the relay indefinitely,
#: and the stop event could not interrupt a call that never returns.
CONNECT_TIMEOUT_SECONDS: Final = 10.0

#: Bound on one publish. Chosen well below the default 30 s lease: a publish that
#: silently hangs must fail *within* the pass, not after another relay instance
#: has already taken the row over.
PUBLISH_TIMEOUT_SECONDS: Final = 5.0

#: `url` in, a robust connection out. Injected so the adapter is testable without
#: a broker; the default is the real `aio_pika.connect_robust`.
ConnectFactory = Callable[..., Awaitable[AbstractRobustConnection]]


class EventNotDelivered(RuntimeError):
    """The broker did not confirm the message, or had nowhere to route it.

    Raised for exactly the outcomes that must leave `published_at` NULL: a
    `Basic.Nack`/`Basic.Reject`, a `Basic.Return` (unroutable with
    `mandatory=True`) or a publish that produced no verdict at all.
    """


class RabbitMQEventPublisher:
    """`EventPublisherPort` adapter that publishes to a durable topic exchange.

    The connection is opened lazily, on the first `publish()`: `--once` on an
    empty outbox and the unit tests must not pay for a broker connection, and an
    adapter that connects in `__init__` cannot report a failure to the relay —
    it would surface as a crash during wiring instead of a failed pass.
    """

    def __init__(
        self,
        *,
        url: str,
        exchange: str,
        connect: ConnectFactory = aio_pika.connect_robust,
        connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
        publish_timeout: float = PUBLISH_TIMEOUT_SECONDS,
    ) -> None:
        self._url = url
        self._exchange_name = exchange
        self._connect = connect
        self._connect_timeout = connect_timeout
        self._publish_timeout = publish_timeout
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._exchange: AbstractExchange | None = None
        #: Guards the connect-or-reuse decision. The relay publishes serially
        #: today, but two concurrent publishes must not open two connections.
        self._lock = asyncio.Lock()

    async def publish(self, *, event: OutboxEvent) -> None:
        exchange = await self._ready_exchange()
        message = Message(
            body=encode_event(event).encode("utf-8"),
            content_type="application/json",
            content_encoding="utf-8",
            # Persistent + durable exchange: the message survives a broker
            # restart, which is the whole point of paying for the broker.
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=str(event.id),
            type=event.event_type,
            timestamp=event.occurred_at,
            headers={"aggregate_id": str(event.aggregate_id)},
        )

        try:
            verdict = await exchange.publish(
                message,
                # The routing key *is* the contract: a consumer binds
                # `order.reserved` for one fact or `order.#` for all of them.
                routing_key=event.event_type,
                mandatory=True,
                timeout=self._publish_timeout,
            )
        # `PublishError` must come first: it subclasses `DeliveryError`, so the
        # broader clause would swallow every unroutable message and report it as a
        # rejection — the wrong diagnosis for the operator, and the "no queue is
        # bound" case is the one that must be loud.
        except PublishError as exc:
            # `Basic.Return`: nothing is bound to this routing key. With
            # `on_return_raises` the channel turns the return into an exception.
            raise EventNotDelivered(
                f"event {event.id} ({event.event_type}) is unroutable on "
                f"exchange {self._exchange_name!r}: no queue is bound to "
                f"{event.event_type!r}"
            ) from exc
        except DeliveryError as exc:
            # `Basic.Nack`/`Basic.Reject`: the broker refused the message.
            raise EventNotDelivered(
                f"broker rejected event {event.id} ({event.event_type}): {exc}"
            ) from exc

        if verdict is None:
            # No confirm frame: treating silence as success is how an outbox
            # starts losing events.
            raise EventNotDelivered(
                f"broker sent no confirmation for event {event.id} ({event.event_type})"
            )

    async def aclose(self) -> None:
        """Close the connection (its channels go with it); never raise.

        A failure to close is logged, not propagated: shutdown is not the place
        to raise, and a re-raised close error would skip disposing the engine and
        mask the reason the relay stopped.
        """
        connection, self._connection = self._connection, None
        self._channel = None
        self._exchange = None
        if connection is None or connection.is_closed:
            return
        try:
            await connection.close()
        except Exception:
            logger.warning(
                "closing the RabbitMQ connection failed; the broker will reap it",
                exc_info=True,
            )

    async def _ready_exchange(self) -> AbstractExchange:
        """Return a declared exchange, connecting first if there is none."""
        if self._exchange is not None and not self._channel.is_closed:
            return self._exchange

        async with self._lock:
            # Re-check: a waiter on the lock must not open a second connection
            # that the winner has just made.
            if self._exchange is not None and not self._channel.is_closed:
                return self._exchange

            connection = self._connection
            if connection is None or connection.is_closed:
                connection = await self._connect(
                    self._url, timeout=self._connect_timeout
                )
                self._connection = connection
                logger.info("connected to RabbitMQ at %s", _redact(self._url))

            channel = self._channel
            if channel is None or channel.is_closed:
                # `publisher_confirms=True` (the default, spelled out because the
                # guarantee depends on it) makes `publish()` await `Basic.Ack`.
                # `on_return_raises=True` turns an unroutable message into
                # `PublishError` instead of a `DeliveredMessage` return value.
                channel = await connection.channel(
                    publisher_confirms=True,
                    on_return_raises=True,
                )
                self._channel = channel
                # Re-declared on every new channel: `durable=True` is what keeps
                # the exchange across a broker restart, and declaring is
                # idempotent, so a reconnect costs one round trip and no state.
                self._exchange = await channel.declare_exchange(
                    self._exchange_name,
                    ExchangeType.TOPIC,
                    durable=True,
                )

            assert self._exchange is not None  # declared just above
            return self._exchange


def _redact(url: str) -> str:
    """Hide the password of an `amqp://user:secret@host` URL before logging it.

    The user stays visible: "which identity failed to connect" is the first
    question when the broker refuses a login, and a login is not the secret —
    the password is. A URL without userinfo is returned untouched, so a
    `amqp://localhost:5672` (where the "userinfo" is really a port) is not
    mangled.
    """
    scheme, separator, rest = url.partition("://")
    userinfo, at, host = rest.rpartition("@")
    user, colon, _ = userinfo.partition(":")
    if not at or not colon:
        return url
    prefix = f"{scheme}{separator}" if scheme else ""
    return f"{prefix}{user}:***@{host}"
