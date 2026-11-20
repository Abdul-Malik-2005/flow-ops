"""RabbitMQ inbound transport: the topology this service consumes from.

The exchange is owned by its producer (Order), but the queue is owned by its
consumer: only this process knows which facts it wants, how many messages it can
hold and where a message goes when it is refused. That split is why nothing here
touches the producer's settings: adding a consumer never requires a deploy of
Order.

Decisions and why:

* **Quorum queue.** It replicates through Raft, and it is what makes the broker
  report `x-acquired-count` on every redelivery — the counter the runtime reads to
  stop retrying. `x-delivery-limit` is passed as well, but against
  `rabbitmq:4-management-alpine` it does **not** dead-letter a message that the
  consumer explicitly requeues: without the in-code check a poison message would
  loop forever.
* **One DLX, one DLQ, explicit dead-letter routing key.** Dead letters do not
  inherit the original routing key here: a single key equal to the DLQ name means
  dead letters from this queue cannot leak into another queue that happens to
  share the DLX. The original key stays in the `x-death` header for diagnosis.
* **Allow-list bindings, never `order.#`.** Subscribing to the whole topic would
  make every future event type an implicit input of a consumer that does not
  understand it; an unknown type would then be dead-lettered instead of simply
  not being routed here.
* **The exchange is re-declared, idempotently.** The producer declares it too,
  but the consumer must be able to start first (a fresh environment, a broker
  restart where only the consumer is up). Declaring with the same arguments is a
  no-op; declaring with different ones fails with `PRECONDITION_FAILED` (406),
  which is exactly the drift an operator must fix.

Ack, nack and the commit-before-ack ordering are **not** here: the adapter hands
a message to the runtime and the runtime decides. That keeps this module about
the broker, and makes the decision table testable without a connection.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Final

import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import (
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)

logger = logging.getLogger(__name__)

#: Bound on one connection attempt, handshake included. Matches the producer on
#: purpose: both processes must give up on a stalled broker in the same order of
#: time, or a rollout would look like a hung consumer.
CONNECT_TIMEOUT_SECONDS: Final = 10.0

#: `url` in, a robust connection out. Injected so the adapter is testable without
#: a broker; the default is the real `aio_pika.connect_robust`.
ConnectFactory = Callable[..., Awaitable[AbstractRobustConnection]]

#: What the runtime provides: one message, and the promise that it acknowledges
#: or rejects it. Returning normally without a verdict leaves the message
#: unacknowledged until the channel closes, which is never what we want.
MessageHandler = Callable[[AbstractIncomingMessage], Awaitable[None]]

#: Args the broker understands; spelled out because a typo is not a type error.
QUEUE_TYPE_ARGUMENT: Final = "x-queue-type"
DELIVERY_LIMIT_ARGUMENT: Final = "x-delivery-limit"
DEAD_LETTER_EXCHANGE_ARGUMENT: Final = "x-dead-letter-exchange"
DEAD_LETTER_ROUTING_KEY_ARGUMENT: Final = "x-dead-letter-routing-key"


class RabbitMQEventConsumer:
    """Declares the consumer topology and subscribes to an allow-list of keys.

    One instance is one subscription. It owns its connection and closes it in
    `aclose()`; a reconnection is a fresh `start()`, not a second instance.
    """

    def __init__(
        self,
        *,
        url: str,
        exchange_name: str,
        queue_name: str,
        dead_letter_exchange: str,
        dead_letter_queue: str,
        routing_keys: tuple[str, ...],
        prefetch_count: int,
        max_delivery_count: int,
        connect: ConnectFactory = aio_pika.connect_robust,
        connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
    ) -> None:
        if not routing_keys:
            raise ValueError("a consumer must subscribe to at least one routing key")

        self._url = url
        self._exchange_name = exchange_name
        self._queue_name = queue_name
        self._dead_letter_exchange = dead_letter_exchange
        self._dead_letter_queue = dead_letter_queue
        self._routing_keys = routing_keys
        self._prefetch_count = prefetch_count
        self._max_delivery_count = max_delivery_count
        self._connect = connect
        self._connect_timeout = connect_timeout

        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._queue: AbstractQueue | None = None
        self._consumer_tag: str | None = None

    async def start(self, handler: MessageHandler) -> None:
        """Declare the topology and subscribe; safe to call again after a drop."""
        if (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
            and self._consumer_tag is not None
        ):
            return

        # A half-dead connection is worse than none: closing it here means the
        # caller never has to special-case "connected but not subscribed".
        await self.aclose()

        connection = await self._connect(self._url, timeout=self._connect_timeout)
        self._connection = connection
        channel = await connection.channel()
        self._channel = channel
        await channel.set_qos(prefetch_count=self._prefetch_count)

        exchange = await channel.declare_exchange(
            self._exchange_name,
            ExchangeType.TOPIC,
            durable=True,
        )
        dead_letters = await channel.declare_exchange(
            self._dead_letter_exchange,
            ExchangeType.DIRECT,
            durable=True,
        )
        queue = await channel.declare_queue(
            self._queue_name,
            durable=True,
            arguments=self._queue_arguments(),
        )
        self._queue = queue
        # The DLQ itself is a durable classic queue: it stores what a human must
        # read, and does not need Raft replication to do that.
        dead_letter_queue = await channel.declare_queue(
            self._dead_letter_queue,
            durable=True,
        )
        await dead_letter_queue.bind(
            dead_letters,
            routing_key=self._dead_letter_queue,
        )
        for routing_key in self._routing_keys:
            await queue.bind(exchange, routing_key=routing_key)

        self._consumer_tag = await queue.consume(handler)
        logger.info(
            "consuming %s from %s into %s (prefetch=%s, delivery limit=%s)",
            ", ".join(self._routing_keys),
            self._exchange_name,
            self._queue_name,
            self._prefetch_count,
            self._max_delivery_count,
        )

    async def aclose(self) -> None:
        """Close the connection; never raise.

        Shutdown is not the place to raise: a re-raised close error would mask
        the reason the process stopped, and the broker reaps a half-closed
        connection on its own. The channel and the consumer go with the
        connection.
        """
        connection, self._connection = self._connection, None
        self._channel = None
        self._queue = None
        self._consumer_tag = None
        if connection is None or connection.is_closed:
            return
        try:
            await connection.close()
        except Exception:
            logger.warning(
                "closing the RabbitMQ consumer connection failed; "
                "the broker will reap it",
                exc_info=True,
            )

    def _queue_arguments(self) -> dict[str, object]:
        """Arguments that define the queue's identity, not just its behaviour.

        The broker rejects a redeclaration with different arguments
        (`PRECONDITION_FAILED`), which is deliberate: changing the delivery limit
        means a new queue (or an operator's explicit policy change), never a
        silently different consumer.
        """
        return {
            QUEUE_TYPE_ARGUMENT: "quorum",
            DELIVERY_LIMIT_ARGUMENT: self._max_delivery_count,
            DEAD_LETTER_EXCHANGE_ARGUMENT: self._dead_letter_exchange,
            DEAD_LETTER_ROUTING_KEY_ARGUMENT: self._dead_letter_queue,
        }
