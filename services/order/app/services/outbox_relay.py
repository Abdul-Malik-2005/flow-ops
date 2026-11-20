"""Transactional outbox relay: moves committed events out of the database.

Guarantees, in order of importance:

* **At-least-once.** An event is marked delivered only after the publisher
  returned, so a crash between publish and mark re-delivers it. Consumers dedupe
  by `event_id`; the relay never promises exactly-once.
* **Lease-based retry.** A failed publish leaves `published_at IS NULL`, the
  claim spent one attempt, and the row is invisible until the lease expires — no
  hot loop over a broken transport.
* **No per-aggregate ordering.** Rows are claimed by `available_at`, a retried
  event may overtake a fresh one. Events carry the aggregate id, not a version.

The transport is hidden behind `EventPublisherPort`: a logger or a broker with
publisher confirms, chosen by configuration. The relay does not know which — it
only knows that `publish()` returning without an exception means "delivered".
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.outbox_event import OutboxEvent
from app.repositories.outbox import OutboxRepositoryPort, SqlOutboxRepository

logger = logging.getLogger(__name__)

#: Upper bound on how stale a *zero* dead-letter backlog can get: the count is
#: re-read at least once every this many passes, so a dead letter created by
#: another relay instance is noticed even by an instance that never fails itself.
DEAD_LETTER_RECHECK_PASSES = 100

#: How often a known-but-unattended dead-letter backlog is repeated in the log.
#: New dead letters are reported immediately; the reminder exists so the backlog
#: is not forgotten, not so it fills the log — at a 5 s interval an ERROR per
#: pass is 12 lines a minute for a condition a human has to fix in minutes.
DEAD_LETTER_REMINDER_PASSES = 60


class EventPublisherPort(Protocol):
    """Transport for one committed event.

    Returns `None` on success and **raises** on failure: the relay marks a row
    only after `publish()` returned, so a swallowed exception would mark an
    undelivered event as delivered.

    Two members, because a transport has a lifetime: `aclose()` releases whatever
    the adapter opened (a broker connection, a channel). It must be safe to call
    when nothing was opened — the log adapter holds no resources — so the relay
    never needs to know which adapter it was given.
    """

    async def publish(self, *, event: OutboxEvent) -> None: ...

    async def aclose(self) -> None: ...


@dataclass(frozen=True, slots=True)
class RelayReport:
    """What one pass did — the runtime logs it and `--once` prints it."""

    #: Events leased by this pass (`attempts` was incremented for each).
    claimed: int = 0
    #: `publish()` returned without raising.
    published: int = 0
    #: `publish()` raised; the row stays unpublished and retries after the lease.
    failed: int = 0
    #: Published, but another relay had already marked the row: at-least-once.
    duplicates: int = 0
    #: Rows that spent the whole attempt budget and are now dead letters.
    exhausted: int = 0


class OutboxRelay:
    """Publishes claimed events and marks them only after a successful publish."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisherPort,
        batch_size: int,
        lease_seconds: float,
        max_attempts: int,
        outbox_factory: Callable[
            [AsyncSession],
            OutboxRepositoryPort,
        ] = SqlOutboxRepository,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._outbox_factory = outbox_factory
        #: Last dead-letter count this instance read; `None` = never read.
        self._dead_letters: int | None = None
        #: Passes since the count was last read — bounds how stale a zero gets.
        self._passes_since_count = 0
        #: Dead letters already reported at ERROR, so growth is what is loud.
        self._reported_dead_letters = 0
        #: Passes since the backlog was last reminded of, at WARNING.
        self._passes_since_reminder = 0

    async def aclose(self) -> None:
        """Release the transport this relay was built with.

        Ownership is the reason this lives here and not in the entry point: the
        relay received the adapter, so the relay is what returns it. A broker
        connection left open keeps the event loop alive past the container's
        `stop_grace_period`, and the process gets SIGKILLed instead of stopping
        cleanly — the difference between "in flight, retried after the lease"
        and "state nobody can explain".
        """
        await self._publisher.aclose()

    async def process_batch(self) -> RelayReport:
        """Claim, publish, mark — in that order, in three short transactions.

        The claim transaction is closed before the first publish: a network call
        (or a blocking log handler) must never run while a transaction — let
        alone the claimed row lock — is still open.
        """
        events = await self._claim()
        if not events:
            exhausted = await self._dead_letters_count(had_failures=False)
            self._report_dead_letters(exhausted)
            return RelayReport(exhausted=exhausted)

        published_ids: list[uuid.UUID] = []
        failed = 0
        for event in events:
            try:
                await self._publisher.publish(event=event)
            except Exception:
                # One bad event must not stop the batch: the rest are real facts
                # consumers are waiting for. The failed row keeps
                # `published_at IS NULL`, so the lease expiry retries it.
                logger.exception(
                    "publishing event %s (%s) failed; retried after the lease "
                    "expires (%s/%s attempts)",
                    event.id,
                    event.event_type,
                    event.attempts,
                    self._max_attempts,
                )
                failed += 1
            else:
                published_ids.append(event.id)

        duplicates = await self._mark(published_ids)
        exhausted = await self._dead_letters_count(had_failures=failed > 0)
        self._report_dead_letters(exhausted)

        report = RelayReport(
            claimed=len(events),
            published=len(published_ids),
            failed=failed,
            duplicates=duplicates,
            exhausted=exhausted,
        )
        logger.debug("relay pass: %s", report)
        return report

    # ------------------------------------------------------------------
    # Transaction boundaries
    # ------------------------------------------------------------------

    async def _claim(self) -> Sequence[OutboxEvent]:
        """TX1: lease a batch and commit, so the lease outlives the publish call.

        A crash right after this commit costs one attempt per row and nothing
        else: the lease expires and the next pass picks the rows up again.
        """
        async with self._session_factory() as session:
            events = await self._outbox(session).claim_batch(
                limit=self._batch_size,
                lease_seconds=self._lease_seconds,
                max_attempts=self._max_attempts,
            )
            await session.commit()
        return events

    async def _mark(self, event_ids: Sequence[uuid.UUID]) -> int:
        """TX2: mark delivered rows; returns how many someone else had marked.

        One transaction for the whole batch: the rows are already published, so
        a rollback here only costs re-delivery, while a commit per event would
        multiply round-trips without adding safety.
        """
        if not event_ids:
            return 0

        duplicates = 0
        async with self._session_factory() as session:
            outbox = self._outbox(session)
            for event_id in event_ids:
                if not await outbox.mark_published(event_id=event_id):
                    # Another relay marked the row while this one held it past
                    # the lease: the event was delivered twice. Expected under
                    # at-least-once — consumers dedupe by `event_id`.
                    duplicates += 1
            await session.commit()
        return duplicates

    async def _dead_letters_count(self, *, had_failures: bool) -> int:
        """The dead-letter backlog, re-read only when it can have changed.

        A dead letter is born only from a failed publish, so a pass that failed
        nothing and already knew the backlog was empty has nothing to learn from
        `count(*)` — and that count is a real read: the partial index covers
        `published_at IS NULL` but not `attempts`, so it walks the whole
        unpublished backlog. Asking it on every idle pass made the relay's
        quietest moment its most expensive one.

        Three cases force a re-read:

        * the count was never read (a fresh instance, or `--once`, must answer
          correctly on its first pass);
        * the backlog is **non-zero** — it has to fall back to zero once a human
          cleans it up, otherwise the warning below would never go quiet;
        * the pass failed something — that is how a dead letter is normally born.

        Between those, a zero is trusted for at most `DEAD_LETTER_RECHECK_PASSES`
        passes, which bounds how long a dead letter created by *another* instance
        can stay invisible to this one.
        """
        known = self._dead_letters
        if (
            known is None
            or known
            or had_failures
            or self._passes_since_count >= DEAD_LETTER_RECHECK_PASSES
        ):
            async with self._session_factory() as session:
                known = await self._outbox(session).count_exhausted_unpublished(
                    max_attempts=self._max_attempts,
                )
            self._dead_letters = known
            self._passes_since_count = 0
        else:
            self._passes_since_count += 1
        return known

    def _outbox(self, session: AsyncSession) -> OutboxRepositoryPort:
        return self._outbox_factory(session)

    def _report_dead_letters(self, exhausted: int) -> None:
        """Report the dead-letter backlog: loudly when it grows, rarely after.

        A dead letter needs a human, so it is reported at ERROR the moment it
        appears — and only then. Repeating the same ERROR on every pass would
        make the relay's noisiest state its steady state, which is how a real
        incident gets lost in a log. While the backlog stays put, it is reminded
        at WARNING once every `DEAD_LETTER_REMINDER_PASSES` passes.
        """
        if not exhausted:
            self._reported_dead_letters = 0
            self._passes_since_reminder = 0
            return

        if exhausted > self._reported_dead_letters:
            self._reported_dead_letters = exhausted
            self._passes_since_reminder = 0
            logger.error(
                "%s outbox event(s) exhausted %s publish attempts and will not be "
                "retried; inspect outbox_events where published_at is null",
                exhausted,
                self._max_attempts,
            )
            return

        # Shrunk (an operator deleted some), unchanged, or both at once: keep the
        # remembered size in sync so the next growth is recognised as growth.
        self._reported_dead_letters = exhausted
        self._passes_since_reminder += 1
        if self._passes_since_reminder < DEAD_LETTER_REMINDER_PASSES:
            return
        self._passes_since_reminder = 0
        logger.warning(
            "%s outbox event(s) are still dead letters; nothing will retry them",
            exhausted,
        )
