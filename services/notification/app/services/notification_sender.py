"""Notification sender: turns recorded inbox rows into delivered notifications.

Guarantees, in order of importance:

* **At-least-once.** A row is marked `sent_at` only after the sender returned, so
  a crash between send and mark re-sends it. `event_id` travels as the
  idempotency key, so a provider (or a future dedupe layer) absorbs the replay.
* **Lease-based retry.** A failed send leaves `sent_at IS NULL`, the claim spent
  one attempt, and the row is invisible until the lease expires — no hot loop over
  a broken provider.
* **No per-aggregate ordering.** Rows are claimed by `occurred_at`; a retried
  notification may overtake a fresh one. Notifications are facts, not commands.

The transport is hidden behind `NotificationSenderPort`: a logger now, an email or
push provider later, chosen by configuration. The sender does not know which — it
only knows that `send()` returning without an exception means "delivered".
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.notifications import NotificationComposer, NotificationMessage
from app.models.notification_inbox import NotificationInbox
from app.repositories.inbox import InboxRepositoryPort, SqlInboxRepository
from app.services.recipient_resolver import (
    LocalRecipientResolver,
    RecipientResolverPort,
)

logger = logging.getLogger(__name__)

#: Upper bound on how stale a *zero* dead-notification backlog can get: the count
#: is re-read at least once every this many passes, so a row that spent its budget
#: on another sender is noticed even by an instance that never failed itself.
DEAD_LETTER_RECHECK_PASSES = 100

#: How often a known-but-unattended dead-notification backlog is repeated in the
#: log. New dead letters are reported immediately; the reminder exists so the
#: backlog is not forgotten, not so it fills the log.
DEAD_LETTER_REMINDER_PASSES = 60


class NotificationSenderPort(Protocol):
    """Transport for one recorded notification.

    Returns `None` on success and **raises** on failure: the sender marks a row
    only after `send()` returned, so a swallowed exception would mark an
    undelivered notification as delivered.

    Two members, because a transport has a lifetime: `aclose()` releases whatever
    the adapter opened. It must be safe to call when nothing was opened — the log
    adapter holds no resources — so the sender never needs to know which adapter
    it was given.
    """

    async def send(
        self,
        *,
        event_id: uuid.UUID,
        message: NotificationMessage,
    ) -> None: ...

    async def aclose(self) -> None: ...


@dataclass(frozen=True, slots=True)
class SendReport:
    """What one pass did — the runtime logs it and `--once` prints it."""

    #: Rows leased by this pass (`attempts` was incremented for each).
    claimed: int = 0
    #: `send()` returned without raising.
    sent: int = 0
    #: `send()` raised; the row stays unsent and retries after the lease.
    failed: int = 0
    #: Sent, but another sender had already marked the row: at-least-once.
    duplicates: int = 0
    #: Rows that spent the whole attempt budget and are now dead letters.
    exhausted: int = 0


class NotificationSender:
    """Delivers claimed rows and marks them only after a successful send."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        sender: NotificationSenderPort,
        batch_size: int,
        lease_seconds: float,
        max_attempts: int,
        composer: NotificationComposer | None = None,
        resolver: RecipientResolverPort | None = None,
        inbox_factory: Callable[
            [AsyncSession],
            InboxRepositoryPort,
        ] = SqlInboxRepository,
    ) -> None:
        self._session_factory = session_factory
        self._sender = sender
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._composer = composer or NotificationComposer()
        self._resolver = resolver or LocalRecipientResolver()
        self._inbox_factory = inbox_factory
        #: Last dead-notification count this instance read; `None` = never read.
        self._dead_letters: int | None = None
        #: Passes since the count was last read — bounds how stale a zero gets.
        self._passes_since_count = 0
        #: Dead letters already reported at ERROR, so growth is what is loud.
        self._reported_dead_letters = 0
        #: Passes since the backlog was last reminded of, at WARNING.
        self._passes_since_reminder = 0

    async def aclose(self) -> None:
        """Release the transport this sender was built with.

        Ownership is the reason this lives here and not in the entry point: the
        sender received the adapter, so the sender is what returns it.
        """
        await self._sender.aclose()

    async def process_batch(self) -> SendReport:
        """Claim, send, mark — in that order, in three short transactions.

        The claim transaction is closed before the first send: a provider call (or
        a blocking log handler) must never run while a transaction — let alone the
        claimed row lock — is still open.
        """
        rows = await self._claim()
        if not rows:
            exhausted = await self._dead_letters_count(had_failures=False)
            self._report_dead_letters(exhausted)
            return SendReport(exhausted=exhausted)

        sent_ids: list[uuid.UUID] = []
        failed = 0
        for row in rows:
            try:
                # Composition and resolution are part of the attempt: a payload
                # without a usable `user_id` is a poison row, and it must spend
                # its budget and dead-letter rather than crash the batch.
                draft = self._composer.compose(row.event_type, row.payload)
                recipient = await self._resolver.resolve(
                    user_id=uuid.UUID(str(row.payload["user_id"])),
                )
                await self._sender.send(
                    event_id=row.event_id,
                    message=NotificationMessage(
                        recipient=recipient,
                        subject=draft.subject,
                        body=draft.body,
                    ),
                )
            except Exception:
                # One bad notification must not stop the batch: the rest are
                # facts customers are waiting for. The failed row keeps
                # `sent_at IS NULL`, so the lease expiry retries it.
                logger.exception(
                    "sending notification for event %s (%s) failed; retried after "
                    "the lease expires (%s/%s attempts)",
                    row.event_id,
                    row.event_type,
                    row.attempts,
                    self._max_attempts,
                )
                failed += 1
            else:
                sent_ids.append(row.event_id)

        duplicates = await self._mark(sent_ids)
        exhausted = await self._dead_letters_count(had_failures=failed > 0)
        self._report_dead_letters(exhausted)

        report = SendReport(
            claimed=len(rows),
            sent=len(sent_ids),
            failed=failed,
            duplicates=duplicates,
            exhausted=exhausted,
        )
        logger.debug("sender pass: %s", report)
        return report

    # ------------------------------------------------------------------
    # Transaction boundaries
    # ------------------------------------------------------------------

    async def _claim(self) -> Sequence[NotificationInbox]:
        """TX1: lease a batch and commit, so the lease outlives the send call.

        A crash right after this commit costs one attempt per row and nothing
        else: the lease expires and the next pass picks the rows up again.
        """
        async with self._session_factory() as session:
            rows = await self._inbox(session).claim_batch(
                limit=self._batch_size,
                lease_seconds=self._lease_seconds,
                max_attempts=self._max_attempts,
            )
            await session.commit()
        return rows

    async def _mark(self, event_ids: Sequence[uuid.UUID]) -> int:
        """TX2: mark delivered rows; returns how many someone else had marked.

        One transaction for the whole batch: the rows are already sent, so a
        rollback here only costs re-delivery, while a commit per row would
        multiply round-trips without adding safety.
        """
        if not event_ids:
            return 0

        duplicates = 0
        async with self._session_factory() as session:
            inbox = self._inbox(session)
            for event_id in event_ids:
                if not await inbox.mark_sent(event_id=event_id):
                    # Another sender marked the row while this one held it past
                    # the lease: the notification was delivered twice. Expected
                    # under at-least-once — the provider deduplicates by id.
                    duplicates += 1
            await session.commit()
        return duplicates

    async def _dead_letters_count(self, *, had_failures: bool) -> int:
        """The dead-notification backlog, re-read only when it can have changed.

        A dead letter is born only from a failed send, so a pass that failed
        nothing and already knew the backlog was empty has nothing to learn from
        `count(*)` — and that count is a real read: the partial index covers
        `sent_at IS NULL` but not `attempts`, so it walks the whole unsent
        backlog. Asking it on every idle pass made the sender's quietest moment
        its most expensive one.
        """
        known = self._dead_letters
        if (
            known is None
            or known
            or had_failures
            or self._passes_since_count >= DEAD_LETTER_RECHECK_PASSES
        ):
            async with self._session_factory() as session:
                known = await self._inbox(session).count_exhausted_unsent(
                    max_attempts=self._max_attempts,
                )
            self._dead_letters = known
            self._passes_since_count = 0
        else:
            self._passes_since_count += 1
        return known

    def _inbox(self, session: AsyncSession) -> InboxRepositoryPort:
        return self._inbox_factory(session)

    def _report_dead_letters(self, exhausted: int) -> None:
        """Report the dead-notification backlog: loudly when it grows, rarely after.

        A dead notification needs a human, so it is reported at ERROR the moment
        it appears — and only then. Repeating the same ERROR on every pass would
        make the sender's noisiest state its steady state, which is how a real
        incident gets lost in a log.
        """
        if not exhausted:
            self._reported_dead_letters = 0
            self._passes_since_reminder = 0
            return

        if exhausted > self._reported_dead_letters:
            self._reported_dead_letters = exhausted
            self._passes_since_reminder = 0
            logger.error(
                "%s notification(s) exhausted %s send attempts and will not be "
                "retried; inspect notification_inbox where sent_at is null",
                exhausted,
                self._max_attempts,
            )
            return

        # Shrunk (an operator cleaned some), unchanged, or both at once: keep the
        # remembered size in sync so the next growth is recognised as growth.
        self._reported_dead_letters = exhausted
        self._passes_since_reminder += 1
        if self._passes_since_reminder < DEAD_LETTER_REMINDER_PASSES:
            return
        self._passes_since_reminder = 0
        logger.warning(
            "%s notification(s) are still dead letters; nothing will retry them",
            exhausted,
        )
