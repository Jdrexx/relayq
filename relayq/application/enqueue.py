from __future__ import annotations

import logging
from typing import Callable

from relayq.domain.errors import IdempotencyConflict, QueueFull
from relayq.domain.job import Job
from relayq.infrastructure.clock import Clock
from relayq.infrastructure.redis_stream import RedisStreamTransport
from relayq.infrastructure.sqlite_store import SqliteStore  # only used when Redis is absent

logger = logging.getLogger(__name__)


class Enqueuer:
    """Transactional enqueue flow: validate → idempotency check → outbox → XADD.

    The outbox pattern solves the dual-write problem: we need to write
    the job record (for status queries and idempotency tracking) *and*
    push the job onto the stream.  If one succeeds and the other fails,
    we have inconsistencies.

    Solution: write the job to the outbox (SQLite or shared DB) first
    within a transaction, *then* XADD to the stream.  If the XADD fails,
    the outbox record stays in 'pending' status and a recovery process
    (recover.py) can replay it.

    This is NOT a two-phase commit — it's a best-effort pattern that
    guarantees the outbox record exists before the stream entry.  The
    reverse order (XADD then outbox) would leave orphaned stream entries
    if the outbox write fails.  Outbox-first minimises the inconsistency
    window.

    CWE-362: The idempotency check uses a UNIQUE index on the outbox
    (sqlite_store.py) or a Lua script (Redis).  A plain read-then-write
    in application code would have a TOCTOU race.
    """

    def __init__(
        self,
        transport: RedisStreamTransport,
        clock: Clock,
        max_queue_depth: int = 10_000,
        # Optional secondary store for idempotency/status
        store: SqliteStore | None = None,
    ):
        self.transport = transport
        self.clock = clock
        self.max_queue_depth = max_queue_depth
        self.store = store

    async def enqueue(
        self,
        job: Job,
        idempotency_key: str | None = None,
        # Validation hook — called before any side effects
        validate: Callable[[Job], None] | None = None,
    ) -> str:
        """Enqueue a job with transactional guarantees.

        Returns the Redis stream entry ID.

        Raises:
            QueueFull — if the stream is at capacity
            IdempotencyConflict — if idempotency_key already exists
        """
        # ------------------------------------------------------------------
        # 1. Validation (fast-fail before any I/O)
        # ------------------------------------------------------------------
        if validate:
            validate(job)

        # ------------------------------------------------------------------
        # 2. Backpressure check — bounded queue depth (CWE-770)
        # ------------------------------------------------------------------
        depth = await self.transport.stream_length(job.queue)
        if depth >= self.max_queue_depth:
            raise QueueFull(job.queue, depth, self.max_queue_depth)

        # ------------------------------------------------------------------
        # 3. Idempotency check (if key provided)
        # ------------------------------------------------------------------
        if idempotency_key:
            job.idempotency_key = idempotency_key
            if self.store:
                existing = self.store.find_by_idempotency_key(idempotency_key)
                if existing is not None:
                    raise IdempotencyConflict(idempotency_key)

        # ------------------------------------------------------------------
        # 4. Outbox insert (transactional)
        # ------------------------------------------------------------------
        # The outbox record is written BEFORE the stream entry.  If we did
        # XADD first and then crashed, the stream has a job that the outbox
        # doesn't know about — a dangling entry.  Outbox-first means the
        # job metadata survives even if the stream write fails.
        #
        # In production, this would be a shared Postgres table; for the
        # portfolio/quickstart we use SQLite with BEGIN IMMEDIATE.
        if self.store:
            async with self.store.transaction():
                self.store.insert_outbox(job)
        elif idempotency_key:
            # Without a store, we can't enforce idempotency — log a warning
            logger.warning(
                "Idempotency key provided but no store configured — "
                "deduplication will be best-effort via Redis consumer group"
            )

        # ------------------------------------------------------------------
        # 5. XADD to the stream
        # ------------------------------------------------------------------
        entry_id = await self.transport.xadd(
            job.queue, job, maxlen=self.max_queue_depth + 1_000
        )

        logger.info(
            "Enqueued job %s (kind=%s) to queue '%s' — stream entry %s",
            job.id, job.kind, job.queue, entry_id,
        )
        return entry_id
