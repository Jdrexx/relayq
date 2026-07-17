from __future__ import annotations

import asyncio
import logging

from relayq.domain.job import Job
from relayq.infrastructure.redis_stream import RedisStreamTransport
from relayq.infrastructure.clock import Clock

logger = logging.getLogger(__name__)


class RecoveryWorker:
    """Lease recovery using XAUTOCLAIM for stale pending entries.

    In Redis Streams consumer groups, an entry stays in the pending list
    until the consumer XACKs it.  If a consumer crashes without XACKing,
    the entry is orphaned — it'll never be processed.

    XAUTOCLAIM is Redis 6.2+'s answer.  It reclaims pending entries
    that have been idle longer than min_idle_ms and reassigns them to
    the calling consumer.  The reclaim is atomic and doesn't interfere
    with other consumers.

    This is the distributed analogue of a lease timeout in traditional
    job queues (e.g., Celery's task expiry, Sidekiq's job reservation).

    CWE-754 (Unchecked Error Handling): Without lease recovery, a single
    crashed worker can stall an entire queue permanently.  This loop
    runs periodically (every CLAIM_INTERVAL) to detect and resolve such
    stalls.
    """

    CLAIM_INTERVAL = 15  # seconds between claim rounds

    def __init__(
        self,
        transport: RedisStreamTransport,
        clock: Clock,
        worker_id: str,
        min_idle_ms: int = 30_000,  # 30 seconds
        claim_batch: int = 10,
    ):
        self.transport = transport
        self.clock = clock
        self.worker_id = worker_id
        self.min_idle_ms = min_idle_ms
        self.claim_batch = claim_batch
        self._running = False

    async def recover_loop(self, queues: list[str]) -> None:
        """Periodically scan queues for stale pending entries.

        This runs as a background task alongside the main worker loop.
        It's intentionally simple: for each queue, call XAUTOCLAIM for
        entries idle longer than min_idle_ms.

        In production you'd want:
          - A backoff for queues that consistently have nothing to claim
          - A circuit breaker per queue if autoclaim keeps failing
          - Prometheus metrics for claimed entry count
        """
        self._running = True
        logger.info(
            "Recovery worker %s started (interval=%ds, min_idle=%dms)",
            self.worker_id, self.CLAIM_INTERVAL, self.min_idle_ms,
        )
        while self._running:
            for queue in queues:
                try:
                    await self._claim_stale(queue)
                except Exception as exc:
                    # CWE-703: Don't let one queue's failure crash the
                    # whole recovery loop.  Log and move on.
                    logger.error(
                        "Recovery claim failed for queue '%s': %s",
                        queue, exc,
                    )
            await asyncio.sleep(self.CLAIM_INTERVAL)

    async def _claim_stale(self, queue: str) -> None:
        """Claim stale pending entries for a single queue."""
        claimed = await self.transport.autoclaim_stale(
            queue,
            self.worker_id,
            min_idle_ms=self.min_idle_ms,
            count=self.claim_batch,
        )
        for stream, entry_id, job in claimed:
            logger.info(
                "Recovered stale job %s (entry %s) from queue '%s'",
                job.id, entry_id, queue,
            )

    def stop(self) -> None:
        self._running = False
