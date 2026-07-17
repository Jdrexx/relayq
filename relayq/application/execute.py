from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Awaitable
from typing import Any

from relayq.domain.errors import JobTimeout, CircuitOpen
from relayq.domain.job import Job, JobStatus
from relayq.domain.retry import RetryPolicy
from relayq.infrastructure.clock import Clock
from relayq.infrastructure.redis_stream import RedisStreamTransport
from relayq.infrastructure.sqlite_store import SqliteStore
from relayq.telemetry.metrics import Metrics

logger = logging.getLogger(__name__)


class Executor:
    """Worker execution: dequeue → admit → run handler → ack/DLQ.

    This is the core of the worker — it takes a job from the transport,
    runs the user-provided handler, and either acknowledges success
    (XACK) or routes to the dead-letter queue on failure.

    Three crash scenarios are explicitly handled:

    1. Crash *before* handler:
       - Stream entry stays in Pending list
       - XAUTOCLAIM in recover.py reassigns to another worker
       - At-least-once: another worker will retry

    2. Crash *after handler succeeds* but *before XACK*:
       - Same as above: entry stays pending, gets reclaimed
       - Handler already ran — side effects fired twice
       - This is why we need IDEMPOTENCY KEYS on the consumer side
       - CWE-754: unchecked error handling would let this silently
         duplicate side effects

    3. Crash *after XACK* but *before outbox status update*:
       - Stream entry acknowledged (won't be redelivered)
       - Outbox says 'processing' forever
       - Recovery process should check for this inconsistency
       - Rare, but possible if SQLite outbox write fails after XACK

    We do NOT claim exactly-once.  At-least-once is the honest contract.
    """

    def __init__(
        self,
        transport: RedisStreamTransport,
        clock: Clock,
        metrics: Metrics,
        retry_policy: RetryPolicy | None = None,
        store: SqliteStore | None = None,
        job_timeout_seconds: float = 300.0,
    ):
        self.transport = transport
        self.clock = clock
        self.metrics = metrics
        self.retry_policy = retry_policy or RetryPolicy()
        self.store = store
        self.job_timeout = job_timeout_seconds

    async def execute(
        self,
        queue: str,
        entry_id: str,
        job: Job,
        handler: Callable[[Job], Awaitable[Any]],
        worker_id: str,
    ) -> None:
        """Execute a single job — run handler, ack on success, DLQ on failure.

        This method is the retry/ack/DLQ decision point.  It does NOT
        catch asyncio.CancelledError — the caller (runner.py) is responsible
        for cancellation handling during graceful shutdown.
        """
        # ------------------------------------------------------------------
        # Mark as processing in the outbox
        # ------------------------------------------------------------------
        if self.store:
            try:
                self.store.update_status(job.id, JobStatus.PROCESSING, attempt=job.attempts)
            except Exception:
                logger.warning("Failed to update outbox status for %s (non-fatal)", job.id)

        # ------------------------------------------------------------------
        # Run the handler with a timeout (CWE-400)
        # ------------------------------------------------------------------
        start = self.clock.monotonic()
        try:
            async with asyncio.timeout(self.job_timeout):
                await handler(job)
        except asyncio.TimeoutError:
            elapsed = self.clock.monotonic() - start
            logger.error("Job %s timed out after %.2fs", job.id, elapsed)
            self.metrics.observe_processing_seconds(job.kind, elapsed)
            await self._handle_failure(queue, entry_id, job, JobTimeout(job.id, self.job_timeout))
            return
        except Exception as exc:
            elapsed = self.clock.monotonic() - start
            logger.exception("Job %s failed: %s", job.id, exc)
            self.metrics.observe_processing_seconds(job.kind, elapsed)
            await self._handle_failure(queue, entry_id, job, exc)
            return

        # ------------------------------------------------------------------
        # Success path: XACK + update outbox
        # ------------------------------------------------------------------
        elapsed = self.clock.monotonic() - start
        self.metrics.observe_processing_seconds(job.kind, elapsed)

        try:
            await self.transport.xack(queue, entry_id)
        except Exception as exc:
            # CWE-754: XACK failure doesn't mean the job failed — the
            # handler already ran.  The entry will be reclaimed by
            # XAUTOCLAIM and re-executed (at-least-once).  Log and move on.
            logger.error(
                "XACK failed for job %s (entry %s) — will be reclaimed: %s",
                job.id, entry_id, exc,
            )

        if self.store:
            try:
                self.store.update_status(job.id, JobStatus.COMPLETED)
            except Exception:
                logger.warning("Failed to update outbox for completed job %s", job.id)

        self.metrics.incr_delivery_attempts(job.kind, success=True)
        logger.info("Job %s completed successfully in %.2fs", job.id, elapsed)

    async def _handle_failure(
        self, queue: str, entry_id: str, job: Job, exc: Exception
    ) -> None:
        """Route a failed job to retry or DLQ."""
        job.attempts += 1
        self.metrics.incr_delivery_attempts(job.kind, success=False)

        if job.should_dlq():
            # --------------------------------------------------------------
            # Exhausted retries — route to dead-letter queue
            # --------------------------------------------------------------
            reason = f"{type(exc).__name__}: {exc}"
            try:
                await self.transport.xadd_dlq(queue, job, reason)
                await self.transport.xack(queue, entry_id)
                self.metrics.incr_dead_letter(job.kind)
                logger.warning(
                    "Job %s routed to DLQ after %d attempts — %s",
                    job.id, job.attempts, reason,
                )
            except Exception as dlq_exc:
                # CWE-703: Even the DLQ write can fail (Redis down).
                # The job stays in the pending list and will be
                # reclaimed later.
                logger.error(
                    "Failed to write job %s to DLQ: %s", job.id, dlq_exc,
                )

            if self.store:
                try:
                    self.store.update_status(job.id, JobStatus.DEAD)
                except Exception:
                    logger.warning("Failed to update outbox for dead job %s", job.id)
        else:
            # --------------------------------------------------------------
            # Still has retries — leave in pending, don't XACK
            # The stream entry stays in the pending list; XAUTOCLAIM
            # or the next xreadgroup will redeliver it.
            # --------------------------------------------------------------
            delay = self.retry_policy.delay(job.attempts - 1)  # attempt already incremented
            logger.info(
                "Job %s failed (attempt %d/%d), retrying in %.2fs",
                job.id, job.attempts, job.max_retries + 1, delay,
            )

            if self.store:
                try:
                    self.store.update_status(job.id, JobStatus.FAILED, attempt=job.attempts)
                except Exception:
                    logger.warning("Failed to update outbox for failed job %s", job.id)

            # We intentionally do NOT sleep here — the delay is enforced
            # by the fact that we don't XACK, so the job reappears after
            # the consumer's idle timeout (min_idle_ms in autoclaim).
            # If we need strict per-retry delays, use a scheduled retry
            # stream; for the portfolio, Redis's idle timeout is sufficient.
