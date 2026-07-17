from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Awaitable
from typing import Any

from relayq.application.execute import Executor
from relayq.application.recover import RecoveryWorker
from relayq.infrastructure.clock import Clock
from relayq.infrastructure.redis_stream import RedisStreamTransport
from relayq.telemetry.metrics import Metrics
from relayq.worker.admission import AdmissionController
from relayq.worker.shutdown import GracefulShutdown

logger = logging.getLogger(__name__)


class WorkerRunner:
    """Async worker loop: polling → semaphore → process → ack.

    Design:
      - Single asyncio task per queue-consumer pair
      - Bounded concurrency per queue via asyncio.Semaphore (bulkhead)
      - Admission control before claiming (backpressure)
      - Graceful shutdown via asyncio.Event

    This is NOT a blocking loop (while True: time.sleep).  It uses
    XREADGROUP with BLOCK, so the Redis connection does the waiting,
    not the CPU.  When no jobs are available, the coroutine suspends
    without burning a thread.

    CWE-400 (Resource Exhaustion): The semaphore bounds the number of
    in-flight jobs per queue.  Without it, a backlog spike could spawn
    thousands of concurrent handler tasks and exhaust memory or file
    descriptors.
    """

    def __init__(
        self,
        transport: RedisStreamTransport,
        executor: Executor,
        metrics: Metrics,
        clock: Clock,
        worker_id: str,
        queue: str,
        handler: Callable[[Job], Awaitable[Any]],
        admission: AdmissionController | None = None,
        recovery: RecoveryWorker | None = None,
        max_concurrency: int = 10,
        poll_timeout_ms: int = 2_000,
        poll_batch: int = 1,
    ):
        self.transport = transport
        self.executor = executor
        self.metrics = metrics
        self.clock = clock
        self.worker_id = worker_id
        self.queue = queue
        self.handler = handler
        self.admission = admission
        self.recovery = recovery
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.poll_timeout = poll_timeout_ms
        self.poll_batch = poll_batch
        self.shutdown = GracefulShutdown()

        # Track in-flight tasks for shutdown coordination
        self._inflight: set[asyncio.Task] = set()

    async def run(self) -> None:
        """Main worker loop.

        Ensures the consumer group exists, then polls for jobs until
        shutdown is requested.

        The loop structure:
          1. Check shutdown event
          2. Wait for semaphore capacity (backpressure within worker)
          3. Check admission controller (backpressure at queue level)
          4. XREADGROUP with BLOCK
          5. For each job: acquire semaphore, spawn handler task
          6. Garbage-collect completed inflight tasks
        """
        await self.transport.ensure_group(self.queue)
        logger.info(
            "Worker %s starting on queue '%s' (max_concurrency=%d)",
            self.worker_id, self.queue, self.semaphore._value,
        )

        # Start the recovery loop for this queue if configured
        recovery_task: asyncio.Task | None = None
        if self.recovery:
            recovery_task = asyncio.create_task(
                self.recovery.recover_loop([self.queue])
            )

        try:
            while not self.shutdown.is_set():
                # ----------------------------------------------------------
                # Admission control — fast-fail before network I/O
                # ----------------------------------------------------------
                if self.admission:
                    try:
                        await self.admission.check(self.queue)
                    except Exception:
                        # Backpressure engaged; sleep briefly before retry
                        await asyncio.sleep(1)
                        continue

                # ----------------------------------------------------------
                # Poll for jobs (blocking read)
                # ----------------------------------------------------------
                try:
                    jobs = await self.transport.read_group(
                        self.queue,
                        self.worker_id,
                        count=self.poll_batch,
                        block_ms=self.poll_timeout,
                    )
                except Exception as exc:
                    logger.error("Poll error on queue '%s': %s", self.queue, exc)
                    await asyncio.sleep(1)
                    continue

                if not jobs:
                    continue

                # ----------------------------------------------------------
                # Process each job with bounded concurrency
                # ----------------------------------------------------------
                for stream, entry_id, job in jobs:
                    # Acquire semaphore before spawning the task
                    await self.semaphore.acquire()

                    task = asyncio.create_task(
                        self._process_job(stream, entry_id, job)
                    )
                    self._inflight.add(task)
                    task.add_done_callback(self._inflight.discard)

                    self.metrics.observe_worker_inflight(
                        self.queue, len(self._inflight)
                    )

        except asyncio.CancelledError:
            # Worker cancelled during shutdown — handle gracefully
            logger.info("Worker loop cancelled for queue '%s'", self.queue)
        finally:
            if recovery_task:
                recovery_task.cancel()
                try:
                    await recovery_task
                except asyncio.CancelledError:
                    pass

            # Wait for in-flight tasks to complete (graceful drain)
            await self.shutdown.drain(self._inflight)
            logger.info("Worker stopped for queue '%s'", self.queue)

    async def _process_job(
        self, stream: str, entry_id: str, job: Any
    ) -> None:
        """Wrap executor.execute with semaphore release."""
        try:
            await self.executor.execute(
                self.queue, entry_id, job, self.handler, self.worker_id
            )
        except asyncio.CancelledError:
            # Task cancelled during shutdown — re-raise
            raise
        except Exception as exc:
            # CWE-754: Unhandled exception in the executor should never
            # crash the worker loop.  Log and move on.
            logger.exception(
                "Unhandled error processing job %s: %s", job.id, exc,
            )
        finally:
            self.semaphore.release()
            self.metrics.observe_worker_inflight(
                self.queue, len(self._inflight)
            )
