from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """Coordinated shutdown: stop polling → wait for inflight → drain.

    We use an asyncio.Event to signal shutdown to the main loop.
    The sequence is:

    1. Set the shutdown event — the polling loop sees it and exits
    2. Cancel inflight tasks with a grace period
    3. Wait up to `timeout` seconds for all tasks to finish
    4. If any tasks remain, log them and continue (they'll be cancelled)

    This is NOT a hard SIGKILL.  Handlers that are mid-execution get a
    chance to finish, up to the timeout.  Handlers that don't respond to
    cancellation (e.g., blocking I/O without asyncio) may be left dangling.

    CWE-754 (Unchecked Error Handling): Without graceful shutdown, killing
    a worker mid-flight orphans every job it was processing.  Those jobs
    sit in the pending list until XAUTOCLAIM (recover.py) picks them up,
    but that can take minutes.  Graceful shutdown reduces the window.

    Design note: We use asyncio.Event rather than a threading.Event or
    global flag because:
      - asyncio.Event is natively awaitable (no busy-waiting)
      - It works across tasks in the same event loop
      - It's fast and doesn't allocate per-check
    """

    def __init__(self, drain_timeout: float = 30.0):
        self._event = asyncio.Event()
        self.drain_timeout = drain_timeout

    def is_set(self) -> bool:
        return self._event.is_set()

    def request(self) -> None:
        """Initiate graceful shutdown."""
        logger.info("Graceful shutdown requested")
        self._event.set()

    async def drain(self, inflight: set[asyncio.Task]) -> None:
        """Wait for in-flight tasks to complete, with a hard timeout.

        Args:
            inflight: A set of asyncio.Task objects currently in progress.
                      This set is mutated by the caller (runner.py) via
                      add_done_callback, so we snapshot it.
        """
        if not inflight:
            return

        logger.info("Draining %d in-flight tasks...", len(inflight))

        # Give tasks a chance to finish on their own
        done, pending = await asyncio.wait(
            inflight.copy(),
            timeout=self.drain_timeout,
            return_when=asyncio.ALL_COMPLETED,
        )

        if pending:
            logger.warning(
                "%d tasks did not complete within drain timeout — cancelling",
                len(pending),
            )
            for task in pending:
                task.cancel()
            # Wait one more time for cancellations to propagate
            await asyncio.wait(pending, timeout=5.0)

        logger.info("Drain complete (%d done, %d cancelled)", len(done), len(pending))

    async def wait_for_shutdown(self) -> None:
        """Block until shutdown is requested (for background tasks)."""
        await self._event.wait()
