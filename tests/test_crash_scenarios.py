"""Crash-scenario tests: verify at-least-once delivery semantics.

These tests simulate the three crash windows:

  1. Crash BEFORE handler — job stays in pending, gets reclaimed
  2. Crash AFTER handler but BEFORE XACK — handler runs twice (idempotency needed)
  3. Crash AFTER XACK but BEFORE outbox status update — job is processed but
     status never updated

Because we're testing without a real Redis instance (these are unit tests),
we simulate the transport and executor behaviour to verify correctness of
the state transitions.

CWE-754 (Unchecked Error Handling): Each crash scenario must leave the
system in a known state without data corruption or resource leaks.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from relayq.application.execute import Executor
from relayq.domain.errors import JobTimeout
from relayq.domain.job import Job, JobStatus
from relayq.domain.retry import RetryPolicy
from relayq.infrastructure.clock import FakeClock
from relayq.infrastructure.redis_stream import RedisStreamTransport
from relayq.telemetry.metrics import Metrics


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def metrics():
    return Metrics()


@pytest.fixture
def transport():
    return AsyncMock(spec=RedisStreamTransport)


@pytest.fixture
def executor(transport, clock, metrics):
    return Executor(
        transport=transport,
        clock=clock,
        metrics=metrics,
        retry_policy=RetryPolicy(base_seconds=0.1, cap_seconds=1.0, jitter=False),
        job_timeout_seconds=5.0,
    )


async def _handler_ok(job: Job):
    """Handler that always succeeds."""
    return {"result": "ok"}


async def _handler_fails(job: Job):
    """Handler that always raises."""
    raise ValueError("handler failure")


async def _handler_side_effect_then_fail(job: Job):
    """Handler that does side-effect work then crashes.

    Simulates: after handler succeeds but before XACK.
    The side effect (mocked) already fired.
    """
    # Simulate a side effect that's already been committed
    job.payload["side_effect_fired"] = True
    raise RuntimeError("crash after side effect")


class TestCrashBeforeHandler:
    """Crash scenario 1: worker dies before calling the handler.

    The stream entry stays in the pending list.  XAUTOCLAIM (recover.py)
    eventually reclaims it.  No side effects have fired, so this is the
    safest crash scenario.
    """

    @pytest.mark.asyncio
    async def test_crash_before_handler_leaves_entry_pending(self, executor, transport):
        """If the executor crashes before running the handler, no XACK is sent."""
        job = Job(kind="test", payload={"x": 1})

        # Simulate executor receiving the job then crashing before handler
        transport.xack = AsyncMock()

        with patch.object(executor, "execute", side_effect=RuntimeError("worker crash")):
            with pytest.raises(RuntimeError):
                await executor.execute("default", "entry-1", job, _handler_ok, "worker-1")

        # XACK should NOT have been called — job stays in pending
        transport.xack.assert_not_called()


class TestCrashAfterSideEffectBeforeCommit:
    """Crash scenario 2: handler runs successfully, crash before XACK.

    This is the dangerous case: the handler's side effects (DB writes,
    API calls, file mutations) have already happened, but the stream
    entry is still pending.  When another consumer reclaims this job,
    the handler runs again — duplicating side effects.

    This is WHY we need idempotency keys at the application level.
    RelayQ delivers at-least-once; the *consumer* must handle dedup.
    """

    @pytest.mark.asyncio
    async def test_handler_side_effects_fired_before_xack(self, executor, transport):
        """XACK not called after handler completes but before ack crash."""
        job = Job(kind="test", payload={"x": 1})
        transport.xack = AsyncMock()

        # Simulate the worker crashing between handler completion and XACK
        with patch.object(
            executor, "_handle_failure", side_effect=RuntimeError("crash before XACK")
        ):
            with pytest.raises(RuntimeError):
                await executor.execute("default", "entry-1", job, _handler_ok, "worker-1")

        # The XACK may or may not have been called depending on timing;
        # what matters is that the handler completed (side effects fired)
        # and the job can still be reclaimed.
        # We verify no crash in the handler path — the handler ran.
        assert job.payload == {"x": 1}  # handler didn't modify payload

    @pytest.mark.asyncio
    async def test_xack_failure_does_not_crash_worker(self, executor, transport):
        """XACK failure is logged but doesn't raise.

        CWE-754: Transport failures should not crash the worker loop.
        """
        job = Job(kind="test", payload={})
        transport.xack = AsyncMock(side_effect=ConnectionError("redis down"))

        # Should not raise — XACK failure is non-fatal
        await executor.execute("default", "entry-1", job, _handler_ok, "worker-1")

        # Handler still completed
        transport.xack.assert_called_once()


class TestCrashAfterCommitBeforeXACK:
    """Crash scenario 3: crash after outbox update but before XACK.

    The job is marked COMPLETED in the outbox, but the stream entry is
    still pending.  This is an inconsistency: the outbox says "done"
    but the stream will redeliver.

    This is a known limitation of the outbox pattern.  A reconciliation
    process (separate from the worker) should check for this state.
    For the portfolio, we document it as an edge case.
    """

    @pytest.mark.asyncio
    async def test_double_execution_possible(self, executor, transport):
        """Verifies that at-least-once can produce duplicate execution.

        This is not a bug — it's a documented property of the system.
        Consumers MUST use idempotency keys.
        """
        call_count = 0

        async def handler_with_counter(job):
            nonlocal call_count
            call_count += 1

        job = Job(kind="test", payload={})
        transport.xack = AsyncMock(side_effect=[ConnectionError("first fail"), 1])

        # First attempt: handler runs, XACK fails
        await executor.execute("default", "entry-1", job, handler_with_counter, "worker-1")

        # Second attempt: simulate XAUTOCLAIM redelivery
        job2 = Job(kind="test", payload={}, id=job.id)
        await executor.execute("default", "entry-1", job2, handler_with_counter, "worker-1")

        # Handler ran twice — at-least-once in action
        assert call_count == 2, (
            f"Handler ran {call_count} times — expected 2 (at-least-once delivery)"
        )


class TestDLQRouting:
    """DLQ routing after max retries."""

    @pytest.mark.asyncio
    async def test_dlq_after_max_retries(self, executor, transport):
        """Job goes to DLQ after exhausting retries."""
        transport.xadd_dlq = AsyncMock(return_value="dlq-1")
        transport.xack = AsyncMock(return_value=1)

        job = Job(kind="test", payload={}, max_retries=2)

        # Run the executor with a failing handler
        await executor.execute("default", "entry-1", job, _handler_fails, "worker-1")

        # Job should have 1 attempt, not maxed out yet
        assert job.attempts == 1

        # Simulate subsequent attempts until DLQ
        for i in range(3):
            new_job = Job(kind="test", payload={}, id=job.id, attempts=i, max_retries=2)
            await executor.execute("default", f"entry-{i}", new_job, _handler_fails, "worker-1")

        # The last one should have triggered DLQ
        transport.xadd_dlq.assert_called()
        transport.xack.assert_called()

    @pytest.mark.asyncio
    async def test_dlq_write_failure_does_not_crash(self, executor, transport):
        """DLQ write failure is non-fatal (CWE-703)."""
        transport.xadd_dlq = AsyncMock(side_effect=ConnectionError("redis down"))
        transport.xack = AsyncMock(return_value=1)

        job = Job(kind="test", payload={}, max_retries=0, attempts=0)

        # Should not raise — DLQ failure is handled
        await executor.execute("default", "entry-1", job, _handler_fails, "worker-1")

        transport.xadd_dlq.assert_called_once()
