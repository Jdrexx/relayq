"""Graceful shutdown and clock-mocking tests.

CWE-754 (Unchecked Error Handling): Graceful shutdown ensures that
in-flight jobs are given time to complete before the worker exits.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from relayq.worker.shutdown import GracefulShutdown


class TestGracefulShutdown:
    """Graceful shutdown behaviour."""

    @pytest.mark.asyncio
    async def test_is_set_returns_false_initially(self):
        shutdown = GracefulShutdown()
        assert shutdown.is_set() is False

    @pytest.mark.asyncio
    async def test_request_sets_flag(self):
        shutdown = GracefulShutdown()
        shutdown.request()
        assert shutdown.is_set() is True

    @pytest.mark.asyncio
    async def test_drain_with_no_inflight(self):
        shutdown = GracefulShutdown()
        # Should complete immediately without error
        await shutdown.drain(set())

    @pytest.mark.asyncio
    async def test_drain_completes_fast_tasks(self):
        shutdown = GracefulShutdown(drain_timeout=5.0)

        async def quick_task():
            await asyncio.sleep(0.01)

        tasks = {asyncio.create_task(quick_task()) for _ in range(5)}
        await shutdown.drain(tasks)
        # All tasks should be done
        assert all(t.done() for t in tasks)

    @pytest.mark.asyncio
    async def test_drain_cancels_slow_tasks_after_timeout(self):
        shutdown = GracefulShutdown(drain_timeout=0.05)  # very short timeout

        async def slow_task():
            await asyncio.sleep(10)  # longer than timeout

        tasks = {asyncio.create_task(slow_task()) for _ in range(3)}
        await shutdown.drain(tasks)
        # Tasks should be cancelled
        assert all(t.done() for t in tasks)
        # At least some should be cancelled
        cancelled = sum(1 for t in tasks if t.cancelled())
        assert cancelled > 0, "Expected at least one task to be cancelled"

    @pytest.mark.asyncio
    async def test_wait_for_shutdown_blocks(self):
        shutdown = GracefulShutdown()

        # Start a task that waits for shutdown
        async def waiter():
            await shutdown.wait_for_shutdown()

        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.05)
        assert not task.done()

        # Trigger shutdown
        shutdown.request()
        await asyncio.sleep(0.05)
        assert task.done()


class TestClockMocking:
    """Deterministic time tests using FakeClock."""

    def test_fake_clock_starts_at_base_time(self):
        from relayq.infrastructure.clock import FakeClock
        from datetime import datetime, timezone, timedelta

        clock = FakeClock()
        assert clock.utcnow() == FakeClock.BASE_TIME
        assert clock.monotonic() == 0.0

    def test_fake_clock_advance_seconds(self):
        from relayq.infrastructure.clock import FakeClock
        from datetime import timedelta

        clock = FakeClock()
        clock.advance(seconds=30)
        assert clock.utcnow() == FakeClock.BASE_TIME + timedelta(seconds=30)
        assert clock.monotonic() == 30.0

    def test_fake_clock_advance_minutes(self):
        from relayq.infrastructure.clock import FakeClock
        from datetime import timedelta

        clock = FakeClock()
        clock.advance(minutes=5)
        assert clock.utcnow() == FakeClock.BASE_TIME + timedelta(minutes=5)
        assert clock.monotonic() == 300.0

    def test_fake_clock_advance_combined(self):
        from relayq.infrastructure.clock import FakeClock
        from datetime import timedelta

        clock = FakeClock()
        clock.advance(seconds=30, minutes=1)
        expected = FakeClock.BASE_TIME + timedelta(seconds=30, minutes=1)
        assert clock.utcnow() == expected
        assert clock.monotonic() == 90.0
