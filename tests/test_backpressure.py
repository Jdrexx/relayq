"""Admission control and backpressure tests.

CWE-770 (Allocation Limits): The admission controller rejects jobs
when the queue is too deep or the oldest job is too old.  This prevents
unbounded resource consumption.

CWE-400 (Resource Exhaustion): Backpressure protects the worker from
being overwhelmed by a backlog spike.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from relayq.infrastructure.clock import FakeClock
from relayq.infrastructure.redis_stream import RedisStreamTransport
from relayq.worker.admission import AdmissionController, AdmissionBlocked


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def transport():
    t = MagicMock(spec=RedisStreamTransport)
    t.stream_length = AsyncMock(return_value=0)
    return t


@pytest.fixture
def controller(transport, clock):
    return AdmissionController(
        transport=transport,
        clock=clock,
        max_depth=100,
        max_oldest_age_seconds=60.0,
    )


class TestAdmissionControl:
    """Backpressure under load."""

    @pytest.mark.asyncio
    async def test_allows_when_below_threshold(self, controller, transport):
        transport.stream_length = AsyncMock(return_value=50)

        # Should not raise
        await controller.check("default")

    @pytest.mark.asyncio
    async def test_blocks_when_at_capacity(self, controller, transport):
        transport.stream_length = AsyncMock(return_value=100)

        with pytest.raises(AdmissionBlocked):
            await controller.check("default")

    @pytest.mark.asyncio
    async def test_blocks_when_over_capacity(self, controller, transport):
        transport.stream_length = AsyncMock(return_value=150)

        with pytest.raises(AdmissionBlocked):
            await controller.check("default")

    @pytest.mark.asyncio
    async def test_is_healthy(self, controller, transport):
        transport.stream_length = AsyncMock(return_value=50)
        assert await controller.is_healthy("default") is True

        transport.stream_length = AsyncMock(return_value=200)
        assert await controller.is_healthy("default") is False

    @pytest.mark.asyncio
    async def test_transport_error_does_not_crash(self, controller, transport):
        """If Redis is unreachable, admission check should not crash the worker."""
        transport.stream_length = AsyncMock(side_effect=ConnectionError("redis down"))

        # Should raise an error (not crash the process)
        with pytest.raises(Exception):
            await controller.check("default")

    @pytest.mark.asyncio
    async def test_different_queues_independent(self, controller, transport):
        """Admission state is per-queue."""
        transport.stream_length = AsyncMock()
        transport.stream_length.side_effect = lambda q: {
            "busy": 200,
            "idle": 10,
        }.get(q, 0)

        with pytest.raises(AdmissionBlocked):
            await controller.check("busy")

        # Idle queue is fine
        await controller.check("idle")
