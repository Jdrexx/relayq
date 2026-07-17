"""Idempotency key deduplication and conflict tests.

CWE-362 (Concurrent Execution):
Idempotency keys prevent duplicate job insertion.  The UNIQUE index
on the SQLite outbox (or the Lua script on Redis) ensures that
concurrent requests with the same key can't both succeed.
"""

from __future__ import annotations

import pytest

from relayq.domain.errors import IdempotencyConflict
from relayq.domain.job import Job
from relayq.infrastructure.clock import FakeClock
from relayq.infrastructure.sqlite_store import SqliteStore
from relayq.application.enqueue import Enqueuer
from relayq.infrastructure.redis_stream import RedisStreamTransport

# Use in-memory SQLite for tests
import tempfile, os


@pytest.fixture
def db_path():
    """Temporary SQLite database path."""
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    yield f.name
    os.unlink(f.name)


@pytest.fixture
def store(db_path):
    return SqliteStore(db_path=db_path, clock=FakeClock())


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def transport():
    """Mocked transport — we don't need Redis for idempotency tests."""
    import uuid
    from unittest.mock import AsyncMock, MagicMock

    t = MagicMock(spec=RedisStreamTransport)
    t.stream_length = AsyncMock(return_value=0)
    t.xadd = AsyncMock(return_value=f"entry-{uuid.uuid4().hex[:8]}")
    t.ensure_group = AsyncMock(return_value=None)
    return t


@pytest.fixture
def enqueuer(transport, clock, store):
    return Enqueuer(transport=transport, clock=clock, store=store)


class TestIdempotencyDedup:
    """Idempotency key prevents duplicate enqueue."""

    @pytest.mark.asyncio
    async def test_same_key_rejected(self, enqueuer):
        job1 = Job(kind="email", payload={"to": "a@b.com"})
        job2 = Job(kind="email", payload={"to": "a@b.com"})

        # First enqueue succeeds
        result1 = await enqueuer.enqueue(job1, idempotency_key="email-abc-123")
        assert result1 is not None

        # Second enqueue with same key is rejected
        with pytest.raises(IdempotencyConflict):
            await enqueuer.enqueue(job2, idempotency_key="email-abc-123")

    @pytest.mark.asyncio
    async def test_different_keys_allowed(self, enqueuer):
        job1 = Job(kind="email", payload={"to": "a@b.com"})
        job2 = Job(kind="email", payload={"to": "b@c.com"})

        result1 = await enqueuer.enqueue(job1, idempotency_key="key-1")
        result2 = await enqueuer.enqueue(job2, idempotency_key="key-2")

        assert result1 is not None
        assert result2 is not None

    @pytest.mark.asyncio
    async def test_no_key_no_conflict(self, enqueuer):
        """Without idempotency keys, duplicate jobs are allowed."""
        job1 = Job(kind="email", payload={"to": "a@b.com"})
        job2 = Job(kind="email", payload={"to": "a@b.com"})

        result1 = await enqueuer.enqueue(job1)
        result2 = await enqueuer.enqueue(job2)

        assert result1 is not None
        assert result2 is not None
