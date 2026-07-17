from __future__ import annotations

import json
import logging
from typing import Any

import redis.asyncio as redis

from relayq.domain.job import Job, JobStatus

logger = logging.getLogger(__name__)


class RedisStreamTransport:
    """Redis Streams transport using consumer groups for reliable delivery.

    Why Redis Streams instead of a simple Redis list (LPUSH/BRPOP)?
    Consumer groups give us:
      - **At-least-once delivery**: pending entry list + XACK + XAUTOCLAIM
      - **Fair distribution**: each new consumer in the group gets a slice
      - **Dead-letter visibility**: pending entries that won't XACK are visible
      - **No polling coordination**: consumers don't fight over the same job

    Each queue gets its own stream key (relayq:{name}:stream) and consumer
    group (relayq:{name}:group).  The stream is capped with MAXLEN ~ to
    prevent unbounded memory growth (CWE-770).

    Transport contract:
      - Messages are JSON-serialised Job dicts
      - Consumers MUST XACK after successful processing
      - Orphaned entries (consumer crashed) are reclaimed via XAUTOCLAIM
      - Entries exceeding max_retries land in a separate DLQ stream
    """

    DLQ_SUFFIX = ":dlq"

    def __init__(self, redis_client: redis.Redis, stream_prefix: str = "relayq"):
        self.redis = redis_client
        self.prefix = stream_prefix

    # -- stream key helpers ---------------------------------------------------

    def _stream_key(self, queue: str) -> str:
        return f"{self.prefix}:{queue}:stream"

    def _group_key(self, queue: str) -> str:
        return f"{self.prefix}:{queue}:group"

    def _dlq_key(self, queue: str) -> str:
        return f"{self.prefix}:{queue}{self.DLQ_SUFFIX}"

    def _consumer_name(self, worker_id: str) -> str:
        return f"worker-{worker_id}"

    # -- lifecycle ------------------------------------------------------------

    async def ensure_group(self, queue: str) -> None:
        """Create the stream and consumer group if they don't exist.

        XGROUP CREATE is idempotent in MKSTREAM mode — if the stream or
        group already exists this is a no-op (Redis returns an error we
        swallow).  This makes startup race-safe across multiple workers.
        """
        stream = self._stream_key(queue)
        group = self._group_key(queue)
        try:
            await self.redis.xgroup_create(
                stream, group, id="0", mkstream=True
            )
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise  # Unexpected Redis error

    async def stream_length(self, queue: str) -> int:
        """Approximate number of messages in the stream."""
        key = self._stream_key(queue)
        return await self.redis.xlen(key)

    # -- enqueue --------------------------------------------------------------

    async def xadd(
        self,
        queue: str,
        job: Job,
        maxlen: int = 10_000,
        approximate: bool = True,
    ) -> str:
        """Append a job to the stream.  Returns the Redis entry ID.

        MAXLIN ~ 10000 bounds memory per queue (CWE-770).  The '~' means
        Redis trims lazily — it's not exact but guarantees the stream won't
        grow unbounded.
        """
        stream = self._stream_key(queue)
        body = {"job": json.dumps(job.to_dict(), default=str)}
        kwargs: dict[str, Any] = {}
        if maxlen:
            kwargs["maxlen"] = maxlen
            if approximate:
                kwargs["approximate"] = True
        entry_id = await self.redis.xadd(stream, body, **kwargs)
        return entry_id

    async def xadd_dlq(self, queue: str, job: Job, reason: str) -> str:
        """Move a job to the dead-letter queue with a reason annotation."""
        dlq = self._dlq_key(queue)
        body = {
            "job": json.dumps(job.to_dict(), default=str),
            "reason": reason,
            "dlqed_at": json.dumps(
                job.created_at.isoformat() if hasattr(job.created_at, "isoformat") else str(job.created_at),
                default=str,
            ),
        }
        return await self.redis.xadd(dlq, body)

    # -- dequeue / claim ------------------------------------------------------

    async def read_group(
        self,
        queue: str,
        worker_id: str,
        count: int = 1,
        block_ms: int = 2_000,
    ) -> list[tuple[str, str, Job]]:
        """Blocking read from the consumer group.

        Returns list of (entry_id, stream_entry_id, Job) tuples.
        Returns empty list on timeout (no jobs available).
        """
        stream = self._stream_key(queue)
        group = self._group_key(queue)
        consumer = self._consumer_name(worker_id)

        results = await self.redis.xreadgroup(
            group, consumer, {stream: ">"}, count=count, block=block_ms
        )
        if not results:
            return []

        jobs: list[tuple[str, str, Job]] = []
        for stream_name, entries in results:
            for entry_id, fields in entries:
                raw = fields.get(b"job", fields.get("job"))
                if raw is None:
                    logger.warning("Malformed stream entry %s — no 'job' field", entry_id)
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                try:
                    job_dict = json.loads(raw)
                    job = Job.from_dict(job_dict)
                    jobs.append((stream_name.decode() if isinstance(stream_name, bytes) else stream_name, entry_id, job))
                except (json.JSONDecodeError, KeyError) as exc:
                    logger.error("Failed to deserialise job from entry %s: %s", entry_id, exc)
        return jobs

    async def xack(self, queue: str, entry_id: str) -> int:
        """Acknowledge a job as processed.

        Returns the number of group messages acknowledged (0 or 1).
        """
        stream = self._stream_key(queue)
        group = self._group_key(queue)
        return await self.redis.xack(stream, group, entry_id)

    async def autoclaim_stale(
        self, queue: str, worker_id: str, min_idle_ms: int = 30_000, count: int = 10
    ) -> list[tuple[str, str, Job]]:
        """Reclaim pending entries that have been idle too long.

        XAUTOCLAIM reassigns pending entries from a dead/frozen consumer
        to the calling worker.  This is the core of lease recovery.

        CWE-754 (Unchecked Error Handling): If a worker crashes mid-processing
        the entry stays in the pending list forever — a resource leak.
        XAUTOCLAIM is the Redis-native fix.  We run it periodically in the
        recovery loop (application/recover.py).

        Returns same shape as read_group: (stream, entry_id, Job).
        """
        stream = self._stream_key(queue)
        group = self._group_key(queue)
        consumer = self._consumer_name(worker_id)

        result = await self.redis.xautoclaim(
            stream, group, consumer, min_idle_ms, "0-0", count=count
        )
        # xautoclaim returns a tuple: (next_cursor, [entries...])
        if not result or len(result) < 2:
            return []

        entries = result[1]
        # entries is a list of (entry_id, {field: value}) tuples
        reclaimed: list[tuple[str, str, Job]] = []
        for entry_id, fields in entries:
            raw = fields.get(b"job", fields.get("job"))
            if raw is None:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                job_dict = json.loads(raw)
                job = Job.from_dict(job_dict)
                reclaimed.append((stream, entry_id, job))
            except (json.JSONDecodeError, KeyError):
                logger.exception("Failed to deserialise reclaimed job %s", entry_id)
        return reclaimed

    # -- DLQ read-back --------------------------------------------------------

    async def read_dlq(self, queue: str, start: str = "-", end: str = "+", count: int = 50) -> list[dict]:
        """Read dead-letter queue entries (reversed — newest first)."""
        dlq = self._dlq_key(queue)
        entries = await self.redis.xrevrange(dlq, end, start, count=count)
        result = []
        for entry_id, fields in entries:
            raw = fields.get(b"job", fields.get("job"))
            reason = fields.get(b"reason", fields.get("reason", b"unknown"))
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            if isinstance(reason, bytes):
                reason = reason.decode("utf-8")
            try:
                job_dict = json.loads(raw)
                result.append({"entry_id": entry_id, "job": job_dict, "reason": reason})
            except json.JSONDecodeError:
                result.append({"entry_id": entry_id, "job": raw, "reason": reason})
        return result

    async def read_dlq_entry(self, queue: str, job_id: str) -> dict | None:
        """Find a specific job in the DLQ by job ID."""
        dlq = self._dlq_key(queue)
        entries = await self.redis.xrevrange(dlq, "+", "-", count=200)
        for entry_id, fields in entries:
            raw = fields.get(b"job", fields.get("job"))
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                job_dict = json.loads(raw)
                if job_dict.get("id") == job_id:
                    reason = fields.get(b"reason", fields.get("reason", b"unknown"))
                    if isinstance(reason, bytes):
                        reason = reason.decode("utf-8")
                    return {"entry_id": entry_id, "job": job_dict, "reason": reason}
            except json.JSONDecodeError:
                continue
        return None

    async def delete_dlq_entry(self, queue: str, entry_id: str) -> None:
        """Delete a single DLQ entry (used after replay)."""
        dlq = self._dlq_key(queue)
        await self.redis.xdel(dlq, entry_id)

    # -- queue listing --------------------------------------------------------

    async def list_queues(self) -> list[str]:
        """Discover queues by scanning stream keys."""
        pattern = f"{self.prefix}:*:stream"
        cursor = 0
        queues: set[str] = set()
        while True:
            cursor, keys = await self.redis.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                if isinstance(key, bytes):
                    key = key.decode("utf-8")
                # extract queue name from relayq:{name}:stream
                parts = key.split(":")
                if len(parts) >= 3:
                    queues.add(parts[1])
            if cursor == 0:
                break
        return sorted(queues)
