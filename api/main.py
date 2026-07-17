from __future__ import annotations

import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from relayq.application.enqueue import Enqueuer
from relayq.domain.errors import IdempotencyConflict, QueueFull
from relayq.domain.job import Job
from relayq.infrastructure.clock import Clock
from relayq.infrastructure.redis_stream import RedisStreamTransport
from relayq.telemetry.metrics import Metrics

logger = logging.getLogger("relayq.api")

# -- globals (set during lifespan) -------------------------------------------

transport: RedisStreamTransport | None = None
enqueuer: Enqueuer | None = None
metrics: Metrics | None = None
redis_client: redis.Redis | None = None


# -- lifespan -----------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global transport, enqueuer, metrics, redis_client

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_client = redis.from_url(redis_url, decode_responses=True)

    clock = Clock()
    transport = RedisStreamTransport(redis_client)
    metrics = Metrics()
    enqueuer = Enqueuer(transport=transport, clock=clock)

    # Warm up — ensure default consumer group exists
    await transport.ensure_group("default")

    logger.info("RelayQ API started (redis=%s)", redis_url)
    yield

    # Shutdown
    if redis_client:
        await redis_client.aclose()
    logger.info("RelayQ API stopped")


# -- FastAPI app --------------------------------------------------------------

app = FastAPI(
    title="RelayQ",
    description="Distributed job queue with Redis Streams transport",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -- middleware ----------------------------------------------------------------


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Attach a unique request ID to every request for tracing."""
    request_id = request.headers.get("X-Request-Id", str(uuid.uuid4()))
    request.state.request_id = request_id
    response: Response = await call_next(request)
    response.headers["X-Request-Id"] = request_id
    return response


@app.middleware("http")
async def structured_logging_middleware(request: Request, call_next):
    """Log every request in structured JSON format."""
    start = datetime.now(timezone.utc)
    response = await call_next(request)
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info(
        json.dumps(
            {
                "request_id": getattr(request.state, "request_id", None),
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "elapsed_seconds": round(elapsed, 3),
            }
        )
    )
    return response


# -- token bucket rate limiter ------------------------------------------------

class TokenBucketRateLimiter:
    """Atomic token bucket via a Lua script.

    Why a Lua script instead of read-modify-write in Python?
    The naive approach:
        1. GET current tokens
        2. If tokens > 0, DECR
        3. Else reject

    This has a TOCTOU race between GET and DECR.  Under concurrent
    requests, two callers can both see 1 token remaining and both
    DECR, letting 2 requests through instead of 1.

    A Lua script atomically refills the bucket (based on elapsed time)
    AND consumes a token in one Redis eval call.  No race.

    The script:
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local rate = tonumber(ARGV[2])       -- tokens per second
        local capacity = tonumber(ARGV[3])    -- max burst
        local cost = tonumber(ARGV[4])        -- tokens to consume (default 1)

        local info = redis.call("HMGET", key, "tokens", "last_refill")
        local tokens = tonumber(info[1]) or capacity
        local last_refill = tonumber(info[2]) or now

        local elapsed = math.max(0, now - last_refill)
        tokens = math.min(capacity, tokens + elapsed * rate)

        if tokens >= cost then
            tokens = tokens - cost
            redis.call("HMSET", key, "tokens", tokens, "last_refill", now)
            return 1  -- allowed
        else
            redis.call("HMSET", key, "tokens", tokens, "last_refill", now)
            return 0  -- rate limited
        end
    """

    LUA_SCRIPT = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local rate = tonumber(ARGV[2])
        local capacity = tonumber(ARGV[3])
        local cost = tonumber(ARGV[4])

        local info = redis.call("HMGET", key, "tokens", "last_refill")
        local tokens = tonumber(info[1]) or capacity
        local last_refill = tonumber(info[2]) or now

        local elapsed = math.max(0, now - last_refill)
        tokens = math.min(capacity, tokens + elapsed * rate)

        if tokens >= cost then
            tokens = tokens - cost
            redis.call("HMSET", key, "tokens", tokens, "last_refill", now)
            return 1
        else
            redis.call("HMSET", key, "tokens", tokens, "last_refill", now)
            return 0
        end
    """

    def __init__(self, redis_client: redis.Redis, key: str, rate: float, capacity: int):
        self.redis = redis_client
        self.key = key
        self.rate = rate
        self.capacity = capacity
        self._script_hash = None

    async def allow(self, cost: int = 1) -> bool:
        if self._script_hash is None:
            self._script_hash = await self.redis.script_load(self.LUA_SCRIPT)
        now = int(__import__("time").time())
        result = await self.redis.evalsha(
            self._script_hash,
            1,
            self.key,
            str(now),
            str(self.rate),
            str(self.capacity),
            str(cost),
        )
        return bool(result)


# Global rate limiter — 100 requests/second per instance
rate_limiter: TokenBucketRateLimiter | None = None


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Apply rate limiting via Redis-backed token bucket."""
    global rate_limiter
    if rate_limiter is None and redis_client is not None:
        rate_limiter = TokenBucketRateLimiter(
            redis_client, "ratelimit:api", rate=100, capacity=150,
        )

    if rate_limiter:
        allowed = await rate_limiter.allow()
        if not allowed:
            return Response(
                content=json.dumps({"error": "rate_limit_exceeded", "retry_after": 1}),
                status_code=429,
                media_type="application/json",
                headers={"Retry-After": "1"},
            )

    return await call_next(request)


# -- circuit breaker helper ---------------------------------------------------

class CircuitBreaker:
    """Simple circuit breaker for downstream dependencies.

    States:
      CLOSED — normal operation, requests pass through
      OPEN   — failures exceed threshold, requests fast-fail
      HALF_OPEN — probing if the dependency has recovered

    We track failures in a rolling window using Redis sorted sets.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        name: str,
        failure_threshold: int = 5,
        window_seconds: int = 60,
        half_open_after: int = 30,
    ):
        self.redis = redis_client
        self.key = f"circuitbreaker:{name}"
        self.lock_key = f"circuitbreaker:{name}:lock"
        self.failure_threshold = failure_threshold
        self.window = window_seconds
        self.half_open_after = half_open_after

    async def record_failure(self) -> None:
        now = __import__("time").time()
        pipe = self.redis.pipeline()
        pipe.zadd(self.key, {str(now): now})
        pipe.zremrangebyscore(self.key, "-inf", now - self.window)
        await pipe.execute()

    async def is_open(self) -> bool:
        state = await self.redis.get(f"{self.key}:state")
        if state is None or state == "CLOSED":
            return False
        if state == "HALF_OPEN":
            return False  # allow one request through
        # state == "OPEN"
        last_failure = await self.redis.get(f"{self.key}:last_failure")
        if last_failure:
            elapsed = __import__("time").time() - float(last_failure)
            if elapsed > self.half_open_after:
                await self.redis.set(f"{self.key}:state", "HALF_OPEN")
                return False
        return True

    async def maybe_trip(self) -> bool:
        now = __import__("time").time()
        # Count failures in the window
        count = await self.redis.zcount(self.key, now - self.window, now)
        if count >= self.failure_threshold:
            await self.redis.set(f"{self.key}:state", "OPEN")
            await self.redis.set(f"{self.key}:last_failure", str(now))
            return True
        return False

    async def reset(self) -> None:
        await self.redis.delete(self.key, f"{self.key}:state", f"{self.key}:last_failure")


# -- request/response models --------------------------------------------------


class EnqueueRequest(BaseModel):
    kind: str = Field(..., description="Job type/routing key")
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = Field(default="default", description="Queue name")
    idempotency_key: str | None = Field(default=None, description="Idempotency key for deduplication")
    max_retries: int = Field(default=3, ge=0, le=25, description="Max retry attempts before DLQ")


class JobStatusResponse(BaseModel):
    id: str
    kind: str
    status: str
    queue: str
    created_at: str
    attempts: int
    max_retries: int
    payload: dict[str, Any]


class QueueStats(BaseModel):
    name: str
    depth: int
    oldest_job_age_seconds: float
    dead_letter_count: int


class QueueListResponse(BaseModel):
    queues: list[QueueStats]


class DLQEntry(BaseModel):
    entry_id: str
    job: dict[str, Any]
    reason: str


class DLQListResponse(BaseModel):
    entries: list[DLQEntry]


# -- endpoints ----------------------------------------------------------------


@app.post("/jobs", status_code=201)
async def enqueue_job(req: EnqueueRequest) -> dict:
    """Enqueue a new job with optional idempotency key."""
    if not enqueuer:
        raise HTTPException(status_code=503, detail="enqueuer not ready")

    job = Job(
        kind=req.kind,
        payload=req.payload,
        queue=req.queue,
        max_retries=req.max_retries,
    )

    try:
        entry_id = await enqueuer.enqueue(job, idempotency_key=req.idempotency_key)
    except QueueFull as exc:
        raise HTTPException(
            status_code=503,
            detail={"error": "queue_full", "message": str(exc), "retry_after": 5},
            headers={"Retry-After": "5"},
        )
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return {"job_id": job.id, "stream_entry_id": entry_id, "status": "pending"}


@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str) -> dict:
    """Get a job's current status from the outbox."""
    if not enqueuer or not enqueuer.store:
        # Without a store we can't look up by ID — return partial info
        return {
            "id": job_id,
            "status": "unknown",
            "note": "Status lookup requires a configured outbox store",
        }

    job = enqueuer.store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"job '{job_id}' not found")

    return {
        "id": job.id,
        "kind": job.kind,
        "status": job.status.value,
        "queue": job.queue,
        "created_at": job.created_at.isoformat(),
        "attempts": job.attempts,
        "max_retries": job.max_retries,
        "payload": job.payload,
    }


@app.get("/queues", response_model=QueueListResponse)
async def list_queues() -> dict:
    """List all queues with depth and stats."""
    if not transport:
        raise HTTPException(status_code=503, detail="transport not ready")

    names = await transport.list_queues()
    stats = []
    for name in names:
        depth = await transport.stream_length(name)
        dlq_entries = await transport.read_dlq(name, count=0)
        # read_dlq returns entries; count=0 means we just get metadata
        # Actually, let's use XLEN on the DLQ stream
        dlq_key = transport._dlq_key(name)
        dlq_count = await redis_client.xlen(dlq_key) if redis_client else 0
        stats.append({
            "name": name,
            "depth": depth,
            "oldest_job_age_seconds": 0.0,
            "dead_letter_count": dlq_count,
        })

    return {"queues": stats}


@app.get("/queues/{name}/dlq")
async def list_dlq(name: str, limit: int = 50) -> dict:
    """List dead-letter queue contents."""
    if not transport:
        raise HTTPException(status_code=503, detail="transport not ready")

    entries = await transport.read_dlq(name, count=limit)
    return {"queue": name, "entries": entries, "count": len(entries)}


@app.post("/queues/{name}/dlq/{job_id}/replay")
async def replay_dlq_job(name: str, job_id: str) -> dict:
    """Replay a DLQ'd job — re-enqueue it for processing."""
    if not transport or not enqueuer:
        raise HTTPException(status_code=503, detail="not ready")

    # Find the entry in the DLQ
    entry = await transport.read_dlq_entry(name, job_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"job '{job_id}' not found in DLQ")

    job_dict = entry["job"]
    job = Job.from_dict(job_dict)
    job.status = "pending"
    job.attempts = 0  # Reset attempt counter for replay

    # Re-enqueue
    entry_id = await enqueuer.enqueue(job)

    # Remove from DLQ
    await transport.delete_dlq_entry(name, entry["entry_id"])

    return {"job_id": job.id, "new_stream_entry_id": entry_id, "status": "replayed"}
