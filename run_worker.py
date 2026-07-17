#!/usr/bin/env python3
"""Worker entrypoint for Docker deployment."""
import asyncio
import logging
import os

import redis.asyncio as redis

from relayq.application.execute import Executor
from relayq.domain.job import Job
from relayq.infrastructure.clock import Clock
from relayq.infrastructure.redis_stream import RedisStreamTransport
from relayq.telemetry.metrics import Metrics
from relayq.worker.runner import WorkerRunner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("relayq.worker")


async def handler(job: Job) -> dict:
    """Default no-op handler for Docker demo."""
    logger.info("Processing job %s (kind=%s, payload=%s)", job.id, job.kind, job.payload)
    return {"ok": True}


async def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    transport = RedisStreamTransport(r)
    clock = Clock()
    metrics = Metrics()
    executor = Executor(transport=transport, clock=clock, metrics=metrics)

    runner = WorkerRunner(
        transport=transport,
        executor=executor,
        metrics=metrics,
        clock=clock,
        worker_id="docker-worker",
        queue="default",
        handler=handler,
        max_concurrency=10,
    )

    logger.info("Worker starting (redis=%s)", redis_url)
    await runner.run()


if __name__ == "__main__":
    asyncio.run(main())
