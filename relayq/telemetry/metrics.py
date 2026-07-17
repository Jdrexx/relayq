from __future__ import annotations

import logging
from typing import Any

from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.registry import REGISTRY

logger = logging.getLogger(__name__)


class Metrics:
    """Prometheus metrics for RelayQ.

    Each queue exports its own set of metrics labeled by queue name
    and (where applicable) job kind.  This lets operators build
    per-queue dashboards.

    Metric      Type        Labels              Description
    ─────────────────────────────────────────────────────────────
    queue_depth            Gauge     queue       Number of entries in the stream
    oldest_job_age         Gauge     queue       Age of the oldest pending entry (seconds)
    processing_seconds     Histogram queue,kind  Time spent executing a job handler
    delivery_attempts      Counter   queue,kind,success  Number of delivery attempts
    dead_letter_total      Counter   queue,kind  Jobs routed to the DLQ
    worker_inflight        Gauge     queue       Number of jobs currently being processed

    CWE-400 (Resource Exhaustion): Metrics help operators detect
    congestion early.  A sudden spike in queue_depth + flat delivery_rate
    = backpressure engaged.
    """

    def __init__(self, registry: Any = REGISTRY, namespace: str = "relayq"):
        self._registry = registry
        self._ns = namespace

        # Use labels=['queue'] for all metrics so they're partitionable
        # by queue in Grafana.

        self.queue_depth = Gauge(
            name=f"{self._ns}_queue_depth",
            documentation="Current number of entries in the queue stream",
            labelnames=["queue"],
            registry=registry,
        )

        self.oldest_job_age = Gauge(
            name=f"{self._ns}_oldest_job_age_seconds",
            documentation="Age of the oldest pending entry in seconds",
            labelnames=["queue"],
            registry=registry,
        )

        self.processing_seconds = Histogram(
            name=f"{self._ns}_processing_seconds",
            documentation="Time spent processing a job (seconds)",
            labelnames=["queue", "kind"],
            buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
            registry=registry,
        )

        self.delivery_attempts = Counter(
            name=f"{self._ns}_delivery_attempts_total",
            documentation="Total number of delivery attempts by outcome",
            labelnames=["queue", "kind", "success"],
            registry=registry,
        )

        self.dead_letter_total = Counter(
            name=f"{self._ns}_dead_letter_total",
            documentation="Total number of jobs routed to the dead-letter queue",
            labelnames=["queue", "kind"],
            registry=registry,
        )

        self.worker_inflight = Gauge(
            name=f"{self._ns}_worker_inflight",
            documentation="Number of jobs currently being processed by workers",
            labelnames=["queue"],
            registry=registry,
        )

    # -- convenience wrappers ------------------------------------------------

    def observe_queue_depth(self, queue: str, depth: int) -> None:
        self.queue_depth.labels(queue=queue).set(depth)

    def observe_oldest_job_age(self, queue: str, age_seconds: float) -> None:
        self.oldest_job_age.labels(queue=queue).set(age_seconds)

    def observe_processing_seconds(self, kind: str, elapsed: float) -> None:
        # Note: we don't always have the queue name in scope here.
        # Defaulting to "unknown" is better than crashing.
        self.processing_seconds.labels(queue="unknown", kind=kind).observe(elapsed)

    def observe_processing_seconds_for_queue(self, queue: str, kind: str, elapsed: float) -> None:
        self.processing_seconds.labels(queue=queue, kind=kind).observe(elapsed)

    def incr_delivery_attempts(self, kind: str, success: bool) -> None:
        self.delivery_attempts.labels(
            queue="unknown", kind=kind, success="true" if success else "false"
        ).inc()

    def incr_dead_letter(self, kind: str) -> None:
        self.dead_letter_total.labels(queue="unknown", kind=kind).inc()

    def observe_worker_inflight(self, queue: str, count: int) -> None:
        self.worker_inflight.labels(queue=queue).set(count)
