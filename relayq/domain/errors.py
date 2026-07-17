from __future__ import annotations


class QueueFull(Exception):
    """The queue has reached its maximum allowed depth.

    CWE-770 (Allocation of Resources Without Limits or Throttling):
    Bounded queue depth prevents runaway memory growth in Redis.
    When the stream exceeds max_length, new enqueue attempts are
    rejected with this error instead of silently expanding.

    The caller should interpret this as backpressure and retry
    after a delay (exponential backoff with jitter, naturally).
    """

    def __init__(self, queue: str, depth: int, max_depth: int):
        self.queue = queue
        self.depth = depth
        self.max_depth = max_depth
        super().__init__(
            f"queue '{queue}' at capacity {depth}/{max_depth}"
        )


class JobTimeout(Exception):
    """A job exceeded its processing deadline.

    CWE-400 (Uncontrolled Resource Consumption / 'Resource Exhaustion'):
    Without processing timeouts, a single stuck handler can hold a
    consumer-group slot indefinitely, starving other jobs.  The worker
    enforces a per-job timeout; if the handler hasn't completed within
    that window the job is considered failed and retried or DLQ'd.

    This bounds worst-case latency for other jobs in the same queue.
    """

    def __init__(self, job_id: str, timeout_seconds: float):
        self.job_id = job_id
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"job '{job_id}' timed out after {timeout_seconds}s"
        )


class CircuitOpen(Exception):
    """Circuit breaker is open — downstream dependency is failing.

    When a queue's failure rate exceeds the threshold, the circuit
    breaker trips.  All subsequent enqueue/dequeue attempts for that
    queue are fast-failed until the breaker half-opens and eventually
    closes again.

    This prevents cascading failures: if the downstream handler (e.g.
    an HTTP API, a database) is unhealthy, there's no point piling
    more work onto it.
    """

    def __init__(self, name: str, failure_ratio: float):
        self.name = name
        self.failure_ratio = failure_ratio
        super().__init__(
            f"circuit breaker open for '{name}' "
            f"(failure ratio {failure_ratio:.2f})"
        )


class IdempotencyConflict(Exception):
    """A job with this idempotency key was already processed.

    This is fundamental to the at-least-once delivery guarantee.
    At-least-once means the same job may be delivered multiple times;
    idempotency keys let the *consumer* deduplicate safely.

    We do NOT claim exactly-once delivery — that's impossible in
    distributed systems without consensus or a perfectly reliable
    deduplication store (which we don't have).  Instead, we provide
    the tools for the *application* to achieve idempotent processing.

    CWE-362 (Concurrent Execution): The idempotency check + outbox
    insert must be atomic (wrapped in a transaction or Lua script).
    A TOCTOU race between check and insert could let duplicate jobs
    through. See application/enqueue.py for the transactional approach.
    """

    def __init__(self, idempotency_key: str):
        self.idempotency_key = idempotency_key
        super().__init__(
            f"idempotency key '{idempotency_key}' already exists"
        )
