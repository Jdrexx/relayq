# RelayQ — Distributed Job Queue

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**RelayQ** is a distributed job queue built on Redis Streams. It delivers at-least-once semantics, bounded backpressure, a dead-letter queue, and Prometheus metrics out of the box. Built for operators who need to understand _what's happening_ in their queue without guessing.

---

## Architecture

```
 ┌──────────┐     POST /jobs      ┌──────────────────────────────────────┐
 │  Client   │ ──────────────────► │            RelayQ API                 │
 │  (app/svc)│                     │  ┌──────────┐  ┌───────────────────┐  │
 └──────────┘                     │  │ Validate  │  │ Idempotency Check │  │
                                   │  │ (schema)  │  │ (UNIQUE index)    │  │
                                   │  └─────┬────┘  └────────┬──────────┘  │
                                   │        └──────┬──────────┘             │
                                   │               ▼                       │
                                   │  ┌─────────────────────────┐          │
                                   │  │  Outbox (SQLite/DB)     │          │
                                   │  │  INSERT job (pending)   │          │
                                   │  └──────────┬──────────────┘          │
                                   │              │ COMMIT                 │
                                   │              ▼                       │
                                   │  ┌─────────────────────────┐          │
                                   │  │  XADD → Redis Stream    │          │
                                   │  └─────────────────────────┘          │
                                   └──────────────────┬───────────────────┘
                                                        │
                                                        ▼
                              ┌────────────────────────────────────────────┐
                              │         Redis Stream (consumer group)      │
                              │  relayq:{queue}:stream  ★ MAXLEN ~ 10000  │
                              └──────────────────┬─────────────────────────┘
                                                    │
                          ┌─────────────────────────┼─────────────────────┐
                          │                         │                     │
                          ▼                         ▼                     ▼
               ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
               │  Worker 1        │    │  Worker 2        │    │  Worker N        │
               │  ┌────────────┐  │    │  ┌────────────┐  │    │  ┌────────────┐  │
               │  │ Admission  │  │    │  │ Admission  │  │    │  │ Admission  │  │
               │  │ Controller │  │    │  │ Controller │  │    │  │ Controller │  │
               │  └──────┬─────┘  │    │  └──────┬─────┘  │    │  └──────┬─────┘  │
               │         ▼        │    │         ▼        │    │         ▼        │
               │  ┌────────────┐  │    │  ┌────────────┐  │    │  ┌────────────┐  │
               │  │ Semaphore  │  │    │  │ Semaphore  │  │    │  │ Semaphore  │  │
               │  │ (bulkhead) │  │    │  │ (bulkhead) │  │    │  │ (bulkhead) │  │
               │  └──────┬─────┘  │    │  └──────┬─────┘  │    │  └──────┬─────┘  │
               │         ▼        │    │         ▼        │    │         ▼        │
               │  ┌────────────┐  │    │  ┌────────────┐  │    │  ┌────────────┐  │
               │  │ Handler    │  │    │  │ Handler    │  │    │  │ Handler    │  │
               │  └──────┬─────┘  │    │  └──────┬─────┘  │    │  └──────┬─────┘  │
               └─────────┼────────┘    └─────────┼────────┘    └─────────┼────────┘
                          │                       │                       │
                          └───────────┬───────────┘                       │
                                      │                                   │
                                      ▼                                   ▼
                          ┌─────────────────────┐              ┌─────────────────────┐
                          │  XACK (success)     │              │  DLQ (dead letters) │
                          │  relayq:{q}:stream  │              │  relayq:{q}:dlq     │
                          └─────────────────────┘              └─────────────────────┘
```

### Core data flow

1. **Client** sends a job description (`kind`, `payload`, `queue`, optional `idempotency_key`)
2. **API** validates the payload, checks the idempotency key (UNIQUE index in outbox), inserts an outbox record with status `pending`, then `XADD`s the job to the Redis Stream
3. **Redis Stream** (consumer group) holds the job until a worker claims it. Stream is capped at ~10K entries (CWE-770)
4. **Worker** polls the stream with `XREADGROUP BLOCK`, passes through admission control (backpressure check), acquires a semaphore slot (bulkhead), and runs the handler
5. **Success** → `XACK` the entry, mark outbox as `completed`
6. **Failure** → increment attempt counter; if `attempts > max_retries`, move to DLQ (`XADD` to `relayq:{queue}:dlq` + `XACK`); otherwise leave in pending for redelivery via `XAUTOCLAIM`
7. **Stale entries** → `XAUTOCLAIM` (recover.py) reclaims pending entries from crashed consumers

---

## Delivery Contract

### At-Least-Once (NOT exactly-once)

RelayQ guarantees that every acknowledged job will be delivered **at least once**. It does NOT guarantee exactly-once delivery.

| Scenario | Behaviour |
|----------|-----------|
| Worker crashes before handler | Job stays in Pending list → XAUTOCLAIM → redelivered to another worker |
| Handler succeeds, crash before XACK | Handler ran (side effects fired). Job stays Pending → redelivered → handler runs AGAIN |
| Handler succeeds, XACK succeeds, crash before outbox update | Job processed. Outbox stuck on `processing`. Reconciliation needed |
| XADD to stream fails | Job stays in outbox as `pending` → recovery process replays |
| DLQ write fails | Job stays in Pending list → retried later, possibly exceeding max_retries |

### How to handle duplication (idempotency keys)

Every enqueue request can include an `idempotency_key`. The API rejects duplicate keys with `409 Conflict`. Use this pattern in your handlers:

```python
async def my_handler(job):
    # 1. Check idempotency
    if await has_been_processed(job.idempotency_key):
        return {"skipped": "already processed"}

    # 2. Do the work
    await send_email(job.payload["to"], job.payload["subject"])

    # 3. Mark as processed (same store as the check)
    await mark_processed(job.idempotency_key)
```

> **Why not exactly-once?** True exactly-once delivery in distributed systems requires consensus (Paxos/Raft), fencing tokens, and idempotent sinks. This complexity is justified for financial transactions but not for general-purpose job queues. We choose honest at-least-once with idempotency tools.

---

## Quickstart

### Prerequisites

- Python 3.11+
- Docker & Docker Compose (for Redis + monitoring)
- Redis 7+ (or the Docker image handles this)

### Option A: docker-compose (recommended for development)

```bash
git clone https://github.com/yourorg/relayq.git
cd relayq

# Start everything
docker compose up -d

# Enqueue a test job
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"kind":"ping","payload":{"msg":"hello world"},"queue":"default"}'

# Check queues
curl http://localhost:8000/queues
```

This starts: API server (`:8000`), worker, Redis (`:6379`), Prometheus (`:9090`), Grafana (`:3000` — admin/admin).

### Option B: local development

```bash
# Install
pip install -e ".[dev]"

# Start Redis (if not running)
docker run -d -p 6379:6379 redis:7-alpine

# Start API server
uvicorn api.main:app --reload --port 8000

# In another terminal, start a worker
python run_worker.py

# Use the CLI
python cli.py enqueue default '{"task": "hello"}'
python cli.py queues
```

### CLI commands

```bash
# Enqueue a job
relayq enqueue <queue> <json-payload>

# Check job status
relayq status <job-id>

# List queues
relayq queues

# List dead letters
relayq dlq <queue>

# Replay a dead letter
relayq replay <queue> <job-id>
```

---

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Transport | **Redis Streams** | Consumer groups, XAUTOCLAIM, MAXLEN — see ADR-001 |
| API    | **FastAPI** | Async-native, Pydantic validation, OpenAPI docs |
| Worker | **asyncio** | Single-threaded concurrency, no GIL contention for I/O |
| Outbox | **SQLite** (dev) / **Postgres** (prod) | Transactional dual-write — see ADR-003 |
| Metrics | **Prometheus** + **Grafana** | Standard observability stack |
| CLI    | **stdlib** (urllib) | Zero extra dependencies for basic ops |
| Deploy | **Docker** / **Railway** | Multi-stage build, non-root user |

---

## Metrics Reference

All metrics are prefixed with `relayq_` and served at `/metrics` (Prometheus scrape endpoint).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `relayq_queue_depth` | Gauge | `queue` | Current number of entries in the stream |
| `relayq_oldest_job_age_seconds` | Gauge | `queue` | Age of the oldest pending entry |
| `relayq_processing_seconds` | Histogram | `queue`, `kind` | Time spent executing a job handler (buckets: 10ms–300s) |
| `relayq_delivery_attempts_total` | Counter | `queue`, `kind`, `success` | Total delivery attempts by outcome |
| `relayq_dead_letter_total` | Counter | `queue`, `kind` | Jobs routed to DLQ |
| `relayq_worker_inflight` | Gauge | `queue` | Jobs currently being processed |

---

## CWE References in Code

RelayQ explicitly addresses the following Common Weakness Enumerations:

| CWE | Name | Where Addressed |
|-----|------|----------------|
| **CWE-770** | Allocation of Resources Without Limits | Bounded queue depth (`MAXLEN ~ 10000`), payload size limits at API layer, bounded retry caps |
| **CWE-400** | Uncontrolled Resource Consumption | Backpressure admission control, retry timeout, job processing timeout, bulkhead concurrency limits |
| **CWE-754** | Unchecked Error Handling | XACK failures logged (not crashed), DLQ write failures logged, recovery loop continues on per-queue errors |
| **CWE-362** | Concurrent Execution | Idempotency keys with UNIQUE index, outbox pattern with `BEGIN IMMEDIATE`, Lua-based token bucket |
| **CWE-799** | Interaction Frequency | Full-jitter retry backoff (not naive exponential), rate-limited API |
| **CWE-703** | Exceptional Conditions | All failure paths in worker loop, executor, and recovery are caught and handled |

---

## Why Redis Streams instead of Celery/Kafka?

### vs Celery
Celery is the de-facto Python job queue. It's battle-tested and feature-rich. But:
- **RelayQ is NOT a Celery wrapper** — the core queue logic (lease management, backpressure, failure test suites) is custom
- Celery's transport layer (RabbitMQ/Redis) is abstracted — you don't write to the stream directly
- RelayQ exposes raw Redis Streams for debugging: you can `redis-cli XLEN relayq:default:stream` directly
- Celery's monitoring requires Flower; RelayQ ships Prometheus metrics + Grafana dashboard
- Celery's retry backoff is configurable but doesn't default to full jitter

### vs Kafka
- Kafka is a log, not a work queue. Consumer groups in Kafka are designed for streaming, not point-to-point job distribution
- Kafka's at-least-once guarantees require careful offset management
- Kafka has a heavy operational footprint (ZooKeeper/KRaft, disk management)
- Redis Streams give us the work-queue semantics we want with a fraction of the complexity

### vs RabbitMQ
- RabbitMQ has excellent job-queue features (dead-letter exchanges, TTL, delayed queues)
- But consumer lease recovery is more manual (consumer cancellation notices, manual requeue)
- XAUTOCLAIM (Redis 6.2+) is a cleaner recovery mechanism
- RabbitMQ runs as a separate Erlang VM; Redis is often already in the stack

**Bottom line:** Redis Streams hit the sweet spot between simplicity and capability for a portfolio-quality distributed job queue.

---

## Project Structure

```
relayq/                     # Core queue library
├── __init__.py
├── domain/                 # Domain models
│   ├── job.py              # Job dataclass, JobStatus enum
│   ├── retry.py            # RetryPolicy with full jitter
│   └── errors.py           # QueueFull, JobTimeout, CircuitOpen, IdempotencyConflict
├── application/            # Use cases
│   ├── enqueue.py          # Transactional enqueue (outbox pattern)
│   ├── execute.py          # Worker execution (handler → ack/DLQ)
│   └── recover.py          # Lease recovery via XAUTOCLAIM
├── infrastructure/         # Transport & persistence
│   ├── redis_stream.py     # Redis Streams transport
│   ├── sqlite_store.py     # SQLite outbox store
│   └── clock.py            # Clock abstraction (FakeClock for tests)
├── worker/                 # Worker lifecycle
│   ├── runner.py           # Async worker loop
│   ├── admission.py        # Backpressure admission control
│   └── shutdown.py         # Graceful shutdown
└── telemetry/
    └── metrics.py          # Prometheus metrics

api/main.py                 # FastAPI server
cli.py                      # CLI client
run_worker.py               # Docker worker entrypoint

tests/                      # pytest suite
├── test_retry.py           # Property-based retry tests
├── test_crash_scenarios.py # Crash: before/after handler, after XACK
├── test_idempotency.py     # Idempotency key dedup
├── test_backpressure.py    # Admission control under load
└── test_shutdown.py        # Graceful shutdown + clock mocking

adr/                        # Architecture Decision Records
├── 001-redis-streams.md
├── 002-at-least-once.md
└── 003-outbox-pattern.md

Dockerfile                  # Multi-stage, non-root
docker-compose.yml          # API + worker + Redis + Prometheus + Grafana
prometheus.yml              # Prometheus scrape config
railway.toml                # Railway deployment config
ENV.md                      # Environment variables reference
```

---

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=relayq --cov-report=term-missing

# Run only unit tests (skip integration tests needing Redis)
pytest -m "not integration"

# Property-based tests (hypothesis)
pytest tests/test_retry.py -v
```

### Test scenarios covered

- **Property test:** retry delay always between 0 and cap (Hypothesis)
- **Crash before handler:** job stays in pending, no XACK sent
- **Crash after side effect but before XACK:** handler ran twice (at-least-once)
- **Crash after XACK but before outbox update:** documented edge case
- **Idempotency key dedup:** same key → 409, different keys → OK
- **DLQ routing:** job goes to DLQ after max retries
- **DLQ write failure:** non-fatal (CWE-703)
- **Backpressure:** admission blocks when queue exceeds threshold
- **Graceful shutdown:** inflight tasks drained, slow tasks cancelled
- **Clock mocking:** deterministic time via FakeClock

---

## License

MIT. See `LICENSE`.
