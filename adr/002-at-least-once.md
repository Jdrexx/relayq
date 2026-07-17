# ADR 002: At-Least-Once Delivery with Idempotency Keys

**Status:** Accepted  
**Date:** 2025-01-01  
**Author:** RelayQ Team  

## Context

Job queues must define their delivery semantics. The choices are:

- **At-most-once:** Job is delivered zero or one times. Fast but can lose jobs.
- **At-least-once:** Job is delivered one or more times. No loss, but duplicate processing is possible.
- **Exactly-once:** Job is delivered exactly once. Theoretically impossible in distributed systems without perfect consensus.

Many queue tutorials claim "exactly-once" but actually mean "at-least-once with idempotent consumers." We believe in honesty about delivery semantics.

## Decision

**Explicitly provide at-least-once delivery with idempotency keys for consumer-side deduplication.**

Key points:
1. The transport (Redis Streams consumer groups) guarantees every job is XACK'd before removal — if a consumer crashes, XAUTOCLAIM redelivers.
2. The API accepts an optional `idempotency_key` header. If a job with the same key already exists (within the deduplication window), the request is rejected with `409 Conflict`.
3. The idempotency key is stored in the outbox (SQLite/Postgres) with a UNIQUE index, preventing TOCTOU races (CWE-362).
4. Without an idempotency key, duplicate delivery is possible and expected.

## Consequences

### Positive
- Honest documentation builds trust: consumers know they must be idempotent
- Idempotency keys are a well-understood pattern (Stripe, AWS, etc.)
- The outbox UNIQUE index makes the check atomic — no application-level locking
- Works with any handler that can check a "has this been processed?" flag
- No consensus protocol needed (no Paxos/Raft, no two-phase commit)

### Negative
- Idempotency keys require consumer cooperation — not automatic
- The deduplication window is bounded by the outbox retention policy (keys eventually expire)
- If the outbox write succeeds but the XADD fails, the idempotency key is consumed but the job never runs — requires reconciliation
- Duplicate deliveries can still happen if the handler runs but XACK fails (crash scenario 2)

### Why not exactly-once?
True exactly-once in a distributed system requires:
- A distributed consensus algorithm (Paxos, Raft, or ZooKeeper)
- Fencing tokens to prevent split-brain
- Idempotent receivers with exactly-once sinks (e.g., Kafka's transactional producer + idempotent producer)

This is valid for financial systems but is overengineered for a general-purpose job queue. The complexity-cost tradeoff doesn't favour it for RelayQ's target use case (async task execution, event-driven processing, scheduled jobs).
