# ADR 003: Outbox Pattern for Dual-Write Consistency

**Status:** Accepted  
**Date:** 2025-01-01  
**Author:** RelayQ Team  

## Context

When enqueueing a job, RelayQ needs to:
1. Write the job metadata (status, idempotency key, timestamps) to a persistent store (for `GET /jobs/{id}` and idempotency checks)
2. Push the job onto the Redis Stream (for worker consumption)

If these two writes aren't atomic, we can have inconsistencies:
- **Stream entry without outbox record:** Job runs but status can't be queried; idempotency key won't block duplicates
- **Outbox record without stream entry:** Job appears in status queries but never runs

This is the classic **dual-write problem**.

## Decision

**Use the outbox pattern: write the outbox record first (within a transaction), then XADD to the stream. Recovery processes handle the edge cases.**

The flow:
1. `BEGIN IMMEDIATE` (SQLite) or begin transaction (Postgres)
2. Insert job record into `jobs` table with status `pending`
3. `COMMIT`
4. `XADD` job to Redis Stream
5. If XADD fails, leave the outbox record in `pending` — the recovery process will eventually replay it

## Consequences

### Positive
- Outbox-first means the idempotency key is always consumed first — no duplicate runs
- The outbox provides a complete audit trail of all jobs ever enqueued
- SQLite with `BEGIN IMMEDIATE` prevents deadlocks under concurrent access (CWE-362)
- Recovery process (scan for `pending` records older than N seconds and replay) is straightforward
- Works without distributed transactions (no 2PC, no saga orchestrator)

### Negative
- **XADD failure window:** If the Redis write fails, the outbox remains in `pending` — the job won't run until the recovery process picks it up
- **Orphaned stream entries:** If the process crashes after XADD but before the outbox commit (if we ever do them in reverse order), the stream has an orphan entry
- Recovery adds latency: a job may sit in `pending` for up to the recovery interval (default 15s)
- Without a shared database (SQLite is local), the outbox is per-node — not suitable for multi-node deployments without Postgres

### Why not XA transactions?
XA (distributed transactions) across Redis and SQLite/Postgres would give atomic dual-write, but:
- XA is complex and often unavailable in managed Redis
- XA has blocking failure modes (in-doubt transactions)
- SQLite doesn't support XA
- The outbox pattern is simpler and well-understood

### Why not "XADD first, then outbox"?
If we XADD first and the outbox write fails, the stream has a job nobody knows about (no status record). Recovery would need to scan the stream and cross-reference with the outbox — more complex than scanning for `pending` records.

**Outbox-first minimizes the inconsistency window and makes recovery trivial.**
