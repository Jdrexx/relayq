# ADR 001: Redis Streams as Transport Layer

**Status:** Accepted  
**Date:** 2025-01-01  
**Author:** RelayQ Team  

## Context

RelayQ needs a transport layer for distributing jobs from producers to workers. The transport must support:
- At-least-once delivery semantics
- Consumer group / work-queue pattern (each job delivered to one worker)
- Lease recovery (reassign jobs from crashed workers)
- Backpressure signalling (don't overwhelm consumers)
- Observability (monitor queue depth, lag, throughput)

Options considered: Redis Streams, Apache Kafka, RabbitMQ, and a simple Redis list (LPUSH/BRPOP).

## Decision

**Use Redis Streams with consumer groups.**

Redis Streams (introduced in Redis 5.0) provide:
- **Consumer groups** with auto-distribution of messages across consumers
- **Pending entry list** for tracking unacknowledged deliveries
- **XAUTOCLAIM** (Redis 6.2+) for lease recovery without a separate dead-letter scan
- **MAXLEN** for bounded queue depth
- **BLOCK** for efficient polling without busy-waiting
- **No external dependency** beyond Redis (which most deployments already have)

## Consequences

### Positive
- No Kafka/ZooKeeper or RabbitMQ Erlang runtime dependency — single-process Redis
- Consumer groups give us at-least-once for free
- XAUTOCLAIM is cleaner than RabbitMQ's manual consumer recovery
- Stream entries are immutable audit logs
- Redis is fast and well-understood by the ops team

### Negative
- Redis Streams are not as battle-tested for job queues as RabbitMQ or Kafka
- No built-in scheduled/delayed delivery (would need a separate sorted-set approach)
- Stream entries are bounded by Redis memory — very large queues need partitioning
- No native DLQ support (we build one as a separate stream key)
- Redis is single-threaded for commands; very high throughput may be bottlenecked

### Compared to Kafka
Kafka would be overkill: we don't need log compaction, multi-consumer offset management, or long-term retention. Kafka's consumer groups are more complex to operate. Redis Streams give us 80% of the functionality at 20% of the operational cost.

### Compared to RabbitMQ
RabbitMQ has mature job-queue features (dead-letter exchanges, TTL, delayed queues). However, consumer lease recovery in RabbitMQ requires manual queue binding or the "consumer cancellation" pattern. Redis Streams' XAUTOCLAIM is simpler.

### Compared to Redis List (LPUSH/BRPOP)
A simple list doesn't support consumer groups — each BRPOP removes the entry, so a crashed worker loses the job permanently. No at-least-once. Redis Streams are strictly superior for this use case.
