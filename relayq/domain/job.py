from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class JobStatus(str, Enum):
    """Lifecycle of a job through the queue.

    pending      — enqueued, waiting for first worker pickup
    processing   — claimed by a worker, running the handler
    completed    — handler succeeded, acknowledged
    failed       — handler raised, will be retried if attempts remain
    dead         — exhausted all retries, routed to DLQ
    """

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class Job:
    """A unit of work flowing through RelayQ.

    The 'kind' field is a logical routing key — workers register handlers
    by kind, so a single queue can process heterogeneous job types.

    'attempts' tracks both successes and failures; the worker checks
    max_retries before deciding to DLQ. We purposely don't split
    success/failure counts because at-least-once means the same job
    could complete after transient failures — exact counts are misleading.

    CWE-770: 'payload' size SHOULD be bounded at the API layer to prevent
    memory exhaustion in Redis stream entries and worker deserialisation.
    """

    id: str = field(default_factory=lambda: f"job_{uuid.uuid4().hex[:16]}")
    kind: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    attempts: int = 0
    max_retries: int = 3
    idempotency_key: str | None = None
    queue: str = "default"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "payload": self.payload,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "idempotency_key": self.idempotency_key,
            "queue": self.queue,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Job":
        return cls(
            id=d["id"],
            kind=d.get("kind", ""),
            payload=d.get("payload", {}),
            status=JobStatus(d.get("status", "pending")),
            created_at=datetime.fromisoformat(d["created_at"])
            if "created_at" in d
            else datetime.utcnow(),
            attempts=d.get("attempts", 0),
            max_retries=d.get("max_retries", 3),
            idempotency_key=d.get("idempotency_key"),
            queue=d.get("queue", "default"),
        )

    def should_dlq(self) -> bool:
        """True when the job has exhausted its retry budget."""
        return self.attempts > self.max_retries
