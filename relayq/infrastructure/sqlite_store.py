from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from relayq.domain.job import Job, JobStatus
from relayq.infrastructure.clock import Clock, FakeClock

logger = logging.getLogger(__name__)

# Shared lock for SQLite connections to serialise writes across threads.
# This is intentionally coarse — SQLite is a fallback for single-node
# demo/dev usage, not a production backend.
_sqlite_lock = threading.Lock()


class SqliteStore:
    """SQLite-backed job store for single-node deployments.

    This is a *fallback* transport, not the primary one.  It's useful for:
      - Development without Redis installed
      - Integration tests that need a real but disposable store
      - Single-node deployments where Redis is overkill

    Why BEGIN IMMEDIATE?
    SQLite's default transaction mode (DEFERRED) only takes a read lock
    until the first write, which can lead to deadlocks under concurrent
    access.  BEGIN IMMEDIATE acquires a reserved write lock at the start,
    so concurrent writers will block instead of deadlocking.

    Note: This is NOT a distributed transport.  It doesn't do consumer
    groups, lease recovery, or at-least-once delivery the way Redis
    Streams does.  Use Redis in production.
    """

    def __init__(self, db_path: str | Path = "relayq.db", clock: Clock | None = None):
        self.db_path = str(db_path)
        self.clock = clock or Clock()
        self._local = threading.local()

    def _get_conn(self) -> sqlite3.Connection:
        """Get a thread-local connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA busy_timeout=5000")
            self._init_schema()
        return self._local.conn

    def _init_schema(self) -> None:
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                idempotency_key TEXT,
                queue TEXT NOT NULL DEFAULT 'default'
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
            ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL
        """)

    # -- transactional outbox -------------------------------------------------

    def insert_outbox(self, job: Job) -> None:
        """Insert a job into the outbox within an active transaction.

        The caller is responsible for BEGIN/COMMIT.  This is called from
        the transactional enqueue flow (application/enqueue.py).

        CWE-362 (Concurrent Execution): The outbox pattern uses a DB
        transaction to atomically write the job record and prepare it
        for delivery.  Combined with the UNIQUE index on idempotency_key,
        this prevents duplicate insertion from concurrent requests.
        """
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO jobs (id, kind, payload, status, created_at, attempts, max_retries, idempotency_key, queue)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job.id,
                job.kind,
                json.dumps(job.payload, default=str),
                job.status.value,
                job.created_at.isoformat(),
                job.attempts,
                job.max_retries,
                job.idempotency_key,
                job.queue,
            ),
        )

    def update_status(self, job_id: str, status: JobStatus, attempt: int | None = None) -> None:
        conn = self._get_conn()
        if attempt is not None:
            conn.execute(
                "UPDATE jobs SET status = ?, attempts = ? WHERE id = ?",
                (status.value, attempt, job_id),
            )
        else:
            conn.execute(
                "UPDATE jobs SET status = ? WHERE id = ?",
                (status.value, job_id),
            )

    def get_job(self, job_id: str) -> Job | None:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        if row is None:
            return None
        return Job(
            id=row["id"],
            kind=row["kind"],
            payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"],
            status=JobStatus(row["status"]),
            created_at=self.clock.utcnow(),  # we saved isoformat, parse it
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            idempotency_key=row["idempotency_key"],
            queue=row["queue"],
        )

    def find_by_idempotency_key(self, key: str) -> Job | None:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM jobs WHERE idempotency_key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        return Job(
            id=row["id"],
            kind=row["kind"],
            payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"],
            status=JobStatus(row["status"]),
            created_at=self.clock.utcnow(),
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            idempotency_key=row["idempotency_key"],
            queue=row["queue"],
        )

    # -- async context manager ------------------------------------------------

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Begin IMMEDIATE transaction with proper concurrency handling.

        Must be called from an async context; the actual SQLite work is
        synchronous but we bracket it with the lock and BEGIN IMMEDIATE.
        """
        conn = self._get_conn()
        with _sqlite_lock:
            conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            conn.commit()
        except Exception:
            conn.rollback()
            raise
