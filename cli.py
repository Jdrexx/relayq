#!/usr/bin/env python3
"""RelayQ CLI — minimal command-line interface.

Usage:
    relayq enqueue <queue> <payload_json>
    relayq status <job-id>
    relayq queues
    relayq dlq <queue>
    relayq replay <queue> <job-id>

Environment:
    RELAYQ_API_URL    Base URL for the RelayQ API (default: http://localhost:8000)
"""

from __future__ import annotations

import json
import os
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError

API_URL = os.getenv("RELAYQ_API_URL", "http://localhost:8000")


def _request(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{API_URL}{path}"
    data = json.dumps(body).encode() if body else None
    req = Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as exc:
        body = exc.read().decode()
        try:
            detail = json.loads(body)
        except json.JSONDecodeError:
            detail = body
        print(f"Error {exc.code}: {detail}", file=sys.stderr)
        sys.exit(1)


def cmd_enqueue(args: list[str]):
    if len(args) < 2:
        print("Usage: relayq enqueue <queue> <payload_json>", file=sys.stderr)
        sys.exit(1)
    queue = args[0]
    try:
        payload = json.loads(args[1])
    except json.JSONDecodeError:
        payload = {"data": args[1]}

    result = _request("POST", "/jobs", {
        "kind": "generic",
        "payload": payload,
        "queue": queue,
    })
    print(f"Enqueued job {result['job_id']} (stream: {result['stream_entry_id']})")


def cmd_status(args: list[str]):
    if len(args) < 1:
        print("Usage: relayq status <job-id>", file=sys.stderr)
        sys.exit(1)
    result = _request("GET", f"/jobs/{args[0]}")
    print(json.dumps(result, indent=2))


def cmd_queues(args: list[str]):
    result = _request("GET", "/queues")
    for q in result.get("queues", []):
        print(f"{q['name']:20s} depth={q['depth']:>6d}  dlq={q['dead_letter_count']:>4d}")


def cmd_dlq(args: list[str]):
    if len(args) < 1:
        print("Usage: relayq dlq <queue>", file=sys.stderr)
        sys.exit(1)
    result = _request("GET", f"/queues/{args[0]}/dlq")
    print(f"DLQ for queue '{args[0]}' ({result.get('count', 0)} entries):")
    for entry in result.get("entries", []):
        job = entry.get("job", {})
        print(f"  {job.get('id', '?')}: {job.get('kind', '?')} — {entry.get('reason', '?')}")


def cmd_replay(args: list[str]):
    if len(args) < 2:
        print("Usage: relayq replay <queue> <job-id>", file=sys.stderr)
        sys.exit(1)
    result = _request("POST", f"/queues/{args[0]}/dlq/{args[1]}/replay")
    print(f"Replayed job {result['job_id']} (new stream entry: {result['new_stream_entry_id']})")


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    command = sys.argv[1]
    args = sys.argv[2:]

    commands = {
        "enqueue": cmd_enqueue,
        "status": cmd_status,
        "queues": cmd_queues,
        "dlq": cmd_dlq,
        "replay": cmd_replay,
    }

    if command not in commands:
        print(f"Unknown command: {command}", file=sys.stderr)
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    commands[command](args)


if __name__ == "__main__":
    main()
