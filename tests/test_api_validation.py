"""API request-model validation tests (no Redis required).

The EnqueueRequest model carries the payload-size bound (CWE-770) and the
queue/kind identifier charset rules — the two API-layer limits that keep
crafted jobs from bloating Redis or colliding with RelayQ's key layout.
"""

import pytest
from pydantic import ValidationError

from api.main import EnqueueRequest


def test_valid_enqueue_request_passes():
    req = EnqueueRequest(
        kind="send_email",
        payload={"to": "a@b.c", "body": "hi"},
        queue="default",
        max_retries=3,
    )
    assert req.kind == "send_email"
    assert req.queue == "default"


def test_oversized_payload_rejected():
    with pytest.raises(ValidationError):
        EnqueueRequest(kind="big", payload={"blob": "x" * (1024 * 1024 + 100)})


def test_payload_boundary_accepted():
    # Just under 1 MB serialized must pass.
    req = EnqueueRequest(kind="ok", payload={"blob": "x" * 900_000})
    assert req.payload["blob"]


def test_queue_name_charset_enforced():
    for bad in ("default:evil", "a/b", "sp ace", 'quote"x', ""):
        with pytest.raises(ValidationError):
            EnqueueRequest(kind="k", queue=bad, payload={})


def test_kind_charset_enforced():
    for bad in ("kind:evil", "a/b", "sp ace", ""):
        with pytest.raises(ValidationError):
            EnqueueRequest(kind=bad, payload={})


def test_max_retries_bounds():
    with pytest.raises(ValidationError):
        EnqueueRequest(kind="k", payload={}, max_retries=26)
    with pytest.raises(ValidationError):
        EnqueueRequest(kind="k", payload={}, max_retries=-1)