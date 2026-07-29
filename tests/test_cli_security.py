import pytest

import cli


@pytest.mark.parametrize(
    "url",
    [
        "file:///tmp/relayq.sock",
        "ftp://example.com",
        "https://user:secret@example.com",
        "https://example.com?redirect=http://internal",
        "not-a-url",
    ],
)
def test_api_url_rejects_unsafe_values(url):
    with pytest.raises(ValueError):
        cli._validated_api_url(url)


def test_api_url_accepts_http_and_https():
    assert cli._validated_api_url("http://localhost:8000/") == "http://localhost:8000"
    assert cli._validated_api_url("https://relayq.example.com") == "https://relayq.example.com"


def test_status_quotes_job_identifier(monkeypatch):
    seen = {}

    def fake_request(method, path, body=None):
        seen["path"] = path
        return {"status": "queued"}

    monkeypatch.setattr(cli, "_request", fake_request)

    cli.cmd_status(["../../admin"])

    assert seen["path"] == "/jobs/..%2F..%2Fadmin"
