"""The static flow's conditional GET: it probes once, then asks with validators.

Kept out of ``test_jobs_transport.py`` so that file stays inside its line budget.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.jobs import transport
from src.jobs.adapters.plugins.provider_api.lifecycle import provider_revalidate_not_modified
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.state_source_state import update_source_state_rows
from tests.helpers.local_http_server import local_http_server


class _FakeHTTPError(Exception):
    pass


class _FakeResponse:
    def __init__(self, *, status_code: int, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.url = ""


class _FakeHttpxModule:
    HTTPError = _FakeHTTPError
    Timeout = staticmethod(lambda value: value)

    def __init__(self, client: Any) -> None:
        self._client = client

    def Client(self, **_: Any) -> Any:  # noqa: N802 - mirrors httpx.Client
        return self._client


class _RecordingClient:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.calls: list[tuple[str, str]] = []

    def __enter__(self) -> _RecordingClient:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def request(self, method: str, url: str) -> Any:
        self.calls.append((method, url))
        return self.response


def test_probe_sends_a_head_without_validators(monkeypatch) -> None:
    """No validators are cached yet, so the probe asks plain: same URL, one HEAD."""
    client = _RecordingClient(
        _FakeResponse(
            status_code=200,
            headers={"ETag": '"etag-1"', "Last-Modified": "Mon, 01 Jun 2026 00:00:00 GMT"},
        )
    )
    monkeypatch.setattr(transport, "httpx", _FakeHttpxModule(client))

    result = transport.conditional_revalidate_url("https://example.com/careers", 5, probe=True)

    assert client.calls == [("HEAD", "https://example.com/careers")]
    assert result == {
        "supported": True,
        "notModified": False,
        "statusCode": 200,
        "etag": '"etag-1"',
        "lastModified": "Mon, 01 Jun 2026 00:00:00 GMT",
    }


def test_without_validators_and_without_probe_no_request_is_made(monkeypatch) -> None:
    """Provider callers keep the old no-request behavior when nothing is cached."""
    client = _RecordingClient(_FakeResponse(status_code=200))
    monkeypatch.setattr(transport, "httpx", _FakeHttpxModule(client))

    result = transport.conditional_revalidate_url("https://example.com/feed.json", 5)

    assert client.calls == []
    assert result["supported"] is False


def test_a_full_read_reports_the_validators_the_real_response_carried() -> None:
    """The GET that produced the rows is also the one that can start revalidation.

    Driven through ``default_fetch_text`` against a loopback server so the sink is
    filled by the shipped transport, not by a test double.
    """
    etag = '"feed-v7"'
    last_modified = "Wed, 21 Oct 2026 07:28:00 GMT"
    routes = {
        "/feed.json": (
            200,
            {
                "Content-Type": "application/json",
                "ETag": etag,
                "Last-Modified": last_modified,
            },
            b'{"jobs": []}',
        )
    }

    with local_http_server(routes) as base:
        captured: dict[str, str] = {}
        text = fetch_with_retries(
            f"{base}/feed.json",
            transport.default_fetch_text,
            5,
            retries=0,
            backoff_s=0,
            capture_headers=captured,
        )

    assert text == '{"jobs": []}'
    report: dict[str, Any] = {}
    transport.apply_full_read_response_headers(report, captured)
    assert report == {
        "httpStatus": 200,
        "httpEtag": etag,
        "httpLastModified": last_modified,
    }


def test_one_echo_is_only_an_observation() -> None:
    """A single echo can be a cache miss on the HEAD, so it is recorded, not believed."""
    report: dict[str, Any] = {}
    transport.apply_conditional_revalidate_result(
        report,
        {"statusCode": 200, "notModified": False, "etag": '"listing-v1"'},
        sent_etag='"listing-v1"',
    )

    assert report["httpProbeEchoAt"], "the echo is recorded for the next ask to confirm"
    assert "httpProbeIgnoredAt" not in report
    assert not transport.probe_retirement_active({})


def test_a_changed_answer_breaks_the_streak_but_a_304_clears_the_retirement() -> None:
    """The second ask has to be an echo too: a changed copy starts the count over."""
    changed: dict[str, Any] = {}
    transport.apply_conditional_revalidate_result(
        changed,
        {"statusCode": 200, "notModified": False, "etag": '"listing-v2"'},
        sent_etag='"listing-v1"',
    )
    assert changed["clearHttpProbeEcho"] is True
    assert "httpProbeEchoAt" not in changed

    report: dict[str, Any] = {}
    transport.apply_conditional_revalidate_result(
        report, {"statusCode": 304, "notModified": True}, sent_etag='"listing-v1"'
    )
    assert report["clearHttpProbeIgnored"] is True
    assert report["clearHttpProbeEcho"] is True


def test_only_an_echoed_validator_retires_and_a_304_clears_it() -> None:
    """A changed tag is an honest change, a missing one is ambiguous, and 304 reverses."""
    assert not transport.conditional_probe_ignored(
        {"statusCode": 200, "etag": '"listing-v2"'}, etag='"listing-v1"'
    )
    assert not transport.conditional_probe_ignored({"statusCode": 200, "etag": ""}, etag='"x"')
    assert transport.conditional_probe_ignored(
        {"statusCode": 200, "etag": '"listing-v1"'}, etag='W/"listing-v1"'
    ), "If-None-Match compares weakly, so W/ is the same validator"

    report: dict[str, Any] = {}
    transport.apply_conditional_revalidate_result(report, {"statusCode": 304, "notModified": True})
    assert report["clearHttpProbeIgnored"] is True


def test_a_changed_tag_is_not_overruled_by_an_identical_date() -> None:
    """An edit inside one second keeps Last-Modified; the tag is what proves the change."""
    same_date = "Wed, 21 Oct 2026 07:28:00 GMT"
    assert not transport.conditional_probe_ignored(
        {"statusCode": 200, "etag": '"listing-v2"', "lastModified": same_date},
        etag='"listing-v1"',
        last_modified=same_date,
    ), "a different tag means the copy really changed, whatever the date says"

    # With no tag to ask about, the date is the whole question and the echo is the proof.
    assert transport.conditional_probe_ignored(
        {"statusCode": 200, "etag": "", "lastModified": same_date},
        last_modified=same_date,
    )
    assert not transport.conditional_probe_ignored(
        {"statusCode": 200, "etag": "", "lastModified": "Thu, 22 Oct 2026 00:00:00 GMT"},
        last_modified=same_date,
    )


def test_a_provider_feed_that_echoes_its_tag_stops_being_probed() -> None:
    """The provider probe shares the retirement: a wasted HEAD is not paid on every run."""
    sent: dict[str, Any] = {}

    def fake_revalidate(url: str, timeout_s: int, **kwargs: Any) -> dict[str, Any]:
        sent.update(kwargs)
        return {
            "supported": True,
            "notModified": False,
            "statusCode": 200,
            "etag": '"feed-v1"',
            "lastModified": "",
        }

    report: dict[str, Any] = {"cacheDecision": "revalidate_only"}
    handled = provider_revalidate_not_modified(
        entry_report=report,
        url="https://example.com/feed.json",
        timeout_s=5,
        source_name="Deaf Feed",
        source_state_rows={"Deaf Feed": {"lastHttpEtag": '"feed-v1"'}},
        revalidate_url=fake_revalidate,
    )

    assert handled is False, "a changed feed still reads"
    assert sent["etag"] == '"feed-v1"'
    assert report["httpProbeEchoAt"], "the echoed tag is recorded for the next ask to confirm"
    assert "httpProbeIgnoredAt" not in report, "one ask does not retire the probe"

    calls: list[str] = []

    def recording_revalidate(url: str, timeout_s: int, **kwargs: Any) -> dict[str, Any]:
        calls.append(url)
        return {"supported": True, "notModified": True, "statusCode": 304}

    retired: dict[str, Any] = {"cacheDecision": "revalidate_only"}
    handled = provider_revalidate_not_modified(
        entry_report=retired,
        url="https://example.com/feed.json",
        timeout_s=5,
        source_name="Deaf Feed",
        source_state_rows={
            "Deaf Feed": {
                "lastHttpEtag": '"feed-v1"',
                "lastHttpProbeIgnoredAt": report["httpProbeEchoAt"],
            }
        },
        revalidate_url=recording_revalidate,
    )

    assert calls == [], "a retired feed is read without a HEAD in front of it"
    assert handled is False


def _echo_report(name: str, echo_at: str) -> dict[str, Any]:
    """A provider-shaped report whose single entry carries one echo observation."""
    return {
        "name": name,
        "adapter": "greenhouse",
        "status": "ok",
        "details": [{"name": name, "status": "ok", "httpStatus": 200, "httpProbeEchoAt": echo_at}],
    }


def _apply_state(
    rows: dict[str, Any], report: dict[str, Any], *, finished_at: str
) -> dict[str, Any]:
    return update_source_state_rows(
        source_state_rows=rows,
        source_reports=[report],
        canonical_rows=[],
        finished_at=finished_at,
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=720,
    )


def test_the_state_update_retires_only_a_confirmed_echo() -> None:
    """Two consecutive asks, not two reports: one echo is a candidate on the row."""
    name = "Deaf Feed"
    first_at = datetime.now(UTC).isoformat()
    rows = _apply_state({}, _echo_report(name, first_at), finished_at=first_at)

    assert rows[name]["lastHttpProbeEchoAt"] == first_at
    assert "lastHttpProbeIgnoredAt" not in rows[name], "one echo is not yet proof"

    # The same stamp applied twice is still one ask, not a confirmation.
    rows = _apply_state(rows, _echo_report(name, first_at), finished_at=first_at)
    assert "lastHttpProbeIgnoredAt" not in rows[name]

    second_at = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    rows = _apply_state(rows, _echo_report(name, second_at), finished_at=second_at)

    assert rows[name]["lastHttpProbeIgnoredAt"] == second_at
    assert transport.probe_retirement_active(rows[name])

    cleared = {
        "name": name,
        "adapter": "greenhouse",
        "status": "excluded",
        "details": [
            {
                "name": name,
                "status": "excluded",
                "httpStatus": 304,
                "clearHttpProbeEcho": True,
                "clearHttpProbeIgnored": True,
            }
        ],
    }
    rows = _apply_state(rows, cleared, finished_at=second_at)

    assert "lastHttpProbeEchoAt" not in rows[name]
    assert "lastHttpProbeIgnoredAt" not in rows[name], "a 304 gives the probe back"


def test_a_retirement_expires_after_its_recheck_interval() -> None:
    fresh = datetime.now(UTC).isoformat()
    stale = (
        datetime.now(UTC) - timedelta(seconds=transport.PROBE_RETIRE_RECHECK_S + 60)
    ).isoformat()
    assert transport.probe_retirement_active({"lastHttpProbeIgnoredAt": fresh})
    assert not transport.probe_retirement_active({"lastHttpProbeIgnoredAt": stale})
