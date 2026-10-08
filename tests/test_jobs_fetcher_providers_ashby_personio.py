"""Tests for jobs fetcher providers Ashby and Personio runtime behavior.

The Ashby cases here pin that the adapter reads the board's posting API. It used
to read the rendered board page and fall back across candidate URLs, because the
page is client-rendered: `jobs.ashbyhq.com/<slug>` serves no `/job/` anchors, so
the page read as empty and registered boards kept 0. The two scenarios those
tests covered are preserved, but they are now settled by resolving the posting
API from the row instead of by walking board URLs:

- a stale `.../<slug>/jobs` board URL still reaches the right board (the slug is
  extracted, so there is no stale-URL fallback to need), and
- the rendered board page is never requested at all.

Measured live on 2026-10-08, both pages and the API carry the same 122 Voodoo
postings and both parse to the same 53 game rows; the API is what answers for
every board, the page only for some.
"""

import json
from collections.abc import Callable
from pathlib import Path
from unittest import mock

import pytest

from src import jobs_fetcher as jf
from src.exceptions import AdapterValidationError

_THAT_GAME_JOB_ID = "7ea5dd25-3fcb-4d42-8217-89dd9b6f5083"


def _ashby_posting_payload(
    job_id: str = _THAT_GAME_JOB_ID, title: str = "Senior 3D Environment Artist"
) -> str:
    return json.dumps(
        {
            "apiVersion": "1",
            "jobs": [
                {
                    "id": job_id,
                    "title": title,
                    "location": "Los Angeles",
                    "employmentType": "FullTime",
                    "isListed": True,
                    "publishedAt": "2026-05-04T11:24:20.290+00:00",
                    "jobUrl": f"https://jobs.ashbyhq.com/thatgamecompany/{job_id}",
                }
            ],
        }
    )


def _patched_ashby_rows(
    source_rows: list[dict[str, object]], fake_fetch: Callable[[str, int], str]
) -> list[dict[str, object]]:
    from src.jobs.adapters.plugins.provider_api import json_feed as json_feed_module

    with (
        mock.patch.object(json_feed_module, "registry_entries", lambda adapter: source_rows),
        mock.patch.object(
            json_feed_module, "fetch_with_retries", lambda url, fetch_text, *_: fetch_text(url, 5)
        ),
        mock.patch.object(json_feed_module, "set_source_diagnostics", lambda name, **kwargs: None),
    ):
        rows = jf.run_ashby_sources_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
    # The runner returns `list[RawJob]` (an untyped `TypedDict` alias), which mypy
    # sees as `Any`; re-wrap so the declared return type holds.
    return [dict(row) for row in rows]


def test_run_ashby_sources_source_reads_the_posting_api_never_the_board_page() -> None:
    """The board page is the thing that reads as empty, so it must not be requested."""
    requested: list[str] = []

    def fake_fetch(url: str, _: int) -> str:
        requested.append(url)
        if url == "https://api.ashbyhq.com/posting-api/job-board/thatgamecompany":
            return _ashby_posting_payload()
        raise AssertionError(f"unexpected url {url}")

    rows = _patched_ashby_rows(
        [
            {
                "name": "thatgamecompany (Ashby)",
                "studio": "thatgamecompany",
                "adapter": "ashby",
                "board_url": "https://jobs.ashbyhq.com/thatgamecompany",
                "careersUrl": "https://thatgamecompany.com/careers/",
                "enabledByDefault": True,
            }
        ],
        fake_fetch,
    )

    assert requested == ["https://api.ashbyhq.com/posting-api/job-board/thatgamecompany"]
    assert len(rows) == 1
    assert str(rows[0].get("title") or "") == "Senior 3D Environment Artist"


def test_run_ashby_sources_source_normalizes_stale_jobs_url_to_the_board_slug() -> None:
    """A stale `.../<slug>/jobs` row still reaches its board, with no URL walking."""

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://api.ashbyhq.com/posting-api/job-board/thatgamecompany":
            return _ashby_posting_payload()
        raise AssertionError(f"unexpected url {url}")

    rows = _patched_ashby_rows(
        [
            {
                "name": "thatgamecompany (Ashby)",
                "studio": "thatgamecompany",
                "adapter": "ashby",
                "board_url": "https://jobs.ashbyhq.com/thatgamecompany/jobs",
                "enabledByDefault": True,
            }
        ],
        fake_fetch,
    )

    assert len(rows) == 1
    assert str(rows[0].get("jobLink") or "").endswith(f"/thatgamecompany/{_THAT_GAME_JOB_ID}")


def test_run_personio_sources_source_classifies_dead_marketing_redirect() -> None:
    from src.jobs.adapters import provider_personio as personio_module

    source_rows = [
        {
            "name": "InnoGames (Personio)",
            "studio": "InnoGames",
            "adapter": "personio",
            "feed_url": "https://innogames.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]
    with (
        mock.patch("src.jobs.adapters.provider_api.registry_entries", return_value=source_rows),
        mock.patch.object(
            personio_module,
            "DISCOVERY_FEED_RECHECK_QUEUE_PATH",
            mock.MagicMock(),
        ) as queue_path,
        mock.patch.object(personio_module, "_append_feed_recheck_queue") as append_queue,
    ):
        jf.SOURCE_DIAGNOSTICS.clear()
        rows = jf.run_personio_sources_source(
            fetch_text=lambda _url, _timeout: (
                "<html><body><h1>HR und Lohnbuchhaltung endlich vereint</h1></body></html>"
            ),
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )
        assert rows == []
        detail = ((jf.SOURCE_DIAGNOSTICS.get("personio_sources") or {}).get("details") or [{}])[0]
        assert str(detail.get("classification") or "") == "site_changed"
        append_queue.assert_called_once_with(
            studio="InnoGames",
            name="InnoGames (Personio)",
            feed_url="https://innogames.jobs.personio.de/xml",
        )
        queue_path.exists.assert_not_called()


def test_personio_append_feed_recheck_queue_is_bounded_and_failure_tolerant(tmp_path) -> None:
    from src.jobs.adapters import provider_personio as personio_module

    queue_path = tmp_path / "discovery-feed-recheck-queue.json"
    with mock.patch.object(personio_module, "DISCOVERY_FEED_RECHECK_QUEUE_PATH", queue_path):
        personio_module._append_feed_recheck_queue(
            studio="Welevel",
            name="Welevel (Personio)",
            feed_url="https://welevel.jobs.personio.de/xml",
        )
        personio_module._append_feed_recheck_queue(
            studio="Welevel",
            name="Welevel (Personio)",
            feed_url="https://welevel.jobs.personio.de/xml",
        )
        personio_module._append_feed_recheck_queue(
            studio="Other", name="Other (Personio)", feed_url="https://other.jobs.personio.de/xml"
        )
        payload = json.loads(queue_path.read_text(encoding="utf-8"))
        assert [row["studio"] for row in payload] == ["Welevel", "Other"]

    # non-list / missing file is tolerated
    queue_path.write_text("not-json", encoding="utf-8")
    with mock.patch.object(personio_module, "DISCOVERY_FEED_RECHECK_QUEUE_PATH", queue_path):
        personio_module._append_feed_recheck_queue(
            studio="X", name="X", feed_url="https://x.jobs.personio.de/xml"
        )


def test_run_personio_sources_source_classifies_rate_limited_errors() -> None:
    from src.jobs.adapters import provider_personio as personio_module

    source_rows = [
        {
            "name": "InnoGames (Personio)",
            "studio": "InnoGames",
            "adapter": "personio",
            "feed_url": "https://innogames.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]
    with (
        mock.patch("src.jobs.adapters.provider_api.registry_entries", return_value=source_rows),
        mock.patch.object(
            personio_module,
            "DISCOVERY_FEED_RECHECK_QUEUE_PATH",
            Path(".tmp") / "personio-rate-limited-queue.json",
        ),
    ):
        jf.SOURCE_DIAGNOSTICS.clear()
        with pytest.raises(AdapterValidationError):
            jf.run_personio_sources_source(
                fetch_text=lambda _url, _timeout: (_ for _ in ()).throw(
                    RuntimeError("HTTP 429 for https://innogames.jobs.personio.de/xml")
                ),
                timeout_s=5,
                retries=0,
                backoff_s=0,
            )
        detail = ((jf.SOURCE_DIAGNOSTICS.get("personio_sources") or {}).get("details") or [{}])[0]
        assert str(detail.get("classification") or "") == "rate_limited"


def test_personio_adapter_skips_recent_rate_limited_source_only() -> None:
    from src.jobs.adapters import provider_api
    from src.jobs.adapters import provider_personio as personio_module

    now = jf.datetime.now(jf.timezone.utc).isoformat()
    registry_rows = [
        {
            "name": "Rate Limited Studio",
            "studio": "Rate Limited Studio",
            "feed_url": "https://example.com/rate.xml",
        },
        {
            "name": "Healthy Studio",
            "studio": "Healthy Studio",
            "feed_url": "https://example.com/ok.xml",
        },
    ]

    def fake_fetch(url: str, _timeout: int) -> str:
        if url.endswith("/ok.xml"):
            return """<?xml version="1.0"?><workzag-jobs><position><id>1</id><name>Engine Programmer</name><office>Remote</office><employmentType>Full-time</employmentType><url>https://example.com/jobs/1</url></position></workzag-jobs>"""
        raise AssertionError(f"unexpected fetch for {url}")

    with (
        mock.patch.object(provider_api, "registry_entries", return_value=registry_rows),
        mock.patch.object(
            personio_module,
            "DISCOVERY_FEED_RECHECK_QUEUE_PATH",
            Path(".tmp") / "personio-rate-limited-queue.json",
        ),
    ):
        rows = provider_api.run_personio_sources_source(
            fetch_text=fake_fetch,
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            source_state_rows={
                "Rate Limited Studio": {
                    "lastError": "HTTP 429 Too Many Requests",
                    "lastFailureAt": now,
                }
            },
        )

    assert len(rows) == 1
    assert rows[0]["title"] == "Engine Programmer"


def test_personio_rate_limit_cooldown_can_be_configured() -> None:
    from src.jobs.adapters import provider_api

    with mock.patch.dict(
        "os.environ", {"BALUFFO_PERSONIO_RATE_LIMIT_COOLDOWN_MINUTES": "15"}, clear=False
    ):
        cutoff = provider_api._personio_rate_limit_cutoff()
    delta_minutes = (jf.datetime.now(jf.timezone.utc) - cutoff).total_seconds() / 60
    assert 14 <= delta_minutes <= 16


def test_personio_429_with_stale_success_queues_feed_recheck() -> None:
    from src.jobs.adapters import provider_api
    from src.jobs.adapters import provider_personio as personio_module

    source_rows = [
        {
            "name": "Welevel (Personio)",
            "studio": "Welevel",
            "adapter": "personio",
            "feed_url": "https://welevel.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]
    stale = (jf.datetime.now(jf.timezone.utc) - jf.timedelta(days=30)).isoformat()
    with (
        mock.patch.object(provider_api, "registry_entries", return_value=source_rows),
        mock.patch.object(personio_module, "_append_feed_recheck_queue") as append_queue,
    ):
        jf.SOURCE_DIAGNOSTICS.clear()
        with pytest.raises(AdapterValidationError):
            jf.run_personio_sources_source(
                fetch_text=lambda _url, _timeout: (_ for _ in ()).throw(
                    RuntimeError("HTTP 429 for https://welevel.jobs.personio.de/xml")
                ),
                timeout_s=5,
                retries=0,
                backoff_s=0,
                source_state_rows={
                    "Welevel (Personio)": {"lastSuccessAt": stale, "lastNonEmptyAt": stale}
                },
            )
        append_queue.assert_called_once_with(
            studio="Welevel",
            name="Welevel (Personio)",
            feed_url="https://welevel.jobs.personio.de/xml",
        )


def test_personio_429_with_recent_success_does_not_queue() -> None:
    from src.jobs.adapters import provider_api
    from src.jobs.adapters import provider_personio as personio_module

    source_rows = [
        {
            "name": "Healthy Studio (Personio)",
            "studio": "Healthy Studio",
            "adapter": "personio",
            "feed_url": "https://healthy.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]
    recent = jf.datetime.now(jf.timezone.utc).isoformat()
    with (
        mock.patch.object(provider_api, "registry_entries", return_value=source_rows),
        mock.patch.object(personio_module, "_append_feed_recheck_queue") as append_queue,
    ):
        jf.SOURCE_DIAGNOSTICS.clear()
        with pytest.raises(AdapterValidationError):
            jf.run_personio_sources_source(
                fetch_text=lambda _url, _timeout: (_ for _ in ()).throw(
                    RuntimeError("HTTP 429 for https://healthy.jobs.personio.de/xml")
                ),
                timeout_s=5,
                retries=0,
                backoff_s=0,
                source_state_rows={
                    "Healthy Studio (Personio)": {"lastSuccessAt": recent, "lastNonEmptyAt": recent}
                },
            )
        append_queue.assert_not_called()


def test_personio_429_cooldown_skip_queues_stale_feed() -> None:
    from src.jobs.adapters import provider_api
    from src.jobs.adapters import provider_personio as personio_module

    now = jf.datetime.now(jf.timezone.utc)
    source_rows = [
        {
            "name": "Welevel (Personio)",
            "studio": "Welevel",
            "adapter": "personio",
            "feed_url": "https://welevel.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]
    stale = (now - jf.timedelta(days=30)).isoformat()
    with (
        mock.patch.object(provider_api, "registry_entries", return_value=source_rows),
        mock.patch.object(personio_module, "_append_feed_recheck_queue") as append_queue,
    ):
        jf.SOURCE_DIAGNOSTICS.clear()
        rows = jf.run_personio_sources_source(
            fetch_text=lambda _url, _timeout: (_ for _ in ()).throw(
                AssertionError("cooldown skip should bypass fetch")
            ),
            timeout_s=5,
            retries=0,
            backoff_s=0,
            source_state_rows={
                "Welevel (Personio)": {
                    "lastError": "HTTP 429 Too Many Requests",
                    "lastFailureAt": now.isoformat(),
                    "lastSuccessAt": stale,
                    "lastNonEmptyAt": stale,
                }
            },
        )
        assert rows == []
        append_queue.assert_called_once_with(
            studio="Welevel",
            name="Welevel (Personio)",
            feed_url="https://welevel.jobs.personio.de/xml",
        )
