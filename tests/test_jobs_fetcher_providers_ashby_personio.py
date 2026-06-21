"""Tests for jobs fetcher providers Ashby and Personio runtime behavior."""

from unittest import mock

import pytest

from src import jobs_fetcher as jf
from src.exceptions import AdapterValidationError


def test_run_ashby_sources_source_falls_back_to_careers_page_when_board_is_stale() -> None:
    from src.jobs.adapters.plugins.provider_api import html_board as html_board_module

    source_rows = [
        {
            "name": "thatgamecompany (Ashby)",
            "studio": "thatgamecompany",
            "adapter": "ashby",
            "board_url": "https://jobs.ashbyhq.com/thatgamecompany/jobs",
            "careersUrl": "https://thatgamecompany.com/careers/",
            "enabledByDefault": True,
        }
    ]

    class _Deps:
        def registry_entries(self, adapter: str):
            assert adapter == "ashby"
            return source_rows

        def fetch_with_retries(
            self, url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float
        ) -> str:
            return fetch_text(url, timeout_s)

        def set_source_diagnostics(self, source_name: str, **kwargs) -> None:
            return None

    deps = _Deps()
    with (
        mock.patch.object(html_board_module, "registry_entries", deps.registry_entries),
        mock.patch.object(html_board_module, "fetch_with_retries", deps.fetch_with_retries),
        mock.patch.object(html_board_module, "set_source_diagnostics", deps.set_source_diagnostics),
    ):

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://jobs.ashbyhq.com/thatgamecompany/jobs":
                return "<html><body><h1>Job not found</h1><a href='/'>View all open positions</a></body></html>"
            if url == "https://jobs.ashbyhq.com/thatgamecompany":
                return "<html><body><h1>Page not found</h1></body></html>"
            if url == "https://thatgamecompany.com/careers/":
                return """
                    <a href="https://thatgamecompany.com/careers/?ashby_jid=7ea5dd25-3fcb-4d42-8217-89dd9b6f5083#/">
                      Senior 3D Environment Artist
                    </a>
                    """
                raise AssertionError(f"unexpected url {url}")

        rows = jf.run_ashby_sources_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 1
        assert str(rows[0].get("title") or "") == "Senior 3D Environment Artist"


def test_run_ashby_sources_source_normalizes_stale_jobs_url_to_board_root() -> None:
    from src.jobs.adapters.plugins.provider_api import html_board as html_board_module

    source_rows = [
        {
            "name": "thatgamecompany (Ashby)",
            "studio": "thatgamecompany",
            "adapter": "ashby",
            "board_url": "https://jobs.ashbyhq.com/thatgamecompany/jobs",
            "enabledByDefault": True,
        }
    ]

    class _Deps:
        def registry_entries(self, adapter: str):
            assert adapter == "ashby"
            return source_rows

        def fetch_with_retries(
            self, url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float
        ) -> str:
            return fetch_text(url, timeout_s)

        def set_source_diagnostics(self, source_name: str, **kwargs) -> None:
            return None

    deps = _Deps()
    with (
        mock.patch.object(html_board_module, "registry_entries", deps.registry_entries),
        mock.patch.object(html_board_module, "fetch_with_retries", deps.fetch_with_retries),
        mock.patch.object(html_board_module, "set_source_diagnostics", deps.set_source_diagnostics),
    ):

        def fake_fetch(url: str, _: int) -> str:
            # The code tries multiple candidate URLs - first the original, then normalized
            if url == "https://jobs.ashbyhq.com/thatgamecompany/jobs":
                # Original URL returns "Job not found" - triggers fallback to next candidate
                return "<html><body><h1>Job not found</h1></body></html>"
            if url == "https://jobs.ashbyhq.com/thatgamecompany":
                # Normalized URL returns actual job
                return """
                    <a href="/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083">
                      Senior 3D Environment Artist
                    </a>
                    """
            raise AssertionError(f"unexpected url {url}")

        rows = jf.run_ashby_sources_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 1
        assert str(rows[0].get("jobLink") or "").endswith(
            "/thatgamecompany/7ea5dd25-3fcb-4d42-8217-89dd9b6f5083"
        )


def test_run_personio_sources_source_classifies_dead_marketing_redirect() -> None:
    source_rows = [
        {
            "name": "InnoGames (Personio)",
            "studio": "InnoGames",
            "adapter": "personio",
            "feed_url": "https://innogames.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]
    with mock.patch("src.jobs.adapters.provider_api.registry_entries", return_value=source_rows):
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
        assert str(detail.get("classification") or "") == "dead_listing_page"


def test_run_personio_sources_source_classifies_rate_limited_errors() -> None:
    source_rows = [
        {
            "name": "InnoGames (Personio)",
            "studio": "InnoGames",
            "adapter": "personio",
            "feed_url": "https://innogames.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]
    with mock.patch("src.jobs.adapters.provider_api.registry_entries", return_value=source_rows):
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

    with mock.patch.object(provider_api, "registry_entries", return_value=registry_rows):
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
