"""Tests for jobs fetcher pipeline provider-family behavior."""

import json
from unittest import mock

from src import jobs_fetcher as jf
from tests.helpers.concurrency import BlockingActiveCounter


def test_provider_family_json_sources_refresh_only_stale_boards() -> None:
    from src.jobs.adapters.plugins.provider_api import json_feed as json_feed_module

    calls = []
    captured = {}

    def _registry_entries(adapter: str):
        assert adapter == "greenhouse"
        return [
            {
                "name": "Fresh Board",
                "studio": "Fresh Board",
                "endpoint": "https://example.com/fresh.json",
            },
            {
                "name": "Stale Board",
                "studio": "Stale Board",
                "endpoint": "https://example.com/stale.json",
            },
        ]

    def _fetch_with_retries(
        url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float
    ) -> str:
        calls.append(url)
        return json.dumps({"jobs": [{"id": url}]})

    def _set_source_diagnostics(source_name: str, **kwargs) -> None:
        captured["source_name"] = source_name
        captured["kwargs"] = kwargs

    now = jf.datetime.now(jf.timezone.utc)
    state_rows = {
        "Fresh Board": {
            "lastAdapter": "greenhouse",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 2,
            "nextEligibleCheckAt": (now + jf.timedelta(minutes=10)).isoformat(),
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
        "Stale Board": {
            "lastAdapter": "greenhouse",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(hours=3)).isoformat(),
            "lastChangedAt": (now - jf.timedelta(days=2)).isoformat(),
            "lastKeptCount": 2,
        },
    }

    with (
        mock.patch.object(json_feed_module, "registry_entries", side_effect=_registry_entries),
        mock.patch.object(json_feed_module, "fetch_with_retries", side_effect=_fetch_with_retries),
        mock.patch.object(
            json_feed_module,
            "set_source_diagnostics",
            side_effect=_set_source_diagnostics,
        ),
    ):
        rows = json_feed_module._run_json_feed_sources(
            adapter_name="greenhouse",
            registry_adapter="greenhouse",
            default_error="missing endpoint",
            parse_payload=lambda source, payload, studio: [
                {
                    "title": f"{studio} Engineer",
                    "company": studio,
                    "city": "",
                    "country": "Unknown",
                    "workType": "",
                    "contractType": "",
                    "jobLink": f"https://example.com/{str(source.get('name') or '').lower().replace(' ', '-')}",
                    "sector": "Game",
                    "postedAt": "",
                    "sourceJobId": f"greenhouse:{str(source.get('name') or '')}",
                }
            ],
            build_url=lambda source: str(source.get("endpoint") or ""),
            payload_count=lambda payload, parsed: len(parsed),
            fetch_text=lambda url, timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
            source_state_rows=state_rows,
            force_refresh_all=False,
        )
    assert calls == ["https://example.com/stale.json"]
    assert len(rows) == 1
    details = captured["kwargs"]["details"]
    fresh_detail = next(row for row in details if row["name"] == "Fresh Board")
    stale_detail = next(row for row in details if row["name"] == "Stale Board")
    assert fresh_detail["status"] == "excluded"
    assert fresh_detail["cacheDecision"] == "skip_fresh"
    assert int(fresh_detail["durationMs"]) >= 0
    assert stale_detail["status"] == "ok"
    assert stale_detail["cacheDecision"] == "run_now"
    assert stale_detail["providerUrl"] == "https://example.com/stale.json"
    assert int(stale_detail["durationMs"]) >= 0
    assert int(stale_detail["fetchMs"]) >= 0
    assert int(stale_detail["parseMs"]) >= 0


def test_provider_family_revalidate_only_board_skips_fetch_on_not_modified() -> None:
    from src.jobs.adapters.plugins.provider_api import json_feed as json_feed_module

    calls = []
    captured = {}

    def _registry_entries(adapter: str):
        assert adapter == "lever"
        return [
            {
                "name": "Revalidate Board",
                "studio": "Revalidate Board",
                "endpoint": "https://example.com/revalidate.json",
            }
        ]

    def _fetch_with_retries(
        url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float
    ) -> str:
        calls.append(url)
        return "[]"

    def _set_source_diagnostics(source_name: str, **kwargs) -> None:
        captured["kwargs"] = kwargs

    now = jf.datetime.now(jf.timezone.utc)
    state_rows = {
        "Revalidate Board": {
            "lastAdapter": "lever",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=30)).isoformat(),
            "lastChangedAt": (now - jf.timedelta(days=2)).isoformat(),
            "lastKeptCount": 1,
            "lastHttpEtag": "etag-1",
        }
    }

    with (
        mock.patch.object(json_feed_module, "registry_entries", side_effect=_registry_entries),
        mock.patch.object(json_feed_module, "fetch_with_retries", side_effect=_fetch_with_retries),
        mock.patch.object(
            json_feed_module,
            "set_source_diagnostics",
            side_effect=_set_source_diagnostics,
        ),
        mock.patch.object(
            json_feed_module,
            "conditional_revalidate_url",
            return_value={
                "supported": True,
                "notModified": True,
                "statusCode": 304,
                "etag": "etag-1",
                "lastModified": "",
            },
        ),
    ):
        rows = json_feed_module._run_json_feed_sources(
            adapter_name="lever",
            registry_adapter="lever",
            default_error="missing endpoint",
            parse_payload=lambda source, payload, studio: [],
            build_url=lambda source: str(source.get("endpoint") or ""),
            payload_count=lambda payload, parsed: len(parsed),
            fetch_text=lambda url, timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
            source_state_rows=state_rows,
            force_refresh_all=False,
        )
    assert rows == []
    assert calls == []
    details = captured["kwargs"]["details"]
    assert len(details) == 1
    assert details[0]["status"] == "excluded"
    assert details[0]["cacheDecision"] == "revalidate_only"
    assert details[0]["cacheDecisionReason"] == "not_modified_304"
    assert details[0]["httpStatus"] == 304


def test_teamtailor_sources_skip_fresh_listing_without_fetching() -> None:
    from src.jobs.adapters.plugins.provider_api import teamtailor_runner as teamtailor_module

    calls = []
    captured = {}

    def _registry_entries(adapter: str):
        assert adapter == "teamtailor"
        return [
            {
                "name": "Paradox Teamtailor",
                "studio": "Paradox Interactive",
                "listing_url": "https://career.paradoxplaza.com/jobs",
                "base_url": "https://career.paradoxplaza.com",
                "company": "Paradox Interactive",
            }
        ]

    def _fetch_with_retries(
        url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float
    ) -> str:
        calls.append(url)
        return ""

    def _set_source_diagnostics(source_name: str, **kwargs) -> None:
        captured["kwargs"] = kwargs

    now = jf.datetime.now(jf.timezone.utc)
    state_rows = {
        "Paradox Teamtailor": {
            "lastAdapter": "teamtailor",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 3,
            "nextEligibleCheckAt": (now + jf.timedelta(minutes=20)).isoformat(),
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        }
    }

    with (
        mock.patch.object(teamtailor_module, "registry_entries", side_effect=_registry_entries),
        mock.patch.object(teamtailor_module, "fetch_with_retries", side_effect=_fetch_with_retries),
        mock.patch.object(
            teamtailor_module,
            "set_source_diagnostics",
            side_effect=_set_source_diagnostics,
        ),
    ):
        rows = teamtailor_module._run_teamtailor_sources(
            fetch_text=lambda url, timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
            source_state_rows=state_rows,
            force_refresh_all=False,
        )
    assert rows == []
    assert calls == []
    details = captured["kwargs"]["details"]
    assert len(details) == 1
    assert details[0]["status"] == "excluded"
    assert details[0]["cacheDecision"] == "skip_fresh"
    assert details[0]["cacheDecisionReason"] == "within_freshness_window"


def test_teamtailor_sources_fetch_detail_pages_with_bounded_concurrency() -> None:
    from src.jobs.adapters.plugins.provider_api import teamtailor_runner as teamtailor_module

    captured = {}
    max_workers_seen = []

    class FakeExecutor:
        def __init__(self, max_workers: int) -> None:
            max_workers_seen.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def map(self, fn, items):
            return [fn(item) for item in items]

    def _registry_entries(adapter: str):
        assert adapter == "teamtailor"
        return [
            {
                "name": "Concurrent Teamtailor",
                "studio": "Concurrent Studio",
                "listing_url": "https://concurrent.example/jobs",
                "base_url": "https://concurrent.example",
                "company": "Concurrent Studio",
            }
        ]

    def _fetch_with_retries(
        url: str, _fetch_text, _timeout_s: int, _retries: int, _backoff_s: float
    ) -> str:
        return f"<html>{url}</html>"

    def _parse_listing_links(_html: str, *, base_url: str) -> list[str]:
        assert base_url == "https://concurrent.example"
        return [
            "https://concurrent.example/jobs/one",
            "https://concurrent.example/jobs/two",
            "https://concurrent.example/jobs/three",
        ]

    def _parse_jobpostings_from_html(_html: str, **kwargs):
        return [
            {
                "sourceJobId": kwargs["fallback_source_id_prefix"],
                "title": "Gameplay Engineer",
                "company": kwargs["fallback_company"],
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": kwargs["base_url"],
                "sector": "Game",
                "postedAt": "",
            }
        ]

    def _set_source_diagnostics(_source_name: str, **kwargs) -> None:
        captured["details"] = kwargs["details"]

    with (
        mock.patch.object(teamtailor_module, "registry_entries", side_effect=_registry_entries),
        mock.patch.object(teamtailor_module, "fetch_with_retries", side_effect=_fetch_with_retries),
        mock.patch.object(
            teamtailor_module,
            "parse_teamtailor_listing_links",
            side_effect=_parse_listing_links,
        ),
        mock.patch.object(
            teamtailor_module,
            "parse_jobpostings_from_html",
            side_effect=_parse_jobpostings_from_html,
        ),
        mock.patch.object(teamtailor_module, "ThreadPoolExecutor", FakeExecutor),
        mock.patch.object(
            teamtailor_module,
            "set_source_diagnostics",
            side_effect=_set_source_diagnostics,
        ),
    ):
        rows = teamtailor_module._run_teamtailor_sources(
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )

    assert len(rows) == 3
    assert max_workers_seen == [3]
    assert captured["details"][0]["detailFetchConcurrency"] == 6


def test_teamtailor_sources_fetch_sources_with_bounded_concurrency() -> None:
    from src.jobs.adapters.plugins.provider_api import teamtailor_runner as teamtailor_module

    fetches = BlockingActiveCounter(auto_release_at=2)
    captured: dict[str, object] = {}

    def _registry_entries(adapter: str) -> list[dict[str, object]]:
        assert adapter == "teamtailor"
        return [
            {
                "name": f"Concurrent Teamtailor {idx}",
                "studio": f"Concurrent Studio {idx}",
                "listing_url": f"https://concurrent-{idx}.teamtailor.com/jobs",
                "base_url": f"https://concurrent-{idx}.teamtailor.com",
            }
            for idx in range(1, 4)
        ]

    def _fetch_with_retries(
        url: str,
        fetch_text: object,
        timeout_s: int,
        retries: int,
        backoff_s: float,
    ) -> str:
        _ = fetch_text, timeout_s, retries, backoff_s
        fetches.enter()
        try:
            fetches.wait_released()
            return "<html>listing</html>" if url.endswith("/jobs") else "<html>detail</html>"
        finally:
            fetches.exit()

    def _parse_listing_links(listing_html: str, *, base_url: str) -> list[str]:
        _ = listing_html
        return [f"{base_url}/jobs/1"]

    def _parse_jobpostings_from_html(
        html: str,
        *,
        base_url: str,
        fallback_company: str,
        fallback_source_id_prefix: str,
    ) -> list[dict[str, object]]:
        _ = html, fallback_source_id_prefix
        return [
            {
                "sourceJobId": f"{base_url}:1",
                "title": "Engineer",
                "company": fallback_company or "Unknown",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": base_url,
                "sector": "Game",
                "postedAt": "",
            }
        ]

    def _set_source_diagnostics(
        name: str,
        *,
        adapter: str,
        studio: str,
        provider_url: str = "",
        details: list[dict[str, object]],
        partial_errors: list[str],
    ) -> None:
        captured.update(
            {
                "name": name,
                "adapter": adapter,
                "studio": studio,
                "providerUrl": provider_url,
                "details": details,
                "partialErrors": partial_errors,
            }
        )

    with (
        mock.patch.object(teamtailor_module, "registry_entries", side_effect=_registry_entries),
        mock.patch.object(teamtailor_module, "fetch_with_retries", side_effect=_fetch_with_retries),
        mock.patch.object(
            teamtailor_module,
            "parse_teamtailor_listing_links",
            side_effect=_parse_listing_links,
        ),
        mock.patch.object(
            teamtailor_module,
            "parse_jobpostings_from_html",
            side_effect=_parse_jobpostings_from_html,
        ),
        mock.patch.object(
            teamtailor_module, "set_source_diagnostics", side_effect=_set_source_diagnostics
        ),
    ):
        rows = teamtailor_module._run_teamtailor_sources(
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=1,
            backoff_s=0,
        )

    assert fetches.peak > 1
    assert [row["studio"] for row in rows] == [
        "Concurrent Studio 1",
        "Concurrent Studio 2",
        "Concurrent Studio 3",
    ]
    assert [detail["sourceFetchConcurrency"] for detail in captured["details"]] == [3, 3, 3]
    assert [detail["keptCount"] for detail in captured["details"]] == [1, 1, 1]
