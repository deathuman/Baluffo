from __future__ import annotations

from typing import Any

import pytest

from src.jobs.adapters.plugins.provider_api import greenhouse_runner, teamtailor_runner
from src.jobs.adapters.plugins.provider_api import html_board as html_board_runner
from src.jobs.adapters.plugins.provider_api import json_feed as json_feed_runner
from src.jobs.adapters.plugins.provider_api import oracle_hcm as oracle_hcm_runner


def test_greenhouse_source_boundary_does_not_swallow_unexpected_runtime_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        greenhouse_runner,
        "registry_entries",
        lambda adapter: [{"slug": "buggy-board", "name": "Buggy Board"}],
    )

    def broken_fetch_with_retries(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("unexpected greenhouse provider bug")

    monkeypatch.setattr(greenhouse_runner, "fetch_with_retries", broken_fetch_with_retries)

    with pytest.raises(RuntimeError, match="unexpected greenhouse provider bug"):
        greenhouse_runner._run_greenhouse_boards(
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )


def test_json_feed_source_boundary_does_not_swallow_unexpected_parse_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        json_feed_runner,
        "registry_entries",
        lambda adapter: [{"name": "Buggy Feed", "endpoint": "https://example.com/jobs.json"}],
    )
    monkeypatch.setattr(json_feed_runner, "fetch_with_retries", lambda *_args, **_kwargs: "{}")

    def broken_parse_payload(
        _source: dict[str, object], _payload: object, _studio: str
    ) -> list[dict[str, object]]:
        raise RuntimeError("unexpected json feed parse bug")

    with pytest.raises(RuntimeError, match="unexpected json feed parse bug"):
        json_feed_runner._run_json_feed_sources(
            adapter_name="lever",
            registry_adapter="lever",
            default_error="missing endpoint",
            parse_payload=broken_parse_payload,
            build_url=lambda source: str(source.get("endpoint") or ""),
            payload_count=lambda _payload, parsed: len(parsed),
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )


def test_html_board_source_boundary_does_not_swallow_unexpected_runtime_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        html_board_runner,
        "registry_entries",
        lambda adapter: [{"name": "Buggy Board", "board_url": "https://example.com/jobs"}],
    )

    def broken_fetch_with_retries(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("unexpected html board provider bug")

    monkeypatch.setattr(html_board_runner, "fetch_with_retries", broken_fetch_with_retries)

    with pytest.raises(RuntimeError, match="unexpected html board provider bug"):
        html_board_runner._run_html_board_sources(
            adapter_name="breezy",
            registry_adapter="breezy",
            default_error="missing board_url",
            parse_html=lambda _html, _url, _studio: [],
            build_url=lambda source: str(source.get("board_url") or ""),
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )


def test_oracle_hcm_source_boundary_does_not_swallow_unexpected_runtime_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        oracle_hcm_runner,
        "registry_entries",
        lambda adapter: [
            {
                "name": "Buggy Oracle",
                "base_url": "https://example.fa.ocs.oraclecloud.com",
            }
        ],
    )

    def broken_fetch_with_retries(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("unexpected oracle hcm provider bug")

    monkeypatch.setattr(oracle_hcm_runner, "fetch_with_retries", broken_fetch_with_retries)

    with pytest.raises(RuntimeError, match="unexpected oracle hcm provider bug"):
        oracle_hcm_runner.run_oracle_hcm_sources_source(
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )


def test_teamtailor_detail_boundary_does_not_swallow_unexpected_runtime_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        teamtailor_runner,
        "registry_entries",
        lambda adapter: [
            {
                "name": "Buggy Teamtailor",
                "listing_url": "https://example.teamtailor.com/jobs",
                "base_url": "https://example.teamtailor.com",
            }
        ],
    )

    def fetch_with_retries(url: str, *_args: Any, **_kwargs: Any) -> str:
        if url.endswith("/jobs"):
            return "<html>listing</html>"
        raise RuntimeError("unexpected teamtailor detail provider bug")

    monkeypatch.setattr(teamtailor_runner, "fetch_with_retries", fetch_with_retries)
    monkeypatch.setattr(
        teamtailor_runner,
        "parse_teamtailor_listing_links",
        lambda _html, *, base_url: [f"{base_url}/jobs/1"],
    )

    with pytest.raises(RuntimeError, match="unexpected teamtailor detail provider bug"):
        teamtailor_runner._run_teamtailor_sources(
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )
