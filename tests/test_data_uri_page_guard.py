"""Inline-asset (data:) URI guard: discovery probe, transport refusal, guardrail.

The 2026-09-07 CarX Technologies contamination: a 6.5 MB ``data:image`` href was
harvested into registry ``pages`` and fetched as a listing page every pipeline
pass, failing the source with ``URL too long``. These tests pin the three
guards: the probe never harvests non-http(s) hrefs, the static fetcher refuses
non-http(s) page URLs, and the seed guardrail fails on rows that still embed
data: URIs in their page lists.
"""

from __future__ import annotations

import json
from pathlib import Path

from src.jobs.adapters.static_runtime_support import StaticHtmlFetcher
from src.source_discovery import probe


def test_static_probe_never_harvests_data_uri_hrefs() -> None:
    """A data:image href whose base64 noise contains /jobs/ must be skipped."""
    base = "https://carx-online.com"
    huge_noise = "iVBOR" + "/jobs/4ca716b735288b97" + "A" * 200
    html = (
        "<a href='/careers/'>Careers</a>"
        f'<a href="data:image/png;base64,{huge_noise}">Logo</a>'
        "<a href='/vacancies/qa-lead'>QA Lead</a>"
    )
    evidence = probe.static_probe_evidence(html, base)
    assert all(not url.lower().startswith("data:") for url in evidence.sample_urls)
    assert "https://carx-online.com/vacancies/qa-lead" in evidence.sample_urls


def test_static_probe_skips_other_non_http_schemes() -> None:
    base = "https://studio.example/jobs/"
    html = (
        "<a href='/jobs/backend-engineer'>Backend Engineer</a>"
        "<a href='tel:+1234567890'>Call us</a>"
        "<a href='ftp://studio.example/jobs/backup'>Backup</a>"
    )
    evidence = probe.static_probe_evidence(html, base)
    assert evidence.sample_urls == ("https://studio.example/jobs/backend-engineer",)


def _fetcher(tmp_path: Path) -> StaticHtmlFetcher:
    return StaticHtmlFetcher(
        fetch_text=lambda url, timeout_s, headers=None: (_ for _ in ()).throw(
            AssertionError(f"data: URI reached the transport: {url[:60]}")
        ),
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )


def test_build_request_refuses_data_uri(tmp_path: Path) -> None:
    fetcher = _fetcher(tmp_path)
    assert fetcher.build_request("data:image/png;base64,iVBORw0KGgo=") is None


def test_build_request_refuses_blob_uri(tmp_path: Path) -> None:
    fetcher = _fetcher(tmp_path)
    assert fetcher.build_request("blob:https://studio.example/uuid") is None


def test_build_request_still_accepts_http_urls(tmp_path: Path) -> None:
    fetcher = _fetcher(tmp_path)
    request = fetcher.build_request("https://studio.example/jobs/")
    assert request is not None
    assert request.fetch_url == "https://studio.example/jobs"


def test_list_rows_with_inline_asset_urls_flags_contaminated_rows() -> None:
    from tools.repo_health.source_registry_duplicate_url_policy import (
        list_rows_with_inline_asset_urls,
    )

    rows = [
        {
            "id": "static:listing_url:https://carx-online.com",
            "pages": ["https://carx-online.com", "data:image/png;base64,AAAA"],
            "detailPagesSample": ["https://hh.ru/vacancy/1"],
        },
        {
            "id": "static:listing_url:https://clean.example",
            "pages": ["https://clean.example/jobs"],
        },
        {
            "id": "static:listing_url:https://battery.example",
            "pages": ["https://battery.example"],
            "detailPagesSample": ["data:img/png;base64,BBBB"],
        },
    ]
    failures = list_rows_with_inline_asset_urls(rows)
    assert len(failures) == 2
    assert "carx-online.com" in failures[0]
    assert "battery.example" in failures[1]


def test_check_active_seed_no_inline_asset_urls_on_real_seed(tmp_path: Path) -> None:
    """The real active seed is clean after the 2026-09-07 purge."""
    from tools.repo_health.source_registry_duplicate_url_policy import (
        check_active_seed_no_inline_asset_urls,
    )

    repo_root = Path(__file__).resolve().parents[1]
    assert check_active_seed_no_inline_asset_urls(repo_root=repo_root) == []


def test_check_active_seed_no_inline_asset_urls_fails_on_fixture(tmp_path: Path) -> None:
    from tools.repo_health.source_registry_duplicate_url_policy import (
        check_active_seed_no_inline_asset_urls,
    )

    defaults = tmp_path / "data" / "defaults"
    defaults.mkdir(parents=True)
    rows = [
        {
            "id": "static:listing_url:https://carx-online.com",
            "listing_url": "https://carx-online.com",
            "pages": ["https://carx-online.com", "data:image/png;base64,AAAA"],
        },
    ]
    (defaults / "source-registry-active.seed.json").write_text(json.dumps(rows), encoding="utf-8")
    failures = check_active_seed_no_inline_asset_urls(repo_root=tmp_path)
    assert len(failures) == 1
    assert "carx-online.com" in failures[0]
    assert "data:" in failures[0]
