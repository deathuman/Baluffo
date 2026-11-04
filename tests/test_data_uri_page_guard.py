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

from src.jobs.adapters.static_runtime import StaticRunDeps, build_static_source_context
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    build_static_source_runtime_config,
)
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
    def _reject_transport(url: str, timeout_s: int, headers: dict[str, str] | None = None) -> str:
        raise AssertionError(f"data: URI reached the transport: {url[:60]}")

    return StaticHtmlFetcher(
        fetch_text=_reject_transport,
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


def _source_context(pages: list[str]):
    run_deps = StaticRunDeps(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
    )
    fetcher = StaticHtmlFetcher(
        fetch_text=run_deps.fetch_text,
        timeout_s=run_deps.timeout_s,
        retries=run_deps.retries,
        backoff_s=run_deps.backoff_s,
    )
    warnings: list[str] = []
    errors: list[str] = []
    ctx = build_static_source_context(
        run_deps=run_deps,
        runtime_config=build_static_source_runtime_config(1),
        html_fetcher=fetcher,
        source={"name": "CarX", "company": "CarX", "pages": pages},
        selected_source_count=1,
        jobs=[],
        warnings=warnings,
        errors=errors,
        details=[],
    )
    return ctx, warnings, errors


def test_source_context_drops_non_fetchable_pages() -> None:
    ctx, warnings, errors = _source_context(
        ["https://carx-online.com", "data:image/png;base64,AAAA"]
    )

    assert ctx.pages == ["https://carx-online.com"]
    assert any("skipped 1 non-fetchable page URL" in warning for warning in warnings)
    assert errors == []


def test_non_fetchable_fetch_failure_is_skipped_without_payload() -> None:
    ctx, _warnings, errors = _source_context(["https://carx-online.com"])
    payload = "data:image/png;base64," + "A" * 10_000
    ctx.record_static_fetch_failure(
        target_url=payload,
        exc=RuntimeError(f"Network error for {payload}: URL too long"),
    )

    assert errors == []
    assert ctx.stats["non_fetchable_url_errors_skipped"] == 1
    assert all(payload not in warning for warning in ctx.warnings)
    assert any("<inline data URL (" in warning for warning in ctx.warnings)


def test_ssl_failure_is_reportable_as_ssl_not_timeout() -> None:
    ctx, _warnings, _errors = _source_context(["https://careers.liquidswords.com/career"])
    url = "https://careers.liquidswords.com/jobs/1851070-open-application"
    ctx.record_static_fetch_failure(
        target_url=url,
        exc=RuntimeError(
            f"Network error for {url}: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify "
            "failed: Hostname mismatch"
        ),
    )

    assert ctx.entry_report["classification"] == "ssl_certificate_error"
    assert any("Hostname mismatch" in error for error in ctx.errors)


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
    # Assert on the full row id: failure lines lead with the row's canonical id,
    # and matching the bare host as a substring would let a lookalike id
    # (``evil-carx-online.com``) satisfy the assertion
    # (CodeQL py/incomplete-url-substring-sanitization #123/#124).
    assert failures[0].startswith("static:listing_url:https://carx-online.com ")
    assert failures[1].startswith("static:listing_url:https://battery.example ")


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
    assert "static:listing_url:https://carx-online.com" in failures[0]
    assert "data:" in failures[0]
