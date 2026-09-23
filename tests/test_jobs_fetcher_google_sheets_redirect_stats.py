from __future__ import annotations

from src.jobs.canonicalize_redirects import _google_sheet_redirect_stats
from src.jobs.pipeline_source_results import _apply_csv_stage_timings
from src.shared.fetch_report_normalization_detail import normalize_fetch_report_detail_stats


def test_redirect_transport_stats_survive_report_normalization() -> None:
    source_url = "https://gracklehq.com/rd/1"
    resolved_url = "https://jobs.lever.co/example/1"
    stats = _google_sheet_redirect_stats(
        redirect_candidates=[(0, source_url)],
        resolved_links={0: resolved_url},
        resolver_stats_before={
            "cacheHits": 0,
            "resolvedCount": 0,
            "transportFailures": 0,
            "shortCircuits": 0,
            "unreachableHosts": 0,
        },
        resolver_stats_after={
            "cacheHits": 2,
            "resolvedCount": 1,
            "transportFailures": 32,
            "shortCircuits": 418,
            "unreachableHosts": 1,
        },
        redirect_resolve_ms=1234,
        canonicalize_ms=56,
    )
    detail_rows = [{"stats": {}}]
    stage_timings: dict[str, int] = {}

    _apply_csv_stage_timings(
        detail_rows=detail_rows,
        google_sheet_redirect_stats=stats,
        stage_timings=stage_timings,
    )
    detail_stats = detail_rows[0]["stats"]
    normalized = normalize_fetch_report_detail_stats(detail_stats)

    assert detail_stats["redirect_transport_failures"] == 32
    assert detail_stats["redirect_short_circuits"] == 418
    assert detail_stats["redirect_unreachable_hosts"] == 1
    assert normalized["redirect_transport_failures"] == 32
    assert normalized["redirect_short_circuits"] == 418
    assert normalized["redirect_unreachable_hosts"] == 1
    assert stage_timings["redirectResolve"] == 1234
