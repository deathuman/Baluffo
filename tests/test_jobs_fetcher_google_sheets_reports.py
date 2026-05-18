from src import jobs_fetcher as jf


def test_normalize_source_report_row_preserves_google_sheets_redirect_stats() -> None:
    row = jf.normalize_source_report_row(
        {
            "name": "google_sheets",
            "status": "ok",
            "adapter": "csv",
            "stageTimingsMs": {
                "parseCsv": 55,
                "redirectResolve": 91,
                "canonicalization": 120,
            },
            "details": [
                {
                    "adapter": "csv",
                    "studio": "community_sheet",
                    "name": "google_sheets",
                    "status": "ok",
                    "stats": {
                        "parse_csv_ms": 55,
                        "redirect_candidates": 7,
                        "redirect_resolved": 6,
                        "redirect_cache_hits": 2,
                        "redirect_resolve_ms": 91,
                        "title_hydration_candidates": 4,
                        "title_hydration_feed_fetches": 2,
                        "title_hydration_cache_hits": 1,
                        "title_hydration_repaired": 3,
                        "title_hydration_missed": 1,
                        "title_hydration_errors": 0,
                        "title_hydration_ms": 17,
                    },
                }
            ],
        }
    )
    assert (row.get("stageTimingsMs") or {}).get("redirectResolve") == 91
    detail_stats = (row.get("details") or [{}])[0].get("stats") or {}
    assert int(detail_stats.get("redirect_candidates") or 0) == 7
    assert int(detail_stats.get("redirect_cache_hits") or 0) == 2
    assert int(detail_stats.get("title_hydration_candidates") or 0) == 4
    assert int(detail_stats.get("title_hydration_feed_fetches") or 0) == 2
    assert int(detail_stats.get("title_hydration_cache_hits") or 0) == 1
    assert int(detail_stats.get("title_hydration_repaired") or 0) == 3
    assert int(detail_stats.get("title_hydration_missed") or 0) == 1
    assert int(detail_stats.get("title_hydration_errors") or 0) == 0
    assert int(detail_stats.get("title_hydration_ms") or 0) == 17


def test_normalize_source_report_row_preserves_sanitizer_drop_reasons() -> None:
    row = jf.normalize_source_report_row(
        {
            "name": "google_sheets",
            "status": "ok",
            "adapter": "csv",
            "fetchedCount": 3,
            "keptCount": 1,
            "loss": {
                "rawFetched": 3,
                "canonicalDropped": 2,
                "canonicalKept": 1,
                "canonicalDropReasons": {
                    "google_sheets_category_row": 2,
                    "non_job_static_page": 1,
                },
            },
        }
    )

    reasons = (row.get("loss") or {}).get("canonicalDropReasons") or {}
    assert reasons["google_sheets_category_row"] == 2
    assert reasons["non_job_static_page"] == 1
    assert reasons["missing_title"] == 0
