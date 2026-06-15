import json
from pathlib import Path

from src import jobs_fetcher as jf
from src.shared.json_io import read_json
from tests.helpers.temp_paths import workspace_tmpdir

QLOC_SOURCE = "static_source::static:listing_url:https://qloc.elevato.net/en/"
QLOC_J240 = "https://qloc.elevato.net/en/technical-artist,j,240"
QLOC_J229 = "https://qloc.elevato.net/pl/technical-artist,j,229"


def _qloc_job() -> dict[str, object]:
    return {
        "title": "Technical Artist",
        "company": "QLOC",
        "city": "Warsaw",
        "country": "Poland",
        "workType": "Onsite",
        "contractType": "Full-time",
        "jobLink": QLOC_J240,
        "sector": "Game",
        "source": QLOC_SOURCE,
        "sourceJobId": "elevato:240",
        "postedAt": "2026-06-01",
    }


def _stale_qloc_sheet_job() -> dict[str, object]:
    return {
        **_qloc_job(),
        "company": "Qloc careers",
        "jobLink": QLOC_J229,
        "source": "google_sheets",
        "sourceJobId": "sheet:qloc:technical-artist",
        "postedAt": "2026-05-01",
        "sourceBundleCount": 1,
        "sourceBundle": [
            {
                "source": "google_sheets",
                "sourceJobId": "sheet:qloc:technical-artist",
                "jobLink": QLOC_J229,
                "adapter": "csv",
                "studio": "Qloc careers",
            }
        ],
    }


def _write_fresh_qloc_source_state(out: Path) -> None:
    future = (jf.datetime.now(jf.timezone.utc) + jf.timedelta(hours=2)).isoformat()
    (out / "jobs-source-state.json").write_text(
        json.dumps(
            {
                "updatedAt": jf.now_iso(),
                "sources": {
                    QLOC_SOURCE: {
                        "lastAdapter": "static",
                        "lastStatus": "ok",
                        "lastSuccessAt": jf.now_iso(),
                        "lastKeptCount": 9,
                        "nextEligibleCheckAt": future,
                        "cacheDecision": "skip_fresh",
                        "cacheDecisionReason": "within_freshness_window",
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def _write_source_check_only_qloc_state(out: Path) -> None:
    now = jf.now_iso()
    future = (jf.datetime.now(jf.timezone.utc) + jf.timedelta(hours=2)).isoformat()
    (out / "jobs-source-state.json").write_text(
        json.dumps(
            {
                "updatedAt": now,
                "sources": {
                    QLOC_SOURCE: {
                        "lastAdapter": "static",
                        "lastStatus": "excluded",
                        "lastCheckedAt": now,
                        "lastSeenInFetchAt": now,
                        "lastSuccessAt": "",
                        "lastSuccessfulFetchAt": "",
                        "lastKeptCount": 0,
                        "lastJobsKept": 0,
                        "lastJobsFound": 9,
                        "nextEligibleCheckAt": future,
                        "cacheDecision": "skip_fresh",
                        "cacheDecisionReason": "within_freshness_window",
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def test_run_pipeline_does_not_treat_source_check_only_state_as_fresh() -> None:
    calls = {"count": 0}

    def qloc_loader(**_: object):
        calls["count"] += 1
        return [_qloc_job()]

    with workspace_tmpdir("jobs-fetcher-qloc-source-check-only") as tmp:
        out = Path(tmp)
        (out / "jobs-unified.json").write_text(
            json.dumps([_stale_qloc_sheet_job()]),
            encoding="utf-8",
        )
        _write_source_check_only_qloc_state(out)

        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[(QLOC_SOURCE, qloc_loader)],
            show_progress=False,
        )

        rows = read_json(out / "jobs-unified.json", [])
        report_row = next(
            row for row in (report.get("sourceFamilies") or []) if row.get("name") == QLOC_SOURCE
        )
        assert calls["count"] == 1
        assert report_row["status"] == "ok"
        assert any(str(row.get("jobLink") or "") == QLOC_J240 for row in rows)
        assert not any(str(row.get("jobLink") or "") == QLOC_J229 for row in rows)


def test_run_pipeline_refreshes_fresh_static_source_missing_from_published_feed() -> None:
    calls = {"count": 0}

    def qloc_loader(**_: object):
        calls["count"] += 1
        return [_qloc_job()]

    with workspace_tmpdir("jobs-fetcher-fresh-static-unpublished") as tmp:
        out = Path(tmp)
        (out / "jobs-unified.json").write_text(
            json.dumps(
                [
                    {
                        "title": "Legacy Producer",
                        "company": "Other Studio",
                        "jobLink": "https://example.com/jobs/legacy-producer",
                        "source": "google_sheets",
                    }
                ]
            ),
            encoding="utf-8",
        )
        _write_fresh_qloc_source_state(out)

        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[(QLOC_SOURCE, qloc_loader)],
            show_progress=False,
        )

        rows = read_json(out / "jobs-unified.json", [])
        assert calls["count"] == 1
        assert int(report["summary"].get("outputCount") or 0) == 2
        assert any(str(row.get("jobLink") or "") == QLOC_J240 for row in rows)


def test_run_pipeline_still_skips_fresh_static_source_present_in_published_feed() -> None:
    calls = {"count": 0}

    def qloc_loader(**_: object):
        calls["count"] += 1
        return [_qloc_job()]

    with workspace_tmpdir("jobs-fetcher-fresh-static-published") as tmp:
        out = Path(tmp)
        (out / "jobs-unified.json").write_text(json.dumps([_qloc_job()]), encoding="utf-8")
        _write_fresh_qloc_source_state(out)

        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[(QLOC_SOURCE, qloc_loader)],
            show_progress=False,
        )

        excluded = [
            row for row in (report.get("sourceFamilies") or []) if row.get("name") == QLOC_SOURCE
        ]
        assert calls["count"] == 0
        assert len(excluded) == 1
        assert str(excluded[0].get("cacheDecision") or "") == "skip_fresh"


def test_deduplicate_jobs_prefers_live_elevato_static_over_stale_google_sheets_detail() -> None:
    stale_sheet = _stale_qloc_sheet_job()
    live_static = {
        **_qloc_job(),
        "sourceBundleCount": 1,
        "sourceBundle": [
            {
                "source": QLOC_SOURCE,
                "sourceJobId": "elevato:240",
                "jobLink": QLOC_J240,
                "adapter": "static",
                "studio": "QLOC",
            }
        ],
    }

    rows, stats = jf.deduplicate_jobs([stale_sheet, live_static])

    assert stats["outputCount"] == 1
    assert int(stats.get("mergedBySparseIdentity") or 0) == 1
    payload = rows[0].to_dict()
    assert payload["source"] == QLOC_SOURCE
    assert payload["jobLink"] == QLOC_J240
    assert "technical-artist,j,229" not in json.dumps(payload.get("sourceBundle") or [])
