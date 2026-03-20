from src import adapter_audit


def test_bucket_for_result_marks_mixed_success_when_jobs_and_partial_errors_exist() -> None:
    bucket = adapter_audit._bucket_for_result(
        jobs_count=28,
        error_text="",
        details=[
            {"classification": "ok_with_jobs", "keptCount": 27, "fetchedCount": 27},
            {"classification": "parser_stale", "keptCount": 0, "fetchedCount": 0, "error": "no jobs extracted"},
        ],
        partial_errors=["ashby:k-ID: no jobs extracted from ashby page"],
    )
    assert bucket == "mixed-success"


def test_ashby_summary_aggregates_counts(monkeypatch, tmp_path) -> None:
    report_path = tmp_path / "ashby-registry-refresh-report.json"
    report_path.write_text('{"removedCount": 5}', encoding="utf-8")
    monkeypatch.setattr(adapter_audit, "ASHBY_REFRESH_REPORT_PATH", report_path)
    summary = adapter_audit._ashby_summary(
        [
            {"fetchedCount": 27, "keptCount": 27},
            {"fetchedCount": 14, "keptCount": 14},
            {"fetchedCount": 0, "keptCount": 0},
        ]
    )
    assert summary == {
        "configuredCompanyCount": 3,
        "liveNonEmptyBoardCount": 2,
        "rawPostingsCount": 41,
        "keptJobsCount": 41,
        "removedStaleOrEmptyCount": 5,
    }
