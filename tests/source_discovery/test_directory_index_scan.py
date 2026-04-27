from __future__ import annotations

from src.source_discovery.directory_index_scan import run_directory_index_scan


def _run_scan(source_text: str = "rows") -> dict[str, object]:
    def append_entry(entry, provider_rows, static_rows, failures):
        if entry.get("invalid"):
            failures.append({"stage": "detail", "name": entry["url"]})
            return True
        provider_rows.append({"adapter": "greenhouse", "slug": entry["studio"]})
        static_rows.append({"adapter": "static", "listing_url": entry["url"]})
        static_rows.append({"adapter": "static", "listing_url": entry["url"]})
        return False

    return run_directory_index_scan(
        source_text=source_text,
        fetch_error="fetch down",
        parse_entries=lambda text: (
            [{"studio": "one", "url": "https://one.example/jobs"}] if text != "empty" else []
        ),
        select_entries=lambda rows: [
            *rows,
            {"studio": "bad", "url": "http://[bad", "invalid": True},
        ],
        append_entry=append_entry,
        dedupe_provider_candidates=lambda rows: rows[:1],
        dedupe_static_candidates=lambda rows: rows[:1],
        build_empty_summary=lambda csv_failures, parse_failures: {
            "csvFetchFailures": csv_failures,
            "parseFailures": parse_failures,
            "rawRows": 0,
            "eligibleRows": 0,
            "invalidUrls": 0,
        },
        build_summary=lambda raw_rows, entries, invalid_count: {
            "rawRows": len(raw_rows),
            "eligibleRows": len(entries),
            "invalidUrls": invalid_count,
            "csvFetchFailures": 0,
            "parseFailures": 0,
        },
        index_fetch_failure=lambda error: {
            "stage": "directory_index_fetch",
            "error": error,
        },
        parse_failure=lambda: {"stage": "directory_parse", "error": "bad headers"},
        completed_identity=lambda entry: entry.get("url", ""),
        batch_timing={"source": "test"},
    )


def test_directory_index_scan_empty_fetch_returns_index_failure_and_empty_progress() -> None:
    result = _run_scan("")

    assert result["providerCandidates"] == []
    assert result["staticCandidates"] == []
    assert result["failures"] == [{"stage": "directory_index_fetch", "error": "fetch down"}]
    assert result["summary"] == {
        "csvFetchFailures": 1,
        "parseFailures": 0,
        "rawRows": 0,
        "eligibleRows": 0,
        "invalidUrls": 0,
    }
    assert result["progress"] == {
        "complete": True,
        "cursor": 0,
        "completedUrlIdentities": [],
    }
    assert result["batchTiming"] == {"source": "test"}


def test_directory_index_scan_parse_failure_records_parse_timing() -> None:
    result = _run_scan("empty")

    assert result["failures"] == [{"stage": "directory_parse", "error": "bad headers"}]
    assert result["summary"]["parseFailures"] == 1
    assert int(result["batchTiming"]["parseMs"]) >= 0
    assert "candidateAnalysisMs" not in result["batchTiming"]


def test_directory_index_scan_appends_dedupes_and_preserves_completed_identities() -> None:
    result = _run_scan()

    assert result["providerCandidates"] == [{"adapter": "greenhouse", "slug": "one"}]
    assert result["staticCandidates"] == [
        {"adapter": "static", "listing_url": "https://one.example/jobs"}
    ]
    assert result["failures"] == [{"stage": "detail", "name": "http://[bad"}]
    assert result["summary"] == {
        "rawRows": 1,
        "eligibleRows": 2,
        "invalidUrls": 1,
        "csvFetchFailures": 0,
        "parseFailures": 0,
    }
    assert result["progress"] == {
        "complete": True,
        "cursor": 2,
        "completedUrlIdentities": ["https://one.example/jobs", "http://[bad"],
    }
    assert int(result["batchTiming"]["parseMs"]) >= 0
    assert int(result["batchTiming"]["candidateAnalysisMs"]) >= 0
