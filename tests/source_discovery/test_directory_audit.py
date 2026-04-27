from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from src.source_discovery import directory_audit

from ._helpers import workspace_tmpdir


def test_directory_audit_reuses_fresh_complete_artifact() -> None:
    with workspace_tmpdir("directory-audit-reuse") as root:
        output_path = root / "audit.json"
        artifact = directory_audit.initial_directory_audit_artifact(
            adapter="test_adapter",
            schema_version=1,
            timeout_s=5,
            signature={"config": "same"},
        )
        artifact["progress"] = {"complete": True}
        artifact["updatedAt"] = datetime.now(UTC).isoformat()
        artifact["providerCandidates"] = [{"adapter": "greenhouse", "slug": "studio"}]
        output_path.write_text(json.dumps(artifact), encoding="utf-8")

        loaded, cache_hit = directory_audit.run_directory_audit(
            adapter="test_adapter",
            schema_version=1,
            output_path=output_path,
            ttl_minutes=60,
            signature={"config": "same"},
            timeout_s=5,
            scan=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh artifact should bypass scan")
            ),
        )

        assert cache_hit is True
        assert loaded["providerCandidates"] == [{"adapter": "greenhouse", "slug": "studio"}]


def test_directory_audit_reruns_stale_incomplete_wrong_schema_or_signature_artifacts() -> None:
    cases = [
        {
            "schemaVersion": 0,
            "updatedAt": datetime.now(UTC).isoformat(),
            "progress": {"complete": True},
            "runtime": {"configSignature": {"config": "same"}},
        },
        {
            "schemaVersion": 1,
            "updatedAt": datetime.now(UTC).isoformat(),
            "progress": {"complete": False},
            "runtime": {"configSignature": {"config": "same"}},
        },
        {
            "schemaVersion": 1,
            "updatedAt": datetime.now(UTC).isoformat(),
            "progress": {"complete": True},
            "runtime": {"configSignature": {"config": "other"}},
        },
        {
            "schemaVersion": 1,
            "updatedAt": (datetime.now(UTC) - timedelta(minutes=90)).isoformat(),
            "progress": {"complete": True},
            "runtime": {"configSignature": {"config": "same"}},
        },
    ]

    for index, existing in enumerate(cases):
        with workspace_tmpdir(f"directory-audit-rerun-{index}") as root:
            output_path = root / "audit.json"
            output_path.write_text(json.dumps(existing), encoding="utf-8")

            artifact, cache_hit = directory_audit.run_directory_audit(
                adapter="test_adapter",
                schema_version=1,
                output_path=output_path,
                ttl_minutes=60,
                signature={"config": "same"},
                timeout_s=5,
                scan=lambda _timeout_s: {
                    "providerCandidates": [{"adapter": "greenhouse", "slug": "fresh"}],
                    "staticCandidates": [],
                    "failures": [],
                    "summary": {"eligibleRows": 1},
                    "batchTiming": {"fetchMs": 1},
                    "progress": {"complete": True, "cursor": 1},
                },
            )

            assert cache_hit is False
            assert artifact["providerCandidates"] == [{"adapter": "greenhouse", "slug": "fresh"}]
            assert artifact["progress"]["complete"] is True


def test_directory_audit_writes_counts_timings_failures_and_size() -> None:
    with workspace_tmpdir("directory-audit-write") as root:
        output_path = root / "audit.json"
        failures = [
            {"stage": "website_fetch", "error": "timeout", "name": "a"},
            {"stage": "website_fetch", "error": "timeout", "name": "b"},
            {"stage": "index_fetch", "error": "boom", "name": "c"},
        ]

        artifact, cache_hit = directory_audit.run_directory_audit(
            adapter="test_adapter",
            schema_version=1,
            output_path=output_path,
            ttl_minutes=60,
            signature={"config": "same"},
            timeout_s=5,
            scan=lambda _timeout_s: {
                "providerCandidates": [{"adapter": "greenhouse", "slug": "studio"}],
                "staticCandidates": [{"adapter": "static", "name": "Studio"}],
                "failures": failures,
                "summary": {"eligibleRows": 2},
                "batchTiming": {"fetchMs": 3, "label": "batch"},
                "progress": {"complete": True, "cursor": 2},
            },
            sample_limit=2,
        )

        assert cache_hit is False
        assert artifact["summary"]["providerCandidates"] == 1
        assert artifact["summary"]["staticCandidates"] == 1
        assert artifact["summary"]["failures"] == 3
        assert artifact["failureCounts"] == {"website_fetch": 2, "index_fetch": 1}
        assert len(artifact["failureSamples"]) == 2
        assert artifact["failures"] == artifact["failureSamples"]
        assert artifact["timings"]["totalsMs"]["fetchMs"] == 3
        assert artifact["timings"]["totalsMs"]["totalMs"] >= 0
        assert artifact["summary"]["artifactSizeBytes"] > 0
        assert output_path.exists()


def test_directory_audit_rows_normalize_candidates_and_failures() -> None:
    rows = directory_audit.directory_audit_rows(
        {
            "providerCandidates": [
                {"adapter": "greenhouse", "slug": "same"},
                {"adapter": "greenhouse", "slug": "same"},
            ],
            "staticCandidates": [{"adapter": "static", "pages": ["https://example.com/jobs"]}],
            "failures": [{"stage": "fetch"}],
        }
    )

    assert len(rows[0]) == 1
    assert len(rows[1]) == 1
    assert rows[2] == [{"stage": "fetch"}]


def test_directory_scan_result_rows_preserve_raw_candidate_lists() -> None:
    provider_rows = [{"adapter": "greenhouse", "slug": "same"}]
    static_rows = [{"adapter": "static", "name": "Same"}]
    failures = [{"stage": "fetch"}]

    rows = directory_audit.directory_scan_result_rows(
        {
            "providerCandidates": provider_rows,
            "staticCandidates": static_rows,
            "failures": failures,
        }
    )

    assert rows == (provider_rows, static_rows, failures)
    assert rows[0] is not provider_rows
    assert rows[1] is not static_rows
    assert rows[2] is not failures


def test_directory_scan_result_rows_missing_keys_return_empty_lists() -> None:
    assert directory_audit.directory_scan_result_rows({}) == ([], [], [])


def test_directory_audit_rows_for_method_filters_rows_and_failures() -> None:
    rows = directory_audit.directory_audit_rows_for_method(
        {
            "providerCandidates": [
                {
                    "adapter": "greenhouse",
                    "slug": "seed",
                    "discoveryMethod": "seed_careers_page",
                },
                {"adapter": "lever", "slug": "web", "discoveryMethod": "web_search"},
            ],
            "staticCandidates": [
                {
                    "adapter": "static",
                    "name": "Seed Jobs",
                    "discoveryMethod": "seed_careers_page",
                },
                {"adapter": "static", "name": "Web Jobs", "discoveryMethod": "web_search"},
            ],
            "failures": [
                {"adapter": "seed_careers_page", "stage": "page_fetch"},
                {"adapter": "web_search", "stage": "page_fetch"},
            ],
        },
        "web_search",
    )

    assert rows[0] == [
        {
            "adapter": "lever",
            "slug": "web",
            "discoveryMethod": "web_search",
            "id": "lever:slug:web",
        }
    ]
    assert rows[1] == [
        {
            "adapter": "static",
            "name": "Web Jobs",
            "discoveryMethod": "web_search",
            "id": "static:name:web jobs",
        }
    ]
    assert rows[2] == [{"adapter": "web_search", "stage": "page_fetch"}]


def test_discover_directory_scan_candidates_calls_scan_once_with_timeout() -> None:
    calls: list[int] = []

    def scan(timeout_s: int) -> dict[str, object]:
        calls.append(timeout_s)
        return {
            "providerCandidates": [{"adapter": "greenhouse", "slug": f"studio-{timeout_s}"}],
            "staticCandidates": [],
            "failures": [],
        }

    rows = directory_audit.discover_directory_scan_candidates(
        9,
        scan,
    )

    assert calls == [9]
    assert rows == ([{"adapter": "greenhouse", "slug": "studio-9"}], [], [])


def test_directory_adapter_wrapper_disabled_bypasses_work() -> None:
    logs: list[str] = []

    rows = directory_audit.discover_directory_adapter_candidates(
        5,
        enabled=False,
        disabled_log="Directory disabled.",
        audit_enabled=True,
        run_audit=lambda: (_ for _ in ()).throw(AssertionError("audit should not run")),
        load_cache=lambda: (_ for _ in ()).throw(AssertionError("cache should not load")),
        scan=lambda _timeout_s: (_ for _ in ()).throw(AssertionError("scan should not run")),
        write_cache=lambda *_args: (_ for _ in ()).throw(AssertionError("cache should not write")),
        emit_log=logs.append,
    )

    assert rows == ([], [], [])
    assert logs == ["Directory disabled."]


def test_directory_adapter_wrapper_audit_path_bypasses_cache_and_scan() -> None:
    rows = directory_audit.discover_directory_adapter_candidates(
        5,
        enabled=True,
        disabled_log="Directory disabled.",
        audit_enabled=True,
        run_audit=lambda: (
            {
                "providerCandidates": [{"adapter": "greenhouse", "slug": "studio"}],
                "staticCandidates": [{"adapter": "static", "name": "Studio Jobs"}],
                "failures": [{"stage": "audit_failure"}],
            },
            False,
        ),
        load_cache=lambda: (_ for _ in ()).throw(AssertionError("cache should not load")),
        scan=lambda _timeout_s: (_ for _ in ()).throw(AssertionError("scan should not run")),
        write_cache=lambda *_args: (_ for _ in ()).throw(AssertionError("cache should not write")),
        emit_log=lambda _message: None,
    )

    assert rows == (
        [{"adapter": "greenhouse", "slug": "studio", "id": "greenhouse:slug:studio"}],
        [{"adapter": "static", "name": "Studio Jobs", "id": "static:name:studio jobs"}],
        [{"stage": "audit_failure"}],
    )


def test_directory_adapter_wrapper_cache_path_bypasses_scan() -> None:
    rows = directory_audit.discover_directory_adapter_candidates(
        5,
        enabled=True,
        disabled_log="Directory disabled.",
        audit_enabled=False,
        run_audit=lambda: (_ for _ in ()).throw(AssertionError("audit should not run")),
        load_cache=lambda: (
            [{"adapter": "lever", "slug": "cached"}],
            [{"adapter": "static", "name": "Cached Jobs"}],
            [{"stage": "cached_failure"}],
        ),
        scan=lambda _timeout_s: (_ for _ in ()).throw(AssertionError("scan should not run")),
        write_cache=lambda *_args: (_ for _ in ()).throw(AssertionError("cache should not write")),
        emit_log=lambda _message: None,
    )

    assert rows == (
        [{"adapter": "lever", "slug": "cached"}],
        [{"adapter": "static", "name": "Cached Jobs"}],
        [{"stage": "cached_failure"}],
    )


def test_directory_adapter_wrapper_scan_writes_cache_when_requested() -> None:
    writes: list[tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]] = []
    logs: list[str] = []

    rows = directory_audit.discover_directory_adapter_candidates(
        7,
        enabled=True,
        disabled_log="Directory disabled.",
        audit_enabled=False,
        run_audit=lambda: (_ for _ in ()).throw(AssertionError("audit should not run")),
        load_cache=lambda: None,
        scan=lambda timeout_s: {
            "providerCandidates": [{"adapter": "greenhouse", "slug": f"studio-{timeout_s}"}],
            "staticCandidates": [{"adapter": "static", "name": "Studio Jobs"}],
            "failures": [{"stage": "website_fetch"}],
            "writeCache": True,
        },
        write_cache=lambda provider_rows, static_rows, failures: writes.append(
            (provider_rows, static_rows, failures)
        ),
        emit_log=logs.append,
        scan_summary_log=lambda provider_rows, static_rows, failures: (
            f"provider={len(provider_rows)}, static={len(static_rows)}, failures={len(failures)}"
        ),
    )

    assert rows == (
        [{"adapter": "greenhouse", "slug": "studio-7"}],
        [{"adapter": "static", "name": "Studio Jobs"}],
        [{"stage": "website_fetch"}],
    )
    assert writes == [rows]
    assert logs == ["provider=1, static=1, failures=1"]


def test_directory_adapter_wrapper_scan_skips_cache_write_without_flag() -> None:
    writes: list[object] = []

    rows = directory_audit.discover_directory_adapter_candidates(
        5,
        enabled=True,
        disabled_log="Directory disabled.",
        audit_enabled=False,
        run_audit=lambda: (_ for _ in ()).throw(AssertionError("audit should not run")),
        load_cache=lambda: None,
        scan=lambda _timeout_s: {
            "providerCandidates": [{"adapter": "greenhouse", "slug": "studio"}],
            "staticCandidates": [],
            "failures": [],
        },
        write_cache=lambda *_args: writes.append(_args),
        emit_log=lambda _message: None,
    )

    assert rows == ([{"adapter": "greenhouse", "slug": "studio"}], [], [])
    assert writes == []


def test_directory_audit_report_summary_normalizes_counts_and_boundaries() -> None:
    summary = directory_audit.directory_audit_report_summary(
        {
            "adapter": "gameprog",
            "progress": {"complete": True},
            "summary": {
                "providerCandidates": "2",
                "staticCandidates": None,
                "failures": "bad",
                "teamsRows": "7",
                "websiteFetchJobs": 3,
                "artifactSizeBytes": 0,
            },
            "runtime": {"artifactSizeBytes": 456},
            "timings": {"totalsMs": {"totalMs": "123", "teamsFetchMs": "4", "label": "skip"}},
            "failureCounts": {"website_fetch": "5", "directory_index_fetch": 1},
        },
        adapter="gameprog",
        cache_hit=True,
    )

    assert summary["adapter"] == "gameprog"
    assert summary["cacheHit"] is True
    assert summary["complete"] is True
    assert summary["auditDurationMs"] == 123
    assert summary["providerCandidates"] == 2
    assert summary["staticCandidates"] == 0
    assert summary["failures"] == 0
    assert summary["artifactSizeBytes"] == 456
    assert summary["timingTotalsMs"]["teamsFetchMs"] == 4
    assert summary["teamsRows"] == 7
    assert summary["websiteFetchJobs"] == 3
    assert summary["topFailureBuckets"] == [
        {"key": "website_fetch", "count": 5},
        {"key": "directory_index_fetch", "count": 1},
    ]


def test_directory_audit_tracks_latest_summaries_for_fresh_and_cached_runs() -> None:
    with workspace_tmpdir("directory-audit-latest-summary") as root:
        output_path = root / "audit.json"
        directory_audit.clear_directory_audit_summaries()

        first_artifact, first_cache_hit = directory_audit.run_directory_audit(
            adapter="test_adapter",
            schema_version=1,
            output_path=output_path,
            ttl_minutes=60,
            signature={"config": "same"},
            timeout_s=5,
            scan=lambda _timeout_s: {
                "providerCandidates": [],
                "staticCandidates": [{"adapter": "static", "name": "Studio"}],
                "failures": [{"stage": "website_fetch", "error": "timeout"}],
                "summary": {"eligibleRows": 1, "websiteFetchJobs": 1},
                "batchTiming": {"fetchMs": 3},
                "progress": {"complete": True, "cursor": 1},
            },
        )
        first_summary = directory_audit.latest_directory_audit_summaries()["test_adapter"]

        second_artifact, second_cache_hit = directory_audit.run_directory_audit(
            adapter="test_adapter",
            schema_version=1,
            output_path=output_path,
            ttl_minutes=60,
            signature={"config": "same"},
            timeout_s=5,
            scan=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh artifact should bypass scan")
            ),
        )
        second_summary = directory_audit.latest_directory_audit_summaries()["test_adapter"]

        assert first_cache_hit is False
        assert second_cache_hit is True
        assert second_artifact == first_artifact
        assert first_summary["cacheHit"] is False
        assert second_summary["cacheHit"] is True
        assert second_summary["staticCandidates"] == 1
        assert second_summary["websiteFetchJobs"] == 1

        directory_audit.clear_directory_audit_summaries()
        assert directory_audit.latest_directory_audit_summaries() == {}
