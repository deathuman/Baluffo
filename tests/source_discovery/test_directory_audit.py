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
