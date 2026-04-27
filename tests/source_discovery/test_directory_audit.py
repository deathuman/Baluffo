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
