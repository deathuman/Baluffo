from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.source_discovery import audit_ledger


def test_audit_ledger_batch_timings_accumulate_ms_fields_only() -> None:
    artifact: dict[str, Any] = {}

    audit_ledger.append_batch_timing(
        artifact,
        {"fetchMs": 10, "probeMs": 5, "candidateCount": 3, "label": "batch"},
    )
    audit_ledger.append_batch_timing(artifact, {"fetchMs": 7, "mergeMs": "4"})

    timings = artifact["timings"]
    assert timings["batches"] == [
        {"fetchMs": 10, "probeMs": 5, "candidateCount": 3, "label": "batch"},
        {"fetchMs": 7, "mergeMs": "4"},
    ]
    assert timings["totalsMs"] == {"fetchMs": 17, "probeMs": 5, "mergeMs": 4}


def test_audit_ledger_failure_aggregation_counts_and_bounds_samples() -> None:
    artifact: dict[str, Any] = {}
    failures = [
        {"stage": "fetch", "error": "timeout", "name": "a"},
        {"stage": "fetch", "error": "timeout", "name": "b"},
        {"stage": "probe", "error": "", "name": "c"},
        {"stage": "fetch", "error": "ignored", "name": "d"},
    ]

    audit_ledger.record_failures(artifact, failures, sample_limit=3)

    assert artifact["failureCounts"] == {"fetch": 3, "probe": 1}
    assert artifact["failureErrorCounts"] == {
        "fetch|timeout": 2,
        "probe|(blank)": 1,
        "fetch|ignored": 1,
    }
    assert len(artifact["failureSamples"]) == 3
    assert artifact["failures"] == artifact["failureSamples"]
    assert audit_ledger.failure_count(artifact) == 4


def test_audit_ledger_freshness_checks_schema_completion_signature_and_ttl() -> None:
    fresh_time = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    artifact = {
        "schemaVersion": 3,
        "updatedAt": fresh_time,
        "progress": {"complete": True},
        "runtime": {"configSignature": {"source": "test"}},
    }

    assert audit_ledger.artifact_signature_matches(
        artifact,
        schema_version=3,
        expected_signature={"source": "test"},
    )
    assert audit_ledger.artifact_is_fresh(
        artifact,
        schema_version=3,
        expected_signature={"source": "test"},
        ttl_minutes=60,
    )
    assert not audit_ledger.artifact_is_fresh(
        {**artifact, "schemaVersion": 2},
        schema_version=3,
        expected_signature={"source": "test"},
        ttl_minutes=60,
    )
    assert not audit_ledger.artifact_is_fresh(
        {**artifact, "progress": {"complete": False}},
        schema_version=3,
        expected_signature={"source": "test"},
        ttl_minutes=60,
    )
    assert not audit_ledger.artifact_is_fresh(
        artifact,
        schema_version=3,
        expected_signature={"source": "other"},
        ttl_minutes=60,
    )
    assert not audit_ledger.artifact_is_fresh(
        {
            **artifact,
            "updatedAt": (datetime.now(UTC) - timedelta(minutes=90)).isoformat(),
        },
        schema_version=3,
        expected_signature={"source": "test"},
        ttl_minutes=60,
    )


def test_audit_ledger_stamp_artifact_size_updates_runtime_and_summary() -> None:
    artifact: dict[str, Any] = {
        "runtime": {"configSignature": {"source": "test"}},
        "summary": {"activeCandidates": 2},
        "activeCandidates": [{"id": "one"}, {"id": "two"}],
    }

    size = audit_ledger.stamp_artifact_size(artifact)

    assert isinstance(size, int)
    assert size > 0
    assert artifact["runtime"]["artifactSizeBytes"] == size
    assert artifact["summary"]["artifactSizeBytes"] == size
    assert artifact["summary"]["activeCandidates"] == 2
