from __future__ import annotations

import json
from pathlib import Path

from src.source_discovery.active_audit_runtime import (
    HomepagePageOutcome,
    NoCandidateOutcome,
    active_audit_artifact_counts,
    append_artifact_rows,
    create_active_audit_artifact,
    finalize_active_audit_artifact,
    load_or_initialize_active_audit_artifact,
    merge_rows_by_identity,
    merge_unique_candidate_rows,
    record_failure_rows,
    run_active_homepage_batch,
    save_updated_active_audit_artifact,
)


def _row_url(row: dict[str, object]) -> str:
    return str(row.get("url") or "").strip()


def _load_json_object(path: Path, default: dict[str, object]) -> object:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def test_active_homepage_batch_direct_rows_infer_provider_and_skip_fetch() -> None:
    result = run_active_homepage_batch(
        batch_rows=[{"name": "Direct", "url": "https://direct.example/jobs"}],
        homepage_fetch_results=[],
        row_url=_row_url,
        infer_direct_provider=lambda row: {"adapter": "greenhouse", "url": _row_url(row)},
        fetch_failure_rejection=lambda row, fetch: {"reason": "failed"},
        analyze_homepage=lambda row, url, html: HomepagePageOutcome(),
        handle_no_candidate=lambda row, url, html: NoCandidateOutcome(),
    )

    assert result.provider_candidates == [
        {"adapter": "greenhouse", "url": "https://direct.example/jobs"}
    ]
    assert result.homepages_fetched == 0


def test_active_homepage_batch_fetch_failure_records_failure_and_rejection() -> None:
    failure = {"adapter": "gamedevmap", "stage": "homepage_fetch", "error": "timeout"}
    result = run_active_homepage_batch(
        batch_rows=[],
        homepage_fetch_results=[
            {
                "url": "https://fail.example",
                "payload": {"name": "Fail", "url": "https://fail.example"},
                "ok": False,
                "failure": failure,
            }
        ],
        row_url=_row_url,
        infer_direct_provider=lambda row: None,
        fetch_failure_rejection=lambda row, fetch: {
            "reason": "homepage_fetch_failed",
            "url": _row_url(row),
        },
        analyze_homepage=lambda row, url, html: HomepagePageOutcome(),
        handle_no_candidate=lambda row, url, html: NoCandidateOutcome(),
    )

    assert result.failures == [failure]
    assert result.rejected_rows == [
        {"reason": "homepage_fetch_failed", "url": "https://fail.example"}
    ]
    assert result.homepages_fetched == 0


def test_active_homepage_batch_success_routes_page_candidates() -> None:
    result = run_active_homepage_batch(
        batch_rows=[],
        homepage_fetch_results=[
            {
                "url": "https://studio.example",
                "payload": {"name": "Studio", "url": "https://studio.example"},
                "ok": True,
                "text": "<html>jobs</html>",
            }
        ],
        row_url=_row_url,
        infer_direct_provider=lambda row: None,
        fetch_failure_rejection=lambda row, fetch: {"reason": "failed"},
        analyze_homepage=lambda row, url, html: HomepagePageOutcome(
            provider_candidates=[{"adapter": "lever", "url": url}],
            static_candidates=[{"adapter": "static", "url": f"{url}/careers"}],
            found_candidates=True,
        ),
        handle_no_candidate=lambda row, url, html: NoCandidateOutcome(
            rejected_rows=[{"reason": "should_not_run"}]
        ),
    )

    assert result.provider_candidates == [{"adapter": "lever", "url": "https://studio.example"}]
    assert result.static_candidates == [
        {"adapter": "static", "url": "https://studio.example/careers"}
    ]
    assert result.rejected_rows == []
    assert result.homepages_fetched == 1


def test_active_homepage_batch_no_candidate_queues_recovery_without_rejection() -> None:
    result = run_active_homepage_batch(
        batch_rows=[],
        homepage_fetch_results=[
            {
                "url": "https://quiet.example",
                "payload": {"name": "Quiet", "url": "https://quiet.example"},
                "ok": True,
                "text": "<html>No jobs</html>",
            }
        ],
        row_url=_row_url,
        infer_direct_provider=lambda row: None,
        fetch_failure_rejection=lambda row, fetch: {"reason": "failed"},
        analyze_homepage=lambda row, url, html: HomepagePageOutcome(found_candidates=False),
        handle_no_candidate=lambda row, url, html: NoCandidateOutcome(
            primary_recovery_jobs=[{"url": f"{url}/careers"}],
            browser_recovery_candidates=[{"url": url, "reasonDetail": "js_shell"}],
        ),
    )

    assert result.primary_recovery_jobs == [{"url": "https://quiet.example/careers"}]
    assert result.browser_recovery_candidates == [
        {"url": "https://quiet.example", "reasonDetail": "js_shell"}
    ]
    assert result.rejected_rows == []


def test_active_homepage_batch_no_candidate_can_reject_when_recovery_not_queued() -> None:
    result = run_active_homepage_batch(
        batch_rows=[],
        homepage_fetch_results=[
            {
                "url": "https://quiet.example",
                "payload": {"name": "Quiet", "url": "https://quiet.example"},
                "ok": True,
                "text": "<html>No jobs</html>",
            }
        ],
        row_url=_row_url,
        infer_direct_provider=lambda row: None,
        fetch_failure_rejection=lambda row, fetch: {"reason": "failed"},
        analyze_homepage=lambda row, url, html: HomepagePageOutcome(found_candidates=False),
        handle_no_candidate=lambda row, url, html: NoCandidateOutcome(
            rejected_rows=[{"reason": "no_careers_evidence", "url": url}],
        ),
    )

    assert result.rejected_rows == [
        {"reason": "no_careers_evidence", "url": "https://quiet.example"}
    ]


def test_merge_unique_candidate_rows_uses_caller_dedupe() -> None:
    def unique_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
        seen: set[str] = set()
        output: list[dict[str, object]] = []
        for row in rows:
            key = str(row.get("url") or "")
            if key in seen:
                continue
            seen.add(key)
            output.append(row)
        return output

    rows = merge_unique_candidate_rows(
        [{"url": "https://one.example"}, {"ignored": True}],
        [{"url": "https://one.example"}, {"url": "https://two.example"}],
        unique_rows=unique_rows,
    )

    assert rows == [
        {"url": "https://one.example"},
        {"ignored": True},
        {"url": "https://two.example"},
    ]


def test_merge_rows_by_identity_replaces_duplicate_identity() -> None:
    rows = merge_rows_by_identity(
        [{"sourceId": "a", "value": 1}, {"value": "passthrough"}],
        [{"sourceId": "a", "value": 2}, {"sourceId": "b", "value": 3}],
        identity_fn=lambda row: str(row.get("sourceId") or ""),
    )

    assert rows == [
        {"value": "passthrough"},
        {"sourceId": "a", "value": 2},
        {"sourceId": "b", "value": 3},
    ]


def test_append_rows_and_record_failures_preserve_artifact_state() -> None:
    artifact: dict[str, object] = {"rejectedForActivation": [{"reason": "old"}]}

    append_artifact_rows(artifact, "rejectedForActivation", [{"reason": "new"}])
    record_failure_rows(
        artifact,
        [
            {"stage": "homepage_fetch", "error": "timeout"},
            {"stage": "homepage_fetch", "error": "dns"},
        ],
        sample_limit=1,
    )

    assert artifact["rejectedForActivation"] == [{"reason": "old"}, {"reason": "new"}]
    assert artifact["failureCounts"] == {"homepage_fetch": 2}
    assert artifact["failures"] == [{"stage": "homepage_fetch", "error": "timeout"}]


def test_active_audit_artifact_counts_uses_caller_bucket_names() -> None:
    artifact = {
        "allRows": [
            {"adapter": "greenhouse", "sourceId": "a", "recovered": True},
            {"adapter": "static", "sourceId": "b"},
        ],
        "activeRows": [{"adapter": "greenhouse", "sourceId": "a", "recovered": True}],
        "zeroRows": [{"sourceId": "z"}],
        "rejections": [
            {"reason": "probe_failed", "reasonDetail": "probe_failed"},
            {
                "reason": "no_careers_evidence",
                "reasonDetail": "no_jobish_links",
                "failureBucket": "coverage_miss",
            },
        ],
        "browserRows": [{"url": "https://shell.example"}],
        "failureSamples": [{"stage": "homepage_fetch"}],
        "failureCounts": {"homepage_fetch": 2},
        "browserRecovery": {"processedCount": 3, "activeCandidates": 1},
        "lostRecoveryAudit": {"lostCount": 1},
    }

    counts = active_audit_artifact_counts(
        artifact,
        all_candidates_key="allRows",
        active_candidates_key="activeRows",
        zero_candidates_key="zeroRows",
        rejected_key="rejections",
        browser_candidates_key="browserRows",
        recovered_predicate=lambda row: bool(row.get("recovered")),
        failure_bucket_fn=lambda row: str(row.get("failureBucket") or "technical_failure"),
    )

    assert len(counts.all_candidates) == 2
    assert len(counts.recovered_candidates) == 1
    assert len(counts.recovered_active) == 1
    assert counts.reason_counts == {"probe_failed": 1, "no_careers_evidence": 1}
    assert counts.detail_counts == {"probe_failed": 1, "no_jobish_links": 1}
    assert counts.active_adapter_counts == {"greenhouse": 1}
    assert counts.zero_job_count == 1
    assert counts.failure_count == 2
    assert counts.failure_sample_count == 1
    assert counts.browser_recovery_candidate_count == 1
    assert counts.browser_recovery_processed_count == 3
    assert counts.browser_recovered_active_count == 1
    assert counts.lost_recovered_active_count == 1


def test_create_active_audit_artifact_preserves_caller_runtime_and_buckets() -> None:
    artifact = create_active_audit_artifact(
        schema_version=7,
        run_id="run-1",
        started_at="2026-04-27T10:00:00Z",
        mode="active_test",
        progress={"complete": False, "batchSize": 5},
        runtime={"timeoutSeconds": 10, "configSignature": "sig"},
        list_keys=["completedItems", "activeRows"],
        dict_keys=["failureCounts"],
    )

    assert artifact["schemaVersion"] == 7
    assert artifact["runId"] == "run-1"
    assert artifact["startedAt"] == "2026-04-27T10:00:00Z"
    assert artifact["mode"] == "active_test"
    assert artifact["progress"] == {"complete": False, "batchSize": 5}
    assert artifact["runtime"] == {"timeoutSeconds": 10, "configSignature": "sig"}
    assert artifact["timings"] == {"batches": [], "totalsMs": {}}
    assert artifact["completedItems"] == []
    assert artifact["activeRows"] == []
    assert artifact["failureCounts"] == {}


def test_load_or_initialize_active_audit_artifact_refreshes_existing_state(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "artifact.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schemaVersion": 7,
                "runtime": {"old": True},
                "progress": {"complete": False, "batchSize": 1},
                "activeRows": [{"sourceId": "a"}],
                "failures": [{"stage": "fetch"}],
            }
        ),
        encoding="utf-8",
    )

    artifact = load_or_initialize_active_audit_artifact(
        artifact_path,
        reset=False,
        schema_version=7,
        initial_artifact={"schemaVersion": 7, "activeRows": []},
        runtime_updates={"timeoutSeconds": 20},
        progress_updates={"batchSize": 10},
        list_keys=["activeRows", "failureSamples"],
        dict_keys=["failureCounts"],
        failure_sample_limit=5,
        load_json_object=_load_json_object,
    )

    assert artifact["runtime"] == {"old": True, "timeoutSeconds": 20}
    assert artifact["progress"] == {"complete": False, "batchSize": 10}
    assert artifact["activeRows"] == [{"sourceId": "a"}]
    assert artifact["failureCounts"] == {}
    assert artifact["failureSamples"] == [{"stage": "fetch"}]


def test_load_or_initialize_active_audit_artifact_reset_uses_initial_state(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "artifact.json"
    artifact_path.write_text(json.dumps({"schemaVersion": 7, "activeRows": ["old"]}))

    artifact = load_or_initialize_active_audit_artifact(
        artifact_path,
        reset=True,
        schema_version=7,
        initial_artifact={"schemaVersion": 7, "activeRows": []},
        runtime_updates={},
        progress_updates={},
        list_keys=["activeRows"],
        dict_keys=[],
        failure_sample_limit=5,
        load_json_object=_load_json_object,
    )

    assert artifact == {"schemaVersion": 7, "activeRows": []}
    assert not artifact_path.exists()


def test_finalize_active_audit_artifact_stamps_progress_and_saves(tmp_path: Path) -> None:
    artifact = {"progress": {"cursorPosition": 0}, "summary": {}}
    output_path = tmp_path / "artifact.json"
    summary_calls: list[set[str]] = []

    finalize_active_audit_artifact(
        artifact,
        output_path,
        completed_identities={"b", "a"},
        complete=True,
        completed_cursor_position=3,
        completed_key="completedItems",
        summarize=lambda current, identities: (
            summary_calls.append(set(identities)),
            current["summary"].update({"count": len(identities)}),
        ),
    )

    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved["progress"]["complete"] is True
    assert saved["progress"]["cursorPosition"] == 3
    assert saved["progress"]["completedUrlsCount"] == 2
    assert saved["completedItems"] == ["a", "b"]
    assert saved["summary"]["count"] == 2
    assert saved["summary"]["artifactSizeBytes"] > 0
    assert saved["updatedAt"]
    assert saved["finishedAt"]
    assert summary_calls == [{"a", "b"}]


def test_save_updated_active_audit_artifact_preserves_progress_completion(
    tmp_path: Path,
) -> None:
    artifact = {"progress": {"complete": False}, "summary": {}}
    output_path = tmp_path / "artifact.json"

    save_updated_active_audit_artifact(
        artifact,
        output_path,
        completed_identities={"a"},
        summarize=lambda current, identities: current["summary"].update({"count": len(identities)}),
    )

    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved["progress"] == {"complete": False}
    assert saved["summary"]["count"] == 1
    assert saved["summary"]["artifactSizeBytes"] > 0
    assert saved["updatedAt"]
    assert "finishedAt" not in saved
