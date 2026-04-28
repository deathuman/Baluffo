from __future__ import annotations

import json
from pathlib import Path

from src.source_discovery.active_audit_runtime import (
    ActiveAuditRecoveryApplicationResult,
    HomepagePageOutcome,
    NoCandidateOutcome,
    active_audit_artifact_counts,
    append_artifact_rows,
    apply_active_audit_probe_results,
    apply_active_audit_recovery_fetch_results,
    compare_recovered_active_maps,
    create_active_audit_artifact,
    finalize_active_audit_artifact,
    index_rejections_by_identity,
    load_or_initialize_active_audit_artifact,
    merge_active_audit_batch_artifact_updates,
    merge_rows_by_identity,
    merge_unique_candidate_rows,
    prune_rerun_rejections,
    record_failure_rows,
    recovered_active_by_identity,
    rejection_lookup_keys,
    rejection_rerun_key,
    row_identity_keys,
    run_active_audit_cache,
    run_active_homepage_batch,
    save_updated_active_audit_artifact,
    select_rerun_rows,
    validated_active_candidates_from_artifact,
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


def test_merge_active_audit_batch_artifact_updates_uses_configured_buckets() -> None:
    artifact: dict[str, object] = {
        "allRows": [{"url": "https://old.example"}],
        "browserRows": [{"url": "https://shell.example"}],
        "rejections": [{"reason": "old"}],
    }

    def unique_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
        output: dict[str, dict[str, object]] = {}
        for row in rows:
            output[str(row.get("url") or row.get("reason") or len(output))] = row
        return list(output.values())

    merge_active_audit_batch_artifact_updates(
        artifact,
        all_candidates=[
            {"url": "https://old.example", "updated": True},
            {"url": "https://new.example"},
        ],
        browser_recovery_rows=[
            {"url": "https://shell.example", "updated": True},
            {"url": "https://new-shell.example"},
        ],
        homepage_failures=[{"stage": "homepage_fetch", "error": "timeout"}],
        recovery_failures=[{"stage": "recovery_fetch", "error": "dns"}],
        rejected_rows=[{"reason": "new"}],
        all_candidates_key="allRows",
        browser_candidates_key="browserRows",
        rejected_key="rejections",
        unique_rows=unique_rows,
        failure_sample_limit=5,
    )

    assert artifact["allRows"] == [
        {"url": "https://old.example", "updated": True},
        {"url": "https://new.example"},
    ]
    assert artifact["browserRows"] == [
        {"url": "https://shell.example", "updated": True},
        {"url": "https://new-shell.example"},
    ]
    assert artifact["rejections"] == [{"reason": "old"}, {"reason": "new"}]
    assert artifact["failureCounts"] == {"homepage_fetch": 1, "recovery_fetch": 1}


def test_increment_active_audit_summary_accumulates_repeated_batches() -> None:
    from src.source_discovery.active_audit_runtime import increment_active_audit_summary

    artifact: dict[str, object] = {"summary": {"homepageFetchAttempts": 2}}

    increment_active_audit_summary(
        artifact,
        {"homepageFetchAttempts": 3, "recoveryFetchAttempts": 1},
    )
    increment_active_audit_summary(
        artifact,
        {"homepageFetchAttempts": 4, "recoveryFetchAttempts": 2},
    )

    assert artifact["summary"] == {
        "homepageFetchAttempts": 9,
        "recoveryFetchAttempts": 3,
    }


def test_apply_active_audit_probe_results_uses_configured_buckets() -> None:
    class Classification:
        positive_candidates = [{"sourceId": "active", "jobsFound": 3}]
        zero_job_candidates = [{"sourceId": "zero", "jobsFound": 0}]
        rejected_rows = [{"reason": "zero_jobs"}]

    calls: list[object] = []
    artifact: dict[str, object] = {
        "activeRows": [{"sourceId": "active", "old": True}],
        "zeroRows": [],
        "rejections": [],
    }

    apply_active_audit_probe_results(
        artifact,
        [("probe",)],
        classify_probe_results=lambda probe_results, **kwargs: (
            calls.append((probe_results, kwargs)) or Classification()
        ),
        probe_failed_rejection=lambda *_args: {"reason": "probe_failed"},
        zero_jobs_rejection=lambda *_args: {"reason": "zero_jobs"},
        active_key="activeRows",
        zero_candidates_key="zeroRows",
        rejected_key="rejections",
        identity_fn=lambda row: str(row.get("sourceId") or ""),
    )

    assert artifact["activeRows"] == [{"sourceId": "active", "jobsFound": 3}]
    assert artifact["zeroRows"] == [{"sourceId": "zero", "jobsFound": 0}]
    assert artifact["rejections"] == [{"reason": "zero_jobs"}]
    assert calls[0][0] == [("probe",)]
    assert set(calls[0][1]) == {"probe_failed_rejection", "zero_jobs_rejection"}


def test_apply_active_audit_recovery_fetch_results_preserves_group_state() -> None:
    grouped = {
        "https://one.example": {
            "row": {"studio": "One"},
            "attempts": 1,
            "fetched": 1,
            "candidates": 0,
            "failures": 0,
        }
    }

    def apply_payload(
        payload: dict[str, object],
        result: dict[str, object],
        grouped_rows: dict[str, dict[str, object]],
        provider_candidates: list[dict[str, object]],
        static_candidates: list[dict[str, object]],
    ) -> str:
        homepage = str(payload.get("homepageUrl") or "")
        group = grouped_rows.setdefault(homepage, {"attempts": 0, "candidates": 0})
        group["attempts"] = int(group.get("attempts") or 0) + 1
        if bool(result.get("ok")):
            group["candidates"] = int(group.get("candidates") or 0) + 1
            static_candidates.append({"url": result["url"], "homepage": homepage})
            return homepage
        return ""

    def finalize_group(group: dict[str, object]) -> list[dict[str, object]]:
        if int(group.get("candidates") or 0) > 0:
            return []
        return [{"reason": "no_careers_evidence", "studio": group.get("row", {}).get("studio")}]

    wave1 = apply_active_audit_recovery_fetch_results(
        [
            {
                "ok": True,
                "url": "https://one.example/careers",
                "payload": {"homepageUrl": "https://one.example"},
            }
        ],
        grouped=grouped,
        finalize=False,
        apply_payload=apply_payload,
        finalize_group=finalize_group,
    )
    wave2 = apply_active_audit_recovery_fetch_results(
        [],
        grouped=wave1.grouped_state,
        finalize=True,
        apply_payload=apply_payload,
        finalize_group=finalize_group,
    )

    assert isinstance(wave1, ActiveAuditRecoveryApplicationResult)
    assert wave1.pages_fetched == 1
    assert wave1.static_candidates == [
        {"url": "https://one.example/careers", "homepage": "https://one.example"}
    ]
    assert wave1.recovered_homepages == {"https://one.example"}
    assert wave2.rejected_rows == []


def test_apply_active_audit_recovery_fetch_results_finalizes_unrecovered_groups() -> None:
    grouped = {
        "https://quiet.example": {
            "row": {"studio": "Quiet"},
            "attempts": 1,
            "fetched": 1,
            "candidates": 0,
            "failures": 0,
        }
    }

    result = apply_active_audit_recovery_fetch_results(
        [],
        grouped=grouped,
        finalize=True,
        apply_payload=lambda *_args: "",
        finalize_group=lambda group: [
            {
                "reason": "no_careers_evidence",
                "studio": dict(group.get("row") or {}).get("studio"),
            }
        ],
    )

    assert result.grouped_state == grouped
    assert result.rejected_rows == [{"reason": "no_careers_evidence", "studio": "Quiet"}]


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


def test_active_audit_identity_keys_cover_rows_and_rejections() -> None:
    assert row_identity_keys(
        {"name": "Studio"},
        url="https://studio.example",
        entry_url="https://directory.example/studio",
    ) == {
        "url:https://studio.example",
        "entry:https://directory.example/studio",
    }
    assert rejection_rerun_key({"url": "https://studio.example"}) == ("url:https://studio.example")
    assert (
        rejection_rerun_key(
            {"candidate": {"sourceDirectoryEntryUrl": "https://directory.example/studio"}}
        )
        == "entry:https://directory.example/studio"
    )
    assert rejection_rerun_key({"candidate": {"listing_url": "https://jobs.example"}}) == (
        "url:https://jobs.example"
    )
    assert rejection_rerun_key({"candidate": {}}) == ""


def test_select_and_prune_rerun_rows_preserves_unrelated_rejections() -> None:
    artifact = {
        "rejections": [
            {"reason": "probe_failed", "url": "https://one.example"},
            {"reason": "zero_jobs", "url": "https://two.example"},
        ]
    }
    rows = [
        {"url": "https://one.example"},
        {"url": "https://two.example"},
        {"url": "https://three.example"},
    ]

    selected, keys = select_rerun_rows(
        artifact,
        rows,
        {"probe_failed"},
        rejected_key="rejections",
        rejection_key_fn=rejection_rerun_key,
        row_keys_fn=lambda row: row_identity_keys(
            row,
            url=str(row.get("url") or ""),
            entry_url="",
        ),
    )
    prune_rerun_rejections(
        artifact,
        rejected_key="rejections",
        rerun_reasons={"probe_failed"},
        rerun_row_keys=keys,
        rejection_key_fn=rejection_rerun_key,
    )

    assert selected == [{"url": "https://one.example"}]
    assert keys == {"url:https://one.example"}
    assert artifact["rejections"] == [{"reason": "zero_jobs", "url": "https://two.example"}]


def test_index_rejections_by_identity_supports_candidate_and_url_keys() -> None:
    artifact = {
        "rejections": [
            {
                "sourceId": "source:one",
                "url": "https://row.example",
                "sourceDirectoryEntryUrl": "https://directory.example/row",
                "candidate": {
                    "sourceId": "candidate:one",
                    "careersUrl": "https://candidate.example/jobs",
                    "sourceDirectoryEntryUrl": "https://directory.example/candidate",
                },
            }
        ]
    }

    indexed = index_rejections_by_identity(
        artifact,
        rejected_key="rejections",
        lookup_keys_fn=lambda rejection: rejection_lookup_keys(
            rejection,
            candidate_identity_fn=lambda candidate: str(candidate.get("sourceId") or ""),
            candidate_url_key_fn=lambda candidate: (
                f"url:{candidate.get('careersUrl')}" if candidate.get("careersUrl") else ""
            ),
        ),
    )

    assert indexed["source:one"] == artifact["rejections"]
    assert indexed["candidate:one"] == artifact["rejections"]
    assert indexed["url:https://row.example"] == artifact["rejections"]
    assert indexed["url:https://candidate.example/jobs"] == artifact["rejections"]
    assert indexed["entry:https://directory.example/row"] == artifact["rejections"]
    assert indexed["entry:https://directory.example/candidate"] == artifact["rejections"]


def test_recovered_active_mapping_and_compare_preserves_ordered_lost_rows() -> None:
    previous_artifact = {
        "activeRows": [
            {"sourceId": "b", "recovered": True, "name": "B"},
            {"sourceId": "a", "recovered": True, "name": "A"},
            {"sourceId": "ignored", "recovered": False},
        ]
    }
    current_artifact = {"activeRows": [{"sourceId": "b", "recovered": True, "name": "B"}]}
    previous = recovered_active_by_identity(
        previous_artifact,
        active_key="activeRows",
        recovered_predicate=lambda row: bool(row.get("recovered")),
        identity_fn=lambda row: str(row.get("sourceId") or ""),
    )
    current = recovered_active_by_identity(
        current_artifact,
        active_key="activeRows",
        recovered_predicate=lambda row: bool(row.get("recovered")),
        identity_fn=lambda row: str(row.get("sourceId") or ""),
    )
    classifier_calls: list[str] = []

    result = compare_recovered_active_maps(
        previous=previous,
        current=current,
        current_rejections={"a": [{"reason": "probe_failed"}]},
        classify_lost=lambda candidate, rejections: (
            classifier_calls.append(str(candidate.get("sourceId") or "")),
            ("probe_failure", rejections.get(str(candidate.get("sourceId") or ""), [{}])[0]),
        )[1],
        lost_row_builder=lambda row_id, cause, candidate, rejection: {
            "sourceId": row_id,
            "cause": cause,
            "name": candidate.get("name"),
            "matchedCurrentRejection": rejection,
        },
    )

    assert result == {
        "previousRecoveredActiveCount": 2,
        "currentRecoveredActiveCount": 1,
        "lostCount": 1,
        "lossCauseCounts": {"probe_failure": 1},
        "lostCandidates": [
            {
                "sourceId": "a",
                "cause": "probe_failure",
                "name": "A",
                "matchedCurrentRejection": {"reason": "probe_failed"},
            }
        ],
    }
    assert classifier_calls == ["a"]


def test_run_active_audit_cache_reuses_fresh_matching_artifact() -> None:
    calls: list[str] = []
    artifact = {"schemaVersion": 1, "fresh": True}

    result, cache_hit = run_active_audit_cache(
        reset=False,
        has_rerun_reasons=False,
        load_artifact=lambda: artifact,
        signature_matches=lambda row: True,
        is_fresh=lambda row: True,
        refresh=lambda reset: calls.append(f"refresh:{reset}") or {"refreshed": True},
        cache_hit_log=lambda row: "cache hit",
        emit_log_fn=calls.append,
    )

    assert result == artifact
    assert cache_hit is True
    assert calls == ["cache hit"]


def test_run_active_audit_cache_reset_and_rerun_bypass_cache() -> None:
    calls: list[str] = []

    reset_result, reset_cache_hit = run_active_audit_cache(
        reset=True,
        has_rerun_reasons=False,
        load_artifact=lambda: calls.append("load") or {"fresh": True},
        signature_matches=lambda row: True,
        is_fresh=lambda row: True,
        refresh=lambda reset: calls.append(f"refresh:{reset}") or {"reset": reset},
        cache_hit_log=lambda row: "cache hit",
        emit_log_fn=calls.append,
    )
    rerun_result, rerun_cache_hit = run_active_audit_cache(
        reset=False,
        has_rerun_reasons=True,
        load_artifact=lambda: calls.append("load") or {"fresh": True},
        signature_matches=lambda row: True,
        is_fresh=lambda row: True,
        refresh=lambda reset: calls.append(f"refresh:{reset}") or {"reset": reset},
        cache_hit_log=lambda row: "cache hit",
        emit_log_fn=calls.append,
    )

    assert reset_result == {"reset": True}
    assert reset_cache_hit is False
    assert rerun_result == {"reset": False}
    assert rerun_cache_hit is False
    assert calls == ["refresh:True", "refresh:False"]


def test_run_active_audit_cache_signature_mismatch_refreshes_with_reset() -> None:
    calls: list[str] = []

    result, cache_hit = run_active_audit_cache(
        reset=False,
        has_rerun_reasons=False,
        load_artifact=lambda: {"schemaVersion": 1},
        signature_matches=lambda row: False,
        is_fresh=lambda row: False,
        refresh=lambda reset: calls.append(f"refresh:{reset}") or {"reset": reset},
        cache_hit_log=lambda row: "cache hit",
        emit_log_fn=calls.append,
        signature_mismatch_log=lambda row: "signature mismatch",
    )

    assert result == {"reset": True}
    assert cache_hit is False
    assert calls == ["signature mismatch", "refresh:True"]


def test_run_active_audit_cache_stale_artifact_refreshes_without_hit() -> None:
    calls: list[str] = []

    result, cache_hit = run_active_audit_cache(
        reset=False,
        has_rerun_reasons=False,
        load_artifact=lambda: {"schemaVersion": 1},
        signature_matches=lambda row: True,
        is_fresh=lambda row: False,
        refresh=lambda reset: calls.append(f"refresh:{reset}") or {"reset": reset},
        cache_hit_log=lambda row: "cache hit",
        emit_log_fn=calls.append,
    )

    assert result == {"reset": False}
    assert cache_hit is False
    assert calls == ["refresh:False"]


def test_validated_active_candidates_filters_and_stamps_provider_rows() -> None:
    artifact = {
        "activeRows": [
            {
                "adapter": "greenhouse",
                "sourceId": "one",
                "probeStatus": "ok",
                "jobsFound": 3,
            },
            {
                "adapter": "lever",
                "sourceId": "bad-status",
                "probeStatus": "failed",
                "jobsFound": 3,
            },
            {
                "adapter": "ashby",
                "sourceId": "zero",
                "probeStatus": "ok",
                "jobsFound": 0,
            },
            {
                "adapter": "workable",
                "probeStatus": "ok",
                "jobsFound": 2,
            },
            {
                "adapter": "greenhouse",
                "sourceId": "one",
                "probeStatus": "ok",
                "jobsFound": 3,
            },
        ]
    }

    provider_rows, static_rows = validated_active_candidates_from_artifact(
        artifact,
        active_key="activeRows",
        identity_fn=lambda row: str(row.get("sourceId") or ""),
        validation_metadata={"prevalidatedDiscovery": True, "auditValidated": True},
        source_directory="test_directory",
        static_transform=lambda row: row,
        unique_rows=lambda rows: list({str(row.get("sourceId")): row for row in rows}.values()),
    )

    assert provider_rows == [
        {
            "adapter": "greenhouse",
            "sourceId": "one",
            "probeStatus": "ok",
            "jobsFound": 3,
            "prevalidatedDiscovery": True,
            "auditValidated": True,
            "sourceDirectory": "test_directory",
        }
    ]
    assert static_rows == []


def test_validated_active_candidates_routes_static_transform_and_suppression() -> None:
    artifact = {
        "activeRows": [
            {
                "adapter": "static",
                "sourceId": "static-one",
                "probeStatus": "ok",
                "sampleCount": 1,
            },
            {
                "adapter": "static",
                "sourceId": "static-two",
                "probeStatus": "ok",
                "jobsFound": 2,
            },
            {
                "adapter": "greenhouse",
                "sourceId": "provider-one",
                "probeStatus": "ok",
                "jobsFound": 1,
            },
        ]
    }

    provider_rows, static_rows = validated_active_candidates_from_artifact(
        artifact,
        active_key="activeRows",
        identity_fn=lambda row: str(row.get("sourceId") or ""),
        validation_metadata={"prevalidatedDiscovery": True},
        source_directory="test_directory",
        static_transform=lambda row: (
            None if row.get("sourceId") == "static-two" else {**row, "transformed": True}
        ),
        unique_rows=lambda rows: rows,
    )

    assert provider_rows == [
        {
            "adapter": "greenhouse",
            "sourceId": "provider-one",
            "probeStatus": "ok",
            "jobsFound": 1,
            "prevalidatedDiscovery": True,
            "sourceDirectory": "test_directory",
        }
    ]
    assert static_rows == [
        {
            "adapter": "static",
            "sourceId": "static-one",
            "probeStatus": "ok",
            "sampleCount": 1,
            "prevalidatedDiscovery": True,
            "sourceDirectory": "test_directory",
            "transformed": True,
        }
    ]
