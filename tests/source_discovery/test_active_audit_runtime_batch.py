from __future__ import annotations

from src.source_discovery.active_audit_runtime import (
    ActiveAuditCandidateMergeResult,
    ActiveAuditPreparedRows,
    ActiveAuditRecoveryApplicationResult,
    ActiveAuditRecoveryFetchResult,
    ActiveHomepageBatchResult,
    run_active_audit_batch,
)


def test_active_audit_batch_sequences_recovery_probe_and_progress() -> None:
    artifact: dict[str, object] = {"progress": {"batchesCompleted": 0}, "timings": {}}
    completed: set[str] = set()
    fetch_labels: list[str] = []
    summary_updates: list[dict[str, object]] = []
    probed_candidates: list[list[dict[str, object]]] = []
    applied_probe_results: list[object] = []
    appended_timing: list[dict[str, object]] = []
    merged_updates: list[dict[str, object]] = []

    result = run_active_audit_batch(
        artifact=artifact,
        batch_rows=[{"url": "https://one.example"}, {"url": "https://two.example"}],
        cursor=2,
        batch_number=1,
        prepare_rows=lambda rows: ActiveAuditPreparedRows(
            direct_provider_candidates=[{"adapter": "greenhouse", "url": "direct"}],
            homepage_rows=rows,
            rejected_rows=[{"reason": "missing"}],
        ),
        fetch_homepages=lambda rows: [
            {"ok": True, "url": row["url"], "payload": row, "text": "<html></html>"} for row in rows
        ],
        analyze_homepages=lambda results: ActiveHomepageBatchResult(
            provider_candidates=[{"adapter": "lever", "url": "homepage-provider"}],
            static_candidates=[{"adapter": "static", "url": "homepage-static"}],
            primary_recovery_jobs=[{"url": "https://one.example/careers"}],
            secondary_recovery_jobs=[
                {"url": "https://one.example/jobs", "payload": {"homepageUrl": "h1"}},
                {"url": "https://two.example/jobs", "payload": {"homepageUrl": "h2"}},
            ],
            browser_recovery_candidates=[{"url": "https://shell.example"}],
            homepages_fetched=2,
        ),
        fetch_recovery=lambda jobs, label: (
            fetch_labels.append(label)
            or ActiveAuditRecoveryFetchResult(
                results=[{"url": job["url"], "payload": job.get("payload", {})} for job in jobs],
                unique_jobs=len(jobs),
                network_jobs=len(jobs),
            )
        ),
        apply_recovery=lambda results, grouped, finalize: ActiveAuditRecoveryApplicationResult(
            provider_candidates=[] if finalize else [{"adapter": "ashby", "url": "wave1"}],
            static_candidates=[{"adapter": "static", "url": "wave2"}] if finalize else [],
            rejected_rows=[{"reason": "recovery_miss"}] if finalize else [],
            failures=[{"stage": "recovery_fetch"}] if finalize else [],
            pages_fetched=len(results),
            grouped_state={"wave1": True},
            recovered_homepages={"h1"} if not finalize else {"h2"},
        ),
        recovery_homepage_key=lambda job: str(job.get("payload", {}).get("homepageUrl") or ""),
        merge_candidates=lambda direct, provider, static, recovery_provider, recovery_static: (
            ActiveAuditCandidateMergeResult(
                candidates=[*direct, *provider, *static, *recovery_provider, *recovery_static],
                rejected_rows=[{"reason": "bad_provider"}],
            )
        ),
        merge_artifact_updates=lambda candidates, browser, homepage_failures, recovery_failures, rejected: (
            merged_updates.append(
                {
                    "candidates": candidates,
                    "browser": browser,
                    "homepageFailures": homepage_failures,
                    "recoveryFailures": recovery_failures,
                    "rejected": rejected,
                }
            )
        ),
        update_summary=summary_updates.append,
        probe_candidates=lambda candidates: (
            probed_candidates.append(candidates) or [{"candidate": candidates[0]}]
        ),
        apply_probe_results=applied_probe_results.append,
        row_identity=lambda row: str(row.get("url") or ""),
        completed_identities=completed,
        append_timing=appended_timing.append,
    )

    assert fetch_labels == [
        "GameDevMap active dry run careers recovery fetch wave 1",
        "GameDevMap active dry run careers recovery fetch wave 2",
    ]
    assert result.recovered_homepages == {"h1", "h2"}
    assert result.timing["recoverySkippedByWave1"] == 1
    assert result.timing["primaryRecoveryJobs"] == 1
    assert result.timing["secondaryRecoveryJobs"] == 1
    assert summary_updates == [
        {
            "homepageFetchAttempts": 2,
            "homepagesFetched": 2,
            "recoveryFetchAttempts": 2,
            "recoveryUniqueFetchAttempts": 2,
            "recoveryNetworkFetchAttempts": 2,
            "recoveryPagesFetched": 2,
        }
    ]
    assert completed == {"https://one.example", "https://two.example"}
    assert artifact["progress"] == {"batchesCompleted": 1}
    assert probed_candidates == [result.candidates]
    assert applied_probe_results == [[{"candidate": result.candidates[0]}]]
    assert appended_timing == [result.timing]
    assert merged_updates[0]["rejected"] == [
        {"reason": "missing"},
        {"reason": "recovery_miss"},
        {"reason": "bad_provider"},
    ]
