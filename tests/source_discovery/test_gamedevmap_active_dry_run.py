from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.source_discovery import active_audit_runtime, recovery_url_planner
from src.source_discovery import gamedevmap_active_dry_run as dry_run
from src.source_discovery.config import DEFAULT_DISCOVERY_CONFIG
from src.source_discovery.directory_page_recovery import (
    build_recovery_fetch_job,
    dedupe_recovery_fetch_jobs,
    plan_recovery_fetch_job_waves,
)

from ._helpers import (
    discovery_config_without_generator_stages,
    discovery_orchestrator,
    mock,
    override_discovery_runtime,
    sd,
    sr,
    workspace_tmpdir,
)
from .gamedevmap_test_helpers import (
    CSV_URL,
    INDEX_URL,
)
from .gamedevmap_test_helpers import (
    gamedevmap_config as _config,
)
from .gamedevmap_test_helpers import (
    gamedevmap_csv_row as _csv_row,
)
from .gamedevmap_test_helpers import (
    gamedevmap_fetcher as _fetcher,
)
from .gamedevmap_test_helpers import (
    gamedevmap_payloads as _payloads,
)
from .gamedevmap_test_helpers import (
    validated_static_candidate as _validated_static_candidate,
)
from .gamedevmap_test_helpers import (
    write_gamedevmap_audit_artifact as _write_audit_artifact,
)


def test_gamedevmap_active_dry_run_writes_partial_batch_without_defers() -> None:
    with workspace_tmpdir("gamedevmap-active-dry-run") as root:
        output_path = root / "dry-run.json"
        calls: list[str] = []

        output = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(),
            fetcher=_fetcher(_payloads(), calls),
            output_path=output_path,
            batch_size=2,
            max_batches=1,
            reset=True,
        )

        saved = json.loads(output_path.read_text(encoding="utf-8"))
        assert output == saved
        assert saved["schemaVersion"] == dry_run.DRY_RUN_SCHEMA_VERSION
        assert saved["progress"]["complete"] is False
        assert saved["progress"]["completedUrlsCount"] == 2
        assert saved["progress"]["batchesCompleted"] == 1
        assert saved["summary"]["eligibleRows"] == 4
        assert saved["summary"]["remainingUrls"] == 2
        assert all(not bool(row.get("deferred")) for row in saved["activeCandidates"])
        assert all("deferReason" not in row for row in saved["activeCandidates"])
        assert CSV_URL in calls


def test_gamedevmap_active_dry_run_resumes_until_complete() -> None:
    with workspace_tmpdir("gamedevmap-active-dry-run-resume") as root:
        output_path = root / "dry-run.json"
        payloads = _payloads()

        first = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(activeAuditRecoveryEscalationEnabled=False),
            fetcher=_fetcher(payloads),
            output_path=output_path,
            batch_size=2,
            max_batches=1,
            reset=True,
        )
        second = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(activeAuditRecoveryEscalationEnabled=False),
            fetcher=_fetcher(payloads),
            output_path=output_path,
            batch_size=2,
            max_batches=0,
            reset=False,
        )

        assert first["progress"]["complete"] is False
        assert second["progress"]["complete"] is True
        assert second["progress"]["completedUrlsCount"] == 4
        assert second["summary"]["remainingUrls"] == 0
        assert second["summary"]["activeCandidates"] == 2
        assert second["summary"]["zeroJobCandidates"] == 1
        assert {"zero_jobs", "no_careers_evidence"} <= {
            str(row.get("reason") or "") for row in second["rejectedForActivation"]
        }


def test_gamedevmap_active_dry_run_reset_starts_over() -> None:
    with workspace_tmpdir("gamedevmap-active-dry-run-reset") as root:
        output_path = root / "dry-run.json"
        payloads = _payloads()

        sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(),
            fetcher=_fetcher(payloads),
            output_path=output_path,
            batch_size=2,
            max_batches=1,
            reset=True,
        )
        reset_output = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(),
            fetcher=_fetcher(payloads),
            output_path=output_path,
            batch_size=1,
            max_batches=1,
            reset=True,
        )

        assert reset_output["progress"]["completedUrlsCount"] == 1
        assert reset_output["progress"]["batchesCompleted"] == 1
        assert reset_output["summary"]["remainingUrls"] == 3


def test_gamedevmap_active_dry_run_records_probe_failures() -> None:
    csv_text = (
        "Organization,URL,City,State/Province,Country/Region,Map Def,Category,Comments,"
        "Updated By,Bluesky,AI Response\n"
        "Bad Board,https://boards.greenhouse.io/bad-board,Rome,Lazio,Italy,Rome,"
        "Developer,Verified gaming studio.,,,Correct (Gaming)\n"
    )
    payloads = {CSV_URL: csv_text}

    with workspace_tmpdir("gamedevmap-active-dry-run-failure") as root:
        output = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(allowed_categories=["Developer"]),
            fetcher=_fetcher(payloads),
            output_path=root / "dry-run.json",
            batch_size=10,
            max_batches=0,
            reset=True,
        )

    assert output["progress"]["complete"] is True
    assert output["summary"]["probeFailures"] == 1
    assert output["summary"]["activeCandidates"] == 0
    assert output["rejectedForActivation"][0]["reason"] == "probe_failed"


def test_gamedevmap_no_careers_recovery_urls_are_bounded() -> None:
    primary_jobs, secondary_jobs = plan_recovery_fetch_job_waves(
        page_url="https://recover.example.com",
        html='<html><a href="/jobs">Jobs</a><a href="/careers">Careers</a></html>',
        primary_paths=dry_run.PRIMARY_RECOVERY_PATHS,
        secondary_paths=dry_run.SECONDARY_RECOVERY_PATHS,
        payload_factory=lambda recovery_url, wave: {"url": recovery_url, "wave": wave},
        name_factory=lambda recovery_url, _wave: recovery_url,
        adapter="gamedevmap",
        failure_stage="gamedevmap_recovery_fetch",
        blocked_hosts=dry_run.SOCIAL_PROFILE_HOSTS | dry_run.THIRD_PARTY_PROFILE_HOSTS,
        html_url_candidate_fn=recovery_url_planner.html_url_candidates,
    )

    assert [job["url"] for job in [*primary_jobs, *secondary_jobs]] == [
        "https://recover.example.com/jobs",
        "https://recover.example.com/careers",
        "https://recover.example.com/join-us",
        "https://recover.example.com/work-with-us",
        "https://recover.example.com/company/careers",
        "https://recover.example.com/about/careers",
    ]
    assert plan_recovery_fetch_job_waves(
        page_url="https://linktr.ee/studio",
        html='<a href="/jobs">Jobs</a>',
        payload_factory=lambda recovery_url, wave: {"url": recovery_url, "wave": wave},
        name_factory=lambda recovery_url, _wave: recovery_url,
        adapter="gamedevmap",
        failure_stage="gamedevmap_recovery_fetch",
        blocked_hosts=dry_run.SOCIAL_PROFILE_HOSTS | dry_run.THIRD_PARTY_PROFILE_HOSTS,
    ) == ([], [])


def test_gamedevmap_active_audit_uses_split_fetch_limits() -> None:
    csv_text = _csv_row("Recover Studio", "https://recover.example.com")
    config = _config(allowed_categories=["Developer"])
    gamedevmap_cfg = dict(config["gamedevmap"])
    gamedevmap_cfg.update(
        {
            "activeAuditHomepageFetchConcurrency": 11,
            "activeAuditRecoveryFetchConcurrency": 13,
            "activeAuditRecoveryPerHostConcurrency": 5,
            "activeAuditRecoveryTimeoutSeconds": 3,
        }
    )
    config["gamedevmap"] = gamedevmap_cfg
    calls: list[dict[str, object]] = []

    def fake_fetch_pages(timeout_s, jobs, **kwargs):
        calls.append(
            {
                "timeout": timeout_s,
                "total": kwargs["total_concurrency"],
                "perHost": kwargs["per_host_concurrency"],
                "label": kwargs["progress_label"],
                "emitLog": kwargs.get("emit_progress_log", True),
                "jobs": len(jobs),
            }
        )
        return [
            {
                "job": job,
                "payload": job.get("payload"),
                "url": job.get("url"),
                "ok": True,
                "text": "<html>No openings here</html>",
                "error": "",
                "failure": None,
            }
            for job in jobs
        ]

    with workspace_tmpdir("gamedevmap-split-fetch-limits") as root:
        with mock.patch.object(dry_run, "fetch_directory_pages", side_effect=fake_fetch_pages):
            sd.run_gamedevmap_active_source_dry_run(
                timeout_s=8,
                config=config,
                fetcher=_fetcher({CSV_URL: csv_text}),
                output_path=root / "dry-run.json",
                batch_size=10,
                reset=True,
            )

    assert calls[0] == {
        "timeout": 8,
        "total": 11,
        "perHost": 2,
        "label": "GameDevMap active dry run homepage fetch",
        "emitLog": False,
        "jobs": 1,
    }
    assert calls[1]["timeout"] == 3
    assert calls[1]["total"] == 13
    assert calls[1]["perHost"] == 5


def test_gamedevmap_recovery_dedupe_fans_out_result_to_requesters() -> None:
    row_a = {"studio": "Studio A", "url": "https://a.example.com"}
    row_b = {"studio": "Studio B", "url": "https://b.example.com"}
    jobs = [
        build_recovery_fetch_job(
            recovery_url="https://shared.example.com/jobs",
            payload={
                "row": row_a,
                "homepageUrl": "https://a.example.com",
                "homepageReasonDetail": "no_jobish_links",
                "recoverySource": "same_party_recovery_url",
                "recoveryWave": 1,
            },
            name="Studio A recovery https://shared.example.com/jobs",
            adapter="gamedevmap",
            failure_stage="gamedevmap_recovery_fetch",
        ),
        build_recovery_fetch_job(
            recovery_url="https://shared.example.com/jobs",
            payload={
                "row": row_b,
                "homepageUrl": "https://b.example.com",
                "homepageReasonDetail": "no_jobish_links",
                "recoverySource": "same_party_recovery_url",
                "recoveryWave": 1,
            },
            name="Studio B recovery https://shared.example.com/jobs",
            adapter="gamedevmap",
            failure_stage="gamedevmap_recovery_fetch",
        ),
    ]
    deduped = dedupe_recovery_fetch_jobs(jobs)
    result = {
        "payload": deduped[0]["payload"],
        "url": "https://shared.example.com/jobs",
        "ok": True,
        "text": '<html><a href="/job/game-engineer">Game Engineer</a></html>',
    }

    provider, static, rejected, _failures, fetched, _groups, recovered = (
        dry_run._apply_recovery_results(
            recovery_fetch_results=[result],
            index_url=INDEX_URL,
        )
    )

    assert len(deduped) == 1
    assert fetched == 1
    assert provider == []
    assert len(static) == 2
    assert rejected == []
    assert recovered == {"https://a.example.com", "https://b.example.com"}


def test_gamedevmap_no_careers_recovery_uses_shared_wave_planning() -> None:
    row = {
        "studio": "Shell Studio",
        "url": "https://shell.example.com",
        "sourceDirectoryEntryUrl": "https://www.gamedevmap.com/shell-studio",
    }
    provider_candidates: list[dict[str, object]] = []
    primary_jobs: list[dict[str, Any]] = []
    secondary_jobs: list[dict[str, Any]] = []
    browser_rows: list[dict[str, Any]] = []

    queued = dry_run._queue_no_careers_recovery(
        row=row,
        target_url="https://shell.example.com",
        html='<div id="root"></div><script src="/app.js"></script><a href="/jobs">Jobs</a>',
        index_url=INDEX_URL,
        provider_candidates=provider_candidates,
        primary_recovery_jobs=primary_jobs,
        secondary_recovery_jobs=secondary_jobs,
        browser_recovery_candidates=browser_rows,
    )

    assert queued is True
    assert provider_candidates == []
    assert [job["url"] for job in primary_jobs][:2] == [
        "https://shell.example.com/jobs",
        "https://shell.example.com/careers",
    ]
    assert [job["url"] for job in secondary_jobs] == [
        "https://shell.example.com/join-us",
        "https://shell.example.com/work-with-us",
        "https://shell.example.com/company/careers",
        "https://shell.example.com/about/careers",
    ]
    assert primary_jobs[0]["payload"] == {
        "row": row,
        "homepageUrl": "https://shell.example.com",
        "homepageReasonDetail": "js_shell",
        "recoverySource": "same_party_recovery_url",
        "recoveryWave": 1,
        "recoveryUrlSource": "html_jobish_link",
        "recoveryUrlPath": "/jobs",
    }
    assert primary_jobs[0]["name"] == "Shell Studio recovery /jobs"
    assert primary_jobs[0]["adapter"] == "gamedevmap"
    assert primary_jobs[0]["failureStage"] == "gamedevmap_recovery_fetch"
    assert primary_jobs[1]["payload"]["recoveryUrlSource"] == "generated_common_path"
    assert primary_jobs[1]["name"] == "Shell Studio recovery /careers"
    assert secondary_jobs[0]["payload"]["recoveryWave"] == 2
    assert secondary_jobs[0]["payload"]["recoveryUrlSource"] == "generated_common_path"
    assert browser_rows == [
        {
            "adapter": "gamedevmap",
            "name": "Shell Studio browser recovery",
            "studio": "Shell Studio",
            "url": "https://shell.example.com",
            "sourceDirectoryEntryUrl": "https://www.gamedevmap.com/shell-studio",
            "reasonDetail": "js_shell",
        }
    ]


def test_gamedevmap_failure_aggregation_bounds_samples() -> None:
    artifact: dict[str, Any] = {}
    failures = [
        {"name": f"failure-{index}", "adapter": "gamedevmap", "stage": "x", "error": "boom"}
        for index in range(dry_run.FAILURE_SAMPLE_LIMIT + 5)
    ]

    active_audit_runtime.record_failure_rows(
        artifact,
        failures,
        sample_limit=dry_run.FAILURE_SAMPLE_LIMIT,
    )

    assert artifact["failureCounts"] == {"x": dry_run.FAILURE_SAMPLE_LIMIT + 5}
    assert len(artifact["failureSamples"]) == dry_run.FAILURE_SAMPLE_LIMIT
    assert artifact["failures"] == artifact["failureSamples"]


def test_gamedevmap_no_careers_recovery_can_create_active_candidate() -> None:
    csv_text = _csv_row("Recover Studio", "https://recover.example.com")
    payloads = {
        CSV_URL: csv_text,
        "https://recover.example.com": "<html><main>Games studio</main></html>",
        "https://recover.example.com/careers": (
            '<html><a href="https://boards.greenhouse.io/recoverstudio">Open roles</a></html>'
        ),
        "https://recover.example.com/jobs": "<html>No openings here</html>",
        "https://recover.example.com/join-us": "<html>No openings here</html>",
        "https://recover.example.com/work-with-us": "<html>No openings here</html>",
        "https://recover.example.com/company/careers": "<html>No openings here</html>",
        "https://recover.example.com/about/careers": "<html>No openings here</html>",
        "https://boards-api.greenhouse.io/v1/boards/recoverstudio/jobs?content=true": json.dumps(
            {
                "jobs": [
                    {
                        "id": 1,
                        "title": "Gameplay Engineer",
                        "absolute_url": "https://job-boards.greenhouse.io/recoverstudio/jobs/1",
                    }
                ]
            }
        ),
    }

    with workspace_tmpdir("gamedevmap-active-dry-run-recovery-active") as root:
        output = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(allowed_categories=["Developer"]),
            fetcher=_fetcher(payloads),
            output_path=root / "dry-run.json",
            batch_size=10,
            reset=True,
        )

    assert output["summary"]["recoveryFetchAttempts"] == 2
    assert output["timings"]["batches"][0]["recoverySkippedByWave1"] == 4
    assert output["summary"]["recoveredCandidates"] == 1
    assert output["summary"]["recoveredActiveCandidates"] == 1
    assert output["summary"]["activeCandidates"] == 1
    assert output["activeCandidates"][0]["gamedevmapRecovery"] is True


def test_gamedevmap_no_careers_records_specific_unrecovered_detail() -> None:
    csv_text = _csv_row("Quiet Studio", "https://quiet.example.com")
    payloads = {
        CSV_URL: csv_text,
        "https://quiet.example.com": "<html><main>Games studio</main></html>",
        "https://quiet.example.com/careers": "<html>No openings here</html>",
        "https://quiet.example.com/jobs": "<html>No openings here</html>",
        "https://quiet.example.com/join-us": "<html>No openings here</html>",
        "https://quiet.example.com/work-with-us": "<html>No openings here</html>",
        "https://quiet.example.com/company/careers": "<html>No openings here</html>",
        "https://quiet.example.com/about/careers": "<html>No openings here</html>",
    }

    with workspace_tmpdir("gamedevmap-active-dry-run-recovery-empty") as root:
        output = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(activeAuditRecoveryEscalationEnabled=False),
            fetcher=_fetcher(payloads),
            output_path=root / "dry-run.json",
            batch_size=10,
            reset=True,
        )

    rejection = output["rejectedForActivation"][0]
    assert rejection["reason"] == "no_careers_evidence"
    assert rejection["reasonDetail"] == "recovery_pages_no_jobs"
    assert rejection["failureBucket"] == "coverage_miss"
    assert output["summary"]["coverageMisses"] == 1
    assert output["summary"]["technicalFailures"] == 0


def test_gamedevmap_bad_provider_inference_is_rejected_before_probe() -> None:
    csv_text = _csv_row("Embed Studio", "https://boards.greenhouse.io/embed")
    payloads = {CSV_URL: csv_text}

    with workspace_tmpdir("gamedevmap-active-dry-run-bad-provider") as root:
        output = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(allowed_categories=["Developer"]),
            fetcher=_fetcher(payloads),
            output_path=root / "dry-run.json",
            batch_size=10,
            reset=True,
        )

    assert output["summary"]["probeFailures"] == 0
    assert output["summary"]["activeCandidates"] == 0
    assert output["rejectedForActivation"][0]["reason"] == "bad_provider_inference"
    assert output["rejectedForActivation"][0]["reasonDetail"] == "bad_greenhouse_slug"


def test_gamedevmap_dry_run_rerun_reasons_select_prior_rejections() -> None:
    csv_text = (
        "Organization,URL,City,State/Province,Country/Region,Map Def,Category,Comments,"
        "Updated By,Bluesky,AI Response\n"
        "Recover Studio,https://recover.example.com,Rome,Lazio,Italy,Rome,Developer,"
        "Verified gaming studio.,,,Correct (Gaming)\n"
        "Skip Studio,https://skip.example.com,Rome,Lazio,Italy,Rome,Developer,"
        "Verified gaming studio.,,,Correct (Gaming)\n"
    )
    first_payloads = {
        CSV_URL: csv_text,
        "https://recover.example.com": "<html><main>Games studio</main></html>",
        "https://skip.example.com": "<html><main>Games studio</main></html>",
    }
    second_payloads = {
        **first_payloads,
        "https://recover.example.com/careers": (
            '<html><a href="https://boards.greenhouse.io/recoverstudio">Open roles</a></html>'
        ),
        "https://recover.example.com/jobs": "<html>No openings here</html>",
        "https://recover.example.com/join-us": "<html>No openings here</html>",
        "https://recover.example.com/work-with-us": "<html>No openings here</html>",
        "https://recover.example.com/company/careers": "<html>No openings here</html>",
        "https://recover.example.com/about/careers": "<html>No openings here</html>",
        "https://boards-api.greenhouse.io/v1/boards/recoverstudio/jobs?content=true": json.dumps(
            {
                "jobs": [
                    {
                        "id": 1,
                        "title": "Gameplay Engineer",
                        "absolute_url": "https://job-boards.greenhouse.io/recoverstudio/jobs/1",
                    }
                ]
            }
        ),
    }
    with workspace_tmpdir("gamedevmap-active-dry-run-rerun") as root:
        output_path = root / "dry-run.json"
        sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(activeAuditRecoveryEscalationEnabled=False),
            fetcher=_fetcher(first_payloads),
            output_path=output_path,
            batch_size=10,
            reset=True,
        )
        calls: list[str] = []
        output = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(activeAuditRecoveryEscalationEnabled=False),
            fetcher=_fetcher(second_payloads, calls),
            output_path=output_path,
            batch_size=10,
            rerun_reasons="no_careers_evidence",
        )

    assert {"https://recover.example.com", "https://skip.example.com"}.issubset(set(calls))
    assert output["progress"]["rerunReasons"] == ["no_careers_evidence"]
    assert output["summary"]["activeCandidates"] == 1


def test_run_discovery_gamedevmap_active_dry_run_does_not_mutate_registries() -> None:
    with workspace_tmpdir("source-discovery-gamedevmap-active-dry-run") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            active_before = [{"id": "active-1", "adapter": "static", "name": "Active"}]
            pending_before = [{"id": "pending-1", "adapter": "static", "name": "Pending"}]
            rejected_before = [{"id": "rejected-1", "adapter": "static", "name": "Rejected"}]
            sr.save_json_atomic(paths.active_path, active_before)
            sr.save_json_atomic(paths.pending_path, pending_before)
            sr.save_json_atomic(paths.rejected_path, rejected_before)

            cli_args = discovery_orchestrator.parse_args(
                [
                    "--gamedevmap-active-dry-run",
                    "--gamedevmap-dry-run-batch-size",
                    "10",
                    "--gamedevmap-dry-run-max-batches",
                    "1",
                ]
            )
            output = discovery_orchestrator.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=True,
                discovery_config=discovery_config_without_generator_stages(
                    gamedevmap=(_config()["gamedevmap"])
                ),
                cli_args=cli_args,
                fetcher=_fetcher(_payloads()),
            )

            assert int((output.get("summary") or {}).get("activeCandidates") or 0) == 2
            assert Path(root / "gamedevmap-active-source-dry-run.json").exists()
            assert json.loads(paths.active_path.read_text(encoding="utf-8")) == active_before
            assert json.loads(paths.pending_path.read_text(encoding="utf-8")) == pending_before
            assert json.loads(paths.rejected_path.read_text(encoding="utf-8")) == rejected_before
            assert not paths.discovery_candidates_path.exists()


def test_gamedevmap_default_discovery_reuses_fresh_audit_artifact() -> None:
    with workspace_tmpdir("gamedevmap-default-audit-cache") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]):
            config = _config()
            _write_audit_artifact(
                root / "gamedevmap-active-source-dry-run.json",
                config=config,
            )
            calls: list[str] = []

            provider_rows, static_rows, failures = sd.discover_gamedevmap_candidates(
                timeout_s=5,
                config=config,
                fetcher=_fetcher({}, calls),
            )

    assert calls == []
    assert len(provider_rows) == 1
    assert len(static_rows) == 1
    assert failures == [{"name": "Fetch Failed", "adapter": "gamedevmap", "stage": "x"}]
    assert provider_rows[0]["prevalidatedDiscovery"] is True
    assert static_rows[0]["prevalidatedDiscovery"] is True


def test_gamedevmap_default_discovery_refreshes_missing_audit_artifact() -> None:
    with workspace_tmpdir("gamedevmap-default-audit-refresh") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]):
            provider_rows, static_rows, _failures = sd.discover_gamedevmap_candidates(
                timeout_s=5,
                config=_config(),
                fetcher=_fetcher(_payloads()),
            )
            assert (root / "gamedevmap-active-source-dry-run.json").exists()

    assert len(provider_rows) == 1
    assert len(static_rows) == 1


def test_gamedevmap_default_active_audit_batch_size_is_1000() -> None:
    assert DEFAULT_DISCOVERY_CONFIG["gamedevmap"]["activeAuditBatchSize"] == 1000


def test_gamedevmap_source_audit_cache_hit_tolerates_legacy_batch_size_signature() -> None:
    with workspace_tmpdir("gamedevmap-batch-size-signature-cache") as root:
        output_path = root / "gamedevmap-active-source-dry-run.json"
        config = _config(activeAuditBatchSize=1000)
        _write_audit_artifact(output_path, config=config)
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        payload["runtime"]["configSignature"]["activeAuditBatchSize"] = 250
        output_path.write_text(json.dumps(payload), encoding="utf-8")
        calls: list[str] = []

        artifact, cache_hit = sd.run_gamedevmap_source_audit(
            timeout_s=5,
            config=config,
            fetcher=_fetcher(_payloads(), calls),
            output_path=output_path,
            max_batches=1,
        )

    assert cache_hit is True
    assert calls == []
    assert artifact["runtime"]["configSignature"]["activeAuditBatchSize"] == 250


def test_gamedevmap_active_audit_reports_subtask_progress() -> None:
    with workspace_tmpdir("gamedevmap-subtask-progress") as root:
        progress_events: list[dict[str, Any]] = []

        sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(),
            fetcher=_fetcher(_payloads()),
            output_path=root / "dry-run.json",
            batch_size=1000,
            max_batches=1,
            reset=True,
            progress_callback=progress_events.append,
        )

    assert progress_events
    assert any(
        ((event.get("counts") or {}).get("activeAuditPhase") == "batch_start")
        for event in progress_events
        if isinstance(event.get("counts"), dict)
    )
    latest_counts = progress_events[-1]["counts"]
    assert isinstance(latest_counts, dict)
    assert latest_counts["subtaskKey"] == "gamedevmap_active_audit"
    assert latest_counts["activeAuditBatchSize"] == 1000
    assert latest_counts["activeAuditTotalUrls"] == 4


def test_gamedevmap_source_audit_resets_stale_signature_artifact() -> None:
    with workspace_tmpdir("gamedevmap-stale-audit") as root:
        output_path = root / "gamedevmap-active-source-dry-run.json"
        stale_config = _config(allowed_categories=["Publisher"])
        stale_candidate = _validated_static_candidate("Stale Static")
        stale_candidate["listing_url"] = "https://stale.example.com/jobs"
        _write_audit_artifact(
            output_path,
            config=stale_config,
            active_candidates=[stale_candidate],
        )
        config = _config()
        gamedevmap_cfg = dict(config["gamedevmap"])
        gamedevmap_cfg["activeAuditBatchSize"] = 2
        config["gamedevmap"] = gamedevmap_cfg

        artifact, cache_hit = sd.run_gamedevmap_source_audit(
            timeout_s=5,
            config=config,
            fetcher=_fetcher(_payloads()),
            output_path=output_path,
            max_batches=1,
        )

    assert not cache_hit
    assert artifact["runtime"]["configSignature"] == dry_run._gamedevmap_cache_signature(
        dict(config["gamedevmap"])
    )
    assert all(
        "stale.example.com" not in str(row.get("listing_url") or "")
        for row in artifact.get("activeCandidates") or []
    )


def test_run_discovery_uses_prevalidated_gamedevmap_without_reprobing() -> None:
    with workspace_tmpdir("gamedevmap-default-prevalidated") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            config = discovery_config_without_generator_stages(gamedevmap=(_config()["gamedevmap"]))
            config["autoApproveHealthyPendingOnComplete"] = False
            _write_audit_artifact(
                root / "gamedevmap-active-source-dry-run.json",
                config=config,
            )
            with mock.patch.object(
                discovery_orchestrator,
                "async_probe_candidate",
                side_effect=AssertionError("prevalidated GameDevMap candidate was reprobed"),
            ):
                report = discovery_orchestrator.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config=config,
                    fetcher=_fetcher({}),
                )

            active_rows = (
                json.loads(paths.active_path.read_text(encoding="utf-8"))
                if paths.active_path.exists()
                else []
            )
            pending_rows = json.loads(paths.pending_path.read_text(encoding="utf-8"))

    assert int((report.get("summary") or {}).get("healthyCount") or 0) == 2
    assert active_rows == []
    assert len(pending_rows) == 2
    assert all(row.get("sourceDirectory") == "gamedevmap" for row in pending_rows)


def test_gamedevmap_prevalidated_static_respects_auto_approve_flag() -> None:
    with workspace_tmpdir("gamedevmap-default-auto-approve") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            config = discovery_config_without_generator_stages(gamedevmap=(_config()["gamedevmap"]))
            config["autoApproveHealthyPendingOnComplete"] = True
            _write_audit_artifact(
                root / "gamedevmap-active-source-dry-run.json",
                config=config,
                active_candidates=[_validated_static_candidate()],
            )
            discovery_orchestrator.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=_fetcher({}),
            )

            active_rows = json.loads(paths.active_path.read_text(encoding="utf-8"))
            pending_rows = json.loads(paths.pending_path.read_text(encoding="utf-8"))

    assert len(active_rows) == 1
    assert active_rows[0]["adapter"] == "static"
    assert active_rows[0]["sourceDirectory"] == "gamedevmap"
    assert pending_rows == []


def test_parse_args_supports_gamedevmap_active_dry_run_options() -> None:
    args = discovery_orchestrator.parse_args(
        [
            "--gamedevmap-active-dry-run",
            "--gamedevmap-dry-run-batch-size",
            "25",
            "--gamedevmap-dry-run-reset",
            "--gamedevmap-dry-run-max-batches",
            "3",
            "--gamedevmap-dry-run-rerun-reasons",
            "no_careers_evidence",
        ]
    )

    assert bool(args.gamedevmap_active_dry_run)
    assert int(args.gamedevmap_dry_run_batch_size) == 25
    assert bool(args.gamedevmap_dry_run_reset)
    assert int(args.gamedevmap_dry_run_max_batches) == 3
    assert str(args.gamedevmap_dry_run_rerun_reasons) == "no_careers_evidence"
