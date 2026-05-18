"""Packaged smoke orchestration behind the root facade."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.orchestrator.root is not configured")
    return root


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _seed_jobs_pipeline_smoke_feed(data_dir: Path, *, finished_at: str) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    row = {
        "id": "packaged-smoke-seed-job",
        "title": "Packaged Smoke Seed Job",
        "company": "Packaged Smoke Studio",
        "location": "Remote",
        "country": "Remote",
        "city": "",
        "source": "packaged_smoke",
    }
    report = {
        "schemaVersion": 1,
        "status": "ok",
        "startedAt": str(finished_at or ""),
        "finishedAt": str(finished_at or ""),
        "summary": {"status": "ok", "outputCount": 1},
    }
    (data_dir / "jobs-fetch-report.json").write_text(
        f"{json.dumps(report, separators=(',', ':'))}\n",
        encoding="utf-8",
    )
    feed = f"{json.dumps([row], separators=(',', ':'))}\n"
    (data_dir / "jobs-unified-light.json").write_text(feed, encoding="utf-8")
    (data_dir / "jobs-unified.json").write_text(feed, encoding="utf-8")
    (data_dir / "jobs-unified.csv").write_text(
        "id,title,company,location,source\n"
        "packaged-smoke-seed-job,Packaged Smoke Seed Job,Packaged Smoke Studio,Remote,packaged_smoke\n",
        encoding="utf-8",
    )


def _seed_jobs_pipeline_smoke_feed_if_needed(
    *,
    node_smoke_script: Path,
    jobs_pipeline_script: Path,
    data_dir: Path,
    finished_at: str,
) -> None:
    if node_smoke_script == jobs_pipeline_script.resolve():
        _seed_jobs_pipeline_smoke_feed(data_dir, finished_at=finished_at)


def _record_rehearsal_artifacts(
    report: dict[str, Any],
    rehearsal: dict[str, Any],
    mappings: tuple[tuple[str, str], ...],
) -> None:
    details = _as_dict(rehearsal.get("details"))
    for src_key, artifact_key in mappings:
        value = str(details.get(src_key) or "").strip()
        if value:
            report["artifacts"][artifact_key] = value


def _apply_rehearsal_result(
    report: dict[str, Any],
    rehearsal: dict[str, Any],
    *,
    artifact_mappings: tuple[tuple[str, str], ...],
    failure_step: str,
    failure_message: str,
) -> dict[str, Any]:
    deps = _root()
    report["scenarios"].append(rehearsal)
    _record_rehearsal_artifacts(report, rehearsal, artifact_mappings)
    report["ok"] = str(rehearsal.get("status")) == "passed"
    if not report["ok"]:
        report["failure"] = deps.build_failure_payload(
            failure_step,
            str(rehearsal.get("error") or failure_message),
        )
    return report


def _append_startup_profile_scenario(
    report: dict[str, Any],
    *,
    startup_profile: dict[str, Any],
) -> None:
    report["scenarios"].append(
        {
            "name": "Startup Profile",
            "slug": "startup-profile",
            "status": "passed" if str(startup_profile.get("status")) == "passed" else "failed",
            "durationMs": int(startup_profile.get("firstUsableMs") or 0),
            "error": ""
            if str(startup_profile.get("status")) == "passed"
            else str(startup_profile.get("classification") or "startup profile threshold exceeded"),
            "startupProfile": startup_profile,
        }
    )


def _startup_profile_regression_message(startup_profile: dict[str, Any]) -> str:
    regressions = [
        row for row in startup_profile.get("perfRegressions") or [] if isinstance(row, dict)
    ]
    if not regressions:
        return str(startup_profile.get("classification") or "startup profile threshold exceeded")
    worst = next(
        (row for row in regressions if str(row.get("severity") or "").strip() == "critical"),
        regressions[0],
    )
    return (
        f"Startup profile threshold exceeded: {worst.get('stage')} "
        f"{int(worst.get('durationMs') or 0)}ms > {int(worst.get('thresholdMs') or 0)}ms."
    )


def _apply_startup_threshold_gate(
    report: dict[str, Any],
    *,
    startup_profile: dict[str, Any],
) -> None:
    if not startup_profile.get("perfRegressions"):
        return
    deps = _root()
    report["ok"] = False
    report["failure"] = deps.build_failure_payload(
        "startup-profile-threshold",
        _startup_profile_regression_message(startup_profile),
        category="startup_profile_threshold_exceeded",
    )


def _build_initial_report(
    *,
    started_at: str,
    artifacts_dir: Path,
    runtime_data_dir: Path,
    report_path: Path,
    stdout_path: Path,
    stderr_path: Path,
    site_base_url: str,
    bridge_base_url: str,
    exe_path: Path,
    preferred_probe_browser_name: str,
    preferred_probe_browser_path: str,
    startup_probe: bool,
    rebuild_output_dir: Path | None,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "ok": False,
        "startedAt": started_at,
        "finishedAt": "",
        "exePath": str(exe_path),
        "dataDir": str(runtime_data_dir),
        "siteBaseUrl": site_base_url,
        "bridgeBaseUrl": bridge_base_url,
        "startupMetrics": [],
        "bridgeReady": False,
        "scenarios": [],
        "startupProfile": {},
        "memoryMetrics": {},
        "artifacts": {
            "artifactsDir": str(artifacts_dir),
            "reportPath": str(report_path),
            "exeStdout": str(stdout_path),
            "exeStderr": str(stderr_path),
        },
        "environment": {},
        "probeBrowser": {
            "requiredManagedWindow": bool(startup_probe),
            "preferredBrowserName": preferred_probe_browser_name,
            "preferredBrowserPath": preferred_probe_browser_path,
            "selectedBrowserName": "",
            "selectedBrowserPath": "",
            "launchMode": "",
            "launchError": "",
            "launchErrorType": "",
            "windowClosedReason": "",
        },
        "failure": None,
    }
    if rebuild_output_dir is not None:
        report["artifacts"]["rebuiltPortableDir"] = str(rebuild_output_dir)
    return report


def _capture_startup_failure_metrics(
    report: dict[str, Any],
    *,
    runtime_data_dir: Path,
    bridge_base_url: str,
    startup_page: str,
    profile_mode: str,
    preferred_probe_browser_name: str,
    preferred_probe_browser_path: str,
    artifacts_dir: Path,
    error_message: str,
) -> None:
    deps = _root()
    partial_metrics = _load_failure_startup_metrics(
        report,
        runtime_data_dir=runtime_data_dir,
        bridge_base_url=bridge_base_url,
    )
    if not partial_metrics:
        return
    report["startupMetrics"] = partial_metrics
    startup_profile = deps.summarize_startup_metrics(
        partial_metrics,
        page=startup_page,
        profile_mode=profile_mode,
    )
    startup_profile = deps.refine_startup_probe_summary(
        startup_profile,
        partial_metrics,
        error_message=error_message,
        preferred_browser_name=preferred_probe_browser_name,
        preferred_browser_path=preferred_probe_browser_path,
    )
    report["startupProfile"] = startup_profile
    report["probeBrowser"] = deps.startup_probe_browser_details(
        partial_metrics,
        preferred_browser_name=preferred_probe_browser_name,
        preferred_browser_path=preferred_probe_browser_path,
    )
    report["artifacts"]["startupProfileSummary"] = str(
        artifacts_dir / "startup-profile-summary.json"
    )
    deps.write_startup_summary(artifacts_dir / "startup-profile-summary.json", startup_profile)
    if not any(
        str(row.get("slug")) == "startup-profile"
        for row in report["scenarios"]
        if isinstance(row, dict)
    ):
        _append_startup_profile_scenario(report, startup_profile=startup_profile)


def _load_failure_startup_metrics(
    report: dict[str, Any],
    *,
    runtime_data_dir: Path,
    bridge_base_url: str,
) -> list[dict[str, Any]]:
    deps = _root()
    partial_metrics = [
        row for row in list(report.get("startupMetrics") or []) if isinstance(row, dict)
    ]
    if not partial_metrics:
        try:
            partial_metrics = deps.fetch_startup_metrics(bridge_base_url, limit=1000)
        except Exception:  # noqa: BLE001
            partial_metrics = []
    if not partial_metrics:
        partial_metrics = deps.read_startup_metrics_file(runtime_data_dir, limit=1000)
    return [row for row in partial_metrics if isinstance(row, dict)]


def _start_process_memory_sampler(process: subprocess.Popen[Any] | None) -> Any | None:
    if process is None:
        return None
    deps = _root()
    memory_sampler = deps.ProcessMemorySampler(int(getattr(process, "pid", 0) or 0))
    memory_sampler.start()
    return memory_sampler


def _stop_process_memory_sampler(memory_sampler: Any | None) -> dict[str, Any]:
    if memory_sampler is None:
        return {}
    metrics = memory_sampler.stop()
    return dict(metrics) if isinstance(metrics, dict) else {}


def run_packaged_smoke(args: argparse.Namespace) -> dict[str, Any]:
    deps = _root()
    started_at = deps.utc_now_iso()
    run_token = deps.generate_packaged_smoke_run_token()
    artifacts_dir = (
        Path(args.artifacts_dir or (deps.DEFAULT_ARTIFACT_ROOT / run_token)).expanduser().resolve()
    )
    runtime_data_dir = artifacts_dir / "runtime-data"
    embedded_artifacts_dir = artifacts_dir / "embedded-runtime-probes"
    stdout_path = artifacts_dir / "desktop-exe.stdout.log"
    stderr_path = artifacts_dir / "desktop-exe.stderr.log"
    latest_report_path = Path(args.report_path or deps.DEFAULT_REPORT_PATH).expanduser().resolve()
    report_path = artifacts_dir / "report.json"
    site_port = int(args.site_port or deps.choose_free_port())
    bridge_port = int(args.bridge_port or deps.choose_free_port())
    site_base_url = f"http://127.0.0.1:{site_port}"
    bridge_base_url = f"http://127.0.0.1:{bridge_port}"
    startup_probe = bool(args.startup_probe or args.profile_only)
    embedded_probes = bool(args.embedded_probes)
    profile_mode = "warm" if str(args.profile_mode or "").strip().lower() == "warm" else "cold"
    open_path = str(args.open_path or "jobs.html").strip() or "jobs.html"
    node_smoke_script = (
        Path(args.node_smoke_script or deps.DEFAULT_NODE_SMOKE_SCRIPT).expanduser().resolve()
    )
    if artifacts_dir.parent == deps.DEFAULT_ARTIFACT_ROOT.resolve():
        deps.prune_packaged_smoke_artifacts(
            deps.DEFAULT_ARTIFACT_ROOT,
            keep_recent_runs=deps.DEFAULT_ARTIFACT_RETENTION_RUNS,
            file_retention_s=deps.DEFAULT_ARTIFACT_FILE_RETENTION_S,
            current_artifacts_dir=artifacts_dir,
        )
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    embedded_artifacts_dir.mkdir(parents=True, exist_ok=True)
    _seed_jobs_pipeline_smoke_feed_if_needed(
        node_smoke_script=node_smoke_script,
        jobs_pipeline_script=deps.JOBS_PIPELINE_NODE_SMOKE_SCRIPT,
        data_dir=runtime_data_dir,
        finished_at=started_at,
    )
    runtime_env = os.environ.copy()
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            node_smoke_script,
            artifacts_dir=artifacts_dir,
            session_scope="runtime",
            startup_probe=startup_probe,
            profile_mode=profile_mode,
            fetch_evidence_mode=str(
                getattr(args, "fetch_evidence_mode", "deterministic") or "deterministic"
            ),
        )
    )
    deps.clear_packaged_desktop_session_state(runtime_env)
    preferred_probe_browser_name = ""
    preferred_probe_browser_path = ""
    startup_page = Path(open_path).stem or "jobs"
    requested_exe_path = Path(args.exe_path or deps.DEFAULT_EXE_PATH).expanduser().resolve()
    rebuild_output_dir = (
        artifacts_dir / "portable-build"
        if bool(args.rebuild) and requested_exe_path == deps.DEFAULT_EXE_PATH.resolve()
        else None
    )
    exe_path = requested_exe_path
    report = _build_initial_report(
        started_at=started_at,
        artifacts_dir=artifacts_dir,
        runtime_data_dir=runtime_data_dir,
        report_path=report_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        exe_path=exe_path,
        preferred_probe_browser_name=preferred_probe_browser_name,
        preferred_probe_browser_path=preferred_probe_browser_path,
        startup_probe=startup_probe,
        rebuild_output_dir=rebuild_output_dir,
    )

    process: subprocess.Popen[Any] | None = None
    memory_sampler: Any | None = None
    stdout_handle = None
    stderr_handle = None
    try:
        if startup_probe:
            preferred_probe_browser = deps.select_startup_probe_browser(runtime_env)
            preferred_probe_browser_name = str(
                preferred_probe_browser.get("browserName") or ""
            ).strip()
            preferred_probe_browser_path = str(
                preferred_probe_browser.get("browserPath") or ""
            ).strip()
            runtime_env[deps.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV] = (
                preferred_probe_browser_path
            )
            report["probeBrowser"]["preferredBrowserName"] = preferred_probe_browser_name
            report["probeBrowser"]["preferredBrowserPath"] = preferred_probe_browser_path
        exe_path = deps.ensure_portable_exe(
            requested_exe_path,
            rebuild=bool(args.rebuild),
            rebuild_output_dir=rebuild_output_dir,
        )
        report["exePath"] = str(exe_path)
        report["environment"] = deps.collect_packaged_smoke_env_diagnostics(
            artifacts_dir=artifacts_dir,
            requested_exe_path=requested_exe_path,
            exe_path=exe_path,
            node_smoke_script=node_smoke_script,
            rebuilt_portable_dir=rebuild_output_dir,
            env=runtime_env,
        )
        if bool(args.sync_rehearsal):
            rehearsal = deps.run_packaged_sync_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=float(args.runtime_timeout or deps.DEFAULT_RUNTIME_TIMEOUT_S),
            )
            return _apply_rehearsal_result(
                report,
                rehearsal,
                artifact_mappings=(
                    ("runtimeStdout", "syncRehearsalStdout"),
                    ("runtimeStderr", "syncRehearsalStderr"),
                ),
                failure_step="packaged-sync-rehearsal",
                failure_message="Packaged sync rehearsal failed.",
            )
        if bool(args.desktop_update_rehearsal):
            rehearsal = deps.run_desktop_update_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=float(args.runtime_timeout or deps.DEFAULT_RUNTIME_TIMEOUT_S),
            )
            return _apply_rehearsal_result(
                report,
                rehearsal,
                artifact_mappings=(
                    ("helperStdoutLog", "helperStdout"),
                    ("helperStderrLog", "helperStderr"),
                    ("helperDiagnosticsLog", "helperDiagnostics"),
                ),
                failure_step="desktop-update-rehearsal",
                failure_message="Packaged desktop update rehearsal failed.",
            )
        if bool(args.orphan_reclaim_rehearsal):
            rehearsal = deps.run_packaged_orphan_reclaim_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=float(args.runtime_timeout or deps.DEFAULT_RUNTIME_TIMEOUT_S),
            )
            return _apply_rehearsal_result(
                report,
                rehearsal,
                artifact_mappings=(
                    ("runtimeStdout", "orphanRehearsalRuntimeStdout"),
                    ("runtimeStderr", "orphanRehearsalRuntimeStderr"),
                    ("staleSiteStdout", "orphanRehearsalSiteStdout"),
                    ("staleSiteStderr", "orphanRehearsalSiteStderr"),
                    ("staleBridgeStdout", "orphanRehearsalBridgeStdout"),
                    ("staleBridgeStderr", "orphanRehearsalBridgeStderr"),
                ),
                failure_step="packaged-orphan-reclaim-rehearsal",
                failure_message="Packaged orphan reclaim rehearsal failed.",
            )
        if bool(args.browser_job_rehearsal):
            rehearsal = deps.run_packaged_browser_job_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=float(args.runtime_timeout or deps.DEFAULT_RUNTIME_TIMEOUT_S),
            )
            return _apply_rehearsal_result(
                report,
                rehearsal,
                artifact_mappings=(
                    ("runtimeStdout", "browserJobRehearsalRuntimeStdout"),
                    ("runtimeStderr", "browserJobRehearsalRuntimeStderr"),
                    ("startupMetrics", "browserJobRehearsalStartupMetrics"),
                ),
                failure_step="packaged-browser-job-rehearsal",
                failure_message="Packaged browser job rehearsal failed.",
            )
        if profile_mode == "warm":
            deps.run_warmup_launch(
                exe_path,
                artifacts_root=artifacts_dir,
                open_path=open_path,
                runtime_timeout_s=float(args.runtime_timeout or deps.DEFAULT_RUNTIME_TIMEOUT_S),
                startup_probe=startup_probe,
                env=runtime_env,
            )
        if embedded_probes and not bool(args.profile_only):
            embedded_scenarios = [
                deps.run_embedded_runtime_probe(
                    exe_path=exe_path,
                    probe=probe,
                    artifacts_root=embedded_artifacts_dir,
                    runtime_timeout_s=float(args.runtime_timeout or deps.DEFAULT_RUNTIME_TIMEOUT_S),
                    startup_probe=startup_probe,
                    profile_mode=profile_mode,
                    env=runtime_env,
                )
                for probe in deps.EMBEDDED_PAGE_PROBES
            ]
            for row in embedded_scenarios:
                print("." if str(row.get("status")) == "passed" else "X", end="", flush=True)
            report["scenarios"].extend(embedded_scenarios)
            first_failed_embedded = next(
                (row for row in embedded_scenarios if str(row.get("status")) != "passed"),
                None,
            )
            if first_failed_embedded:
                raise RuntimeError(
                    f"{first_failed_embedded.get('name', 'Embedded probe')} failed: {first_failed_embedded.get('error', '')}".strip()
                )
        process, stdout_handle, stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            open_path=open_path,
            startup_probe=startup_probe,
            env=runtime_env,
        )
        memory_sampler = _start_process_memory_sampler(process)
        runtime_state = deps.wait_for_packaged_runtime(
            process,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            timeout_s=float(args.runtime_timeout or deps.DEFAULT_RUNTIME_TIMEOUT_S),
            open_path=open_path,
            required_events=deps.STARTUP_REQUIRED_EVENTS,
            require_managed_window=startup_probe,
            require_page_ready=not startup_probe,
        )
        report["bridgeReady"] = True
        report["startupMetrics"] = runtime_state.get("startupMetrics") or []
        if startup_probe:
            report["startupMetrics"] = deps.wait_for_runtime_events(
                bridge_base_url,
                deps.startup_profile_required_events(startup_page),
                timeout_s=max(5.0, float(args.runtime_timeout or deps.DEFAULT_RUNTIME_TIMEOUT_S)),
            )
            startup_profile = deps.summarize_startup_metrics(
                report["startupMetrics"],
                page=startup_page,
                profile_mode=profile_mode,
            )
            startup_profile = deps.refine_startup_probe_summary(
                startup_profile,
                report["startupMetrics"],
                preferred_browser_name=preferred_probe_browser_name,
                preferred_browser_path=preferred_probe_browser_path,
            )
            report["startupProfile"] = startup_profile
            report["probeBrowser"] = deps.startup_probe_browser_details(
                report["startupMetrics"],
                preferred_browser_name=preferred_probe_browser_name,
                preferred_browser_path=preferred_probe_browser_path,
            )
            report["artifacts"]["startupProfileSummary"] = str(
                artifacts_dir / "startup-profile-summary.json"
            )
            deps.write_startup_summary(
                artifacts_dir / "startup-profile-summary.json",
                startup_profile,
            )
            _append_startup_profile_scenario(report, startup_profile=startup_profile)
            if bool(getattr(args, "fail_on_threshold", False)):
                _apply_startup_threshold_gate(report, startup_profile=startup_profile)

        if bool(args.profile_only):
            record_only = bool(getattr(args, "profile_record_only", False))
            report["ok"] = (
                True
                if record_only
                else all(str(row.get("status")) == "passed" for row in report["scenarios"])
            )
            if not report["ok"] and not report["failure"]:
                report["failure"] = deps.build_failure_payload(
                    "startup-profile",
                    str(
                        report["startupProfile"].get("classification")
                        or "startup profile threshold exceeded"
                    ),
                    category=deps.classify_startup_probe_failure(
                        report.get("startupMetrics") or [],
                        summary=report.get("startupProfile")
                        if isinstance(report.get("startupProfile"), dict)
                        else None,
                    )[1],
                )
            return report

        report["artifacts"].update(deps.capture_runtime_snapshot(bridge_base_url, artifacts_dir))

        smoke_runner_result = deps.run_packaged_node_smoke(
            requested_exe_path=requested_exe_path,
            exe_path=exe_path,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            artifacts_dir=artifacts_dir,
            node_smoke_script=node_smoke_script,
            headed=bool(args.headed),
            pause_on_failure=bool(args.pause_on_failure),
            timeout_s=float(args.playwright_timeout or deps.DEFAULT_SMOKE_RUNNER_TIMEOUT_S),
        )
        report["artifacts"]["smokeReport"] = str(smoke_runner_result["reportPath"])
        report["artifacts"]["smokeOutputDir"] = str(smoke_runner_result["outputDir"])
        report["artifacts"]["smokeRunnerStdout"] = str(artifacts_dir / "smoke-runner-stdout.log")
        report["artifacts"]["smokeRunnerStderr"] = str(artifacts_dir / "smoke-runner-stderr.log")
        report["artifacts"]["playwrightReport"] = report["artifacts"]["smokeReport"]
        report["artifacts"]["playwrightOutputDir"] = report["artifacts"]["smokeOutputDir"]
        report["artifacts"]["playwrightStdout"] = report["artifacts"]["smokeRunnerStdout"]
        report["artifacts"]["playwrightStderr"] = report["artifacts"]["smokeRunnerStderr"]
        report["scenarios"].extend(list(smoke_runner_result.get("scenarios") or []))
        if isinstance(smoke_runner_result.get("environment"), dict):
            report["environment"] = dict(smoke_runner_result["environment"])
        if int(smoke_runner_result.get("exitCode", 1)) != 0:
            failed = next(
                (row for row in report["scenarios"] if str(row.get("status")) != "passed"),
                None,
            )
            report["failure"] = deps.build_failure_payload(
                "playwright",
                failed.get("error")
                if isinstance(failed, dict) and failed.get("error")
                else str(
                    smoke_runner_result.get("runnerError") or "Packaged desktop smoke failed."
                ),
                category=str(smoke_runner_result.get("failureCategory") or ""),
            )
        else:
            report["ok"] = all(str(row.get("status")) == "passed" for row in report["scenarios"])

        for row in smoke_runner_result.get("scenarios", []):
            print("." if str(row.get("status")) == "passed" else "X", end="", flush=True)

        report["artifacts"].update(deps.capture_runtime_snapshot(bridge_base_url, artifacts_dir))
    except Exception as exc:  # noqa: BLE001
        if startup_probe:
            _capture_startup_failure_metrics(
                report,
                runtime_data_dir=runtime_data_dir,
                bridge_base_url=bridge_base_url,
                startup_page=startup_page,
                profile_mode=profile_mode,
                preferred_probe_browser_name=preferred_probe_browser_name,
                preferred_probe_browser_path=preferred_probe_browser_path,
                artifacts_dir=artifacts_dir,
                error_message=str(exc),
            )
        else:
            report["startupMetrics"] = _load_failure_startup_metrics(
                report,
                runtime_data_dir=runtime_data_dir,
                bridge_base_url=bridge_base_url,
            )
        if not report["failure"]:
            report["failure"] = deps.build_failure_payload(
                "runner",
                exc,
                category=deps.classify_startup_probe_failure(
                    report.get("startupMetrics") or [],
                    error_message=str(exc),
                    summary=report.get("startupProfile")
                    if isinstance(report.get("startupProfile"), dict)
                    else None,
                )[1]
                or deps.classify_subprocess_error(exc),
            )
    finally:
        report["memoryMetrics"] = _stop_process_memory_sampler(memory_sampler)
        memory_sampler = None
        deps.terminate_process_tree(process)
        if deps.os.name == "nt":
            deps.time.sleep(0.25)
        deps.cleanup_orphaned_desktop_ports_nt(site_port, bridge_port)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
        report["finishedAt"] = deps.utc_now_iso()
        deps.write_json(report_path, report)
        deps.write_json(latest_report_path, report)
    return report
