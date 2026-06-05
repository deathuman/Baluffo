from __future__ import annotations

"""Snapshot, embedded-probe, and warmup helpers for packaged smoke."""

import time
from pathlib import Path
from typing import Any


def _snapshot_json(deps: Any, url: str) -> dict[str, Any]:
    try:
        payload = deps.fetch_json(url)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}
    return payload if isinstance(payload, dict) else {"ok": False, "payload": payload}


def capture_runtime_snapshot(
    deps: Any, bridge_base_url: str, artifacts_dir: Path
) -> dict[str, str]:
    snapshots = {
        "opsHealthSnapshot": artifacts_dir / "ops-health.json",
        "sessionSnapshot": artifacts_dir / "session.json",
        "startupMetricsSnapshot": artifacts_dir / "startup-metrics.json",
        "storageMetricsSnapshot": artifacts_dir / "storage-metrics.json",
        "storageHealthSnapshot": artifacts_dir / "storage-health.json",
        "performanceProfileSnapshot": artifacts_dir / "performance-profile.json",
    }
    deps.write_json(
        snapshots["opsHealthSnapshot"], _snapshot_json(deps, f"{bridge_base_url}/ops/health")
    )
    deps.write_json(
        snapshots["sessionSnapshot"],
        _snapshot_json(deps, f"{bridge_base_url}/desktop-local-data/session"),
    )
    metrics_payload = _snapshot_json(
        deps, f"{bridge_base_url}/desktop-local-data/startup-metrics?limit=1000"
    )
    deps.write_json(snapshots["startupMetricsSnapshot"], metrics_payload)
    storage_metrics_payload = _snapshot_json(deps, f"{bridge_base_url}/ops/storage-metrics")
    deps.write_json(snapshots["storageMetricsSnapshot"], storage_metrics_payload)
    storage_health_payload = _snapshot_json(deps, f"{bridge_base_url}/ops/storage-health")
    deps.write_json(snapshots["storageHealthSnapshot"], storage_health_payload)
    performance_profile_payload = _snapshot_json(deps, f"{bridge_base_url}/ops/performance-profile")
    deps.write_json(snapshots["performanceProfileSnapshot"], performance_profile_payload)
    return {key: str(path) for key, path in snapshots.items()}


def capture_performance_profile_snapshot(
    deps: Any,
    bridge_base_url: str,
    artifacts_dir: Path,
    *,
    filename: str = "performance-profile.json",
) -> dict[str, str]:
    snapshot_path = artifacts_dir / filename
    deps.write_json(
        snapshot_path,
        _snapshot_json(deps, f"{bridge_base_url}/ops/performance-profile"),
    )
    return {"performanceProfileSnapshot": str(snapshot_path)}


def run_embedded_runtime_probe(
    deps: Any,
    *,
    exe_path: Path,
    probe: dict[str, Any],
    artifacts_root: Path,
    runtime_timeout_s: float,
    startup_probe: bool,
    profile_mode: str,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    slug = deps.slugify_token(str(probe.get("name") or "embedded-probe"))
    probe_dir = artifacts_root / slug
    runtime_data_dir = probe_dir / "runtime-data"
    stdout_path = probe_dir / "desktop-exe.stdout.log"
    stderr_path = probe_dir / "desktop-exe.stderr.log"
    site_port = deps.choose_free_port()
    bridge_port = deps.choose_free_port()
    site_base_url = f"http://127.0.0.1:{site_port}"
    bridge_base_url = f"http://127.0.0.1:{bridge_port}"
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    process = None
    stdout_handle = None
    stderr_handle = None
    started = time.perf_counter()
    runtime_env = dict(env or deps.os.environ)
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=probe_dir,
            session_scope="runtime",
            startup_probe=startup_probe,
            profile_mode=profile_mode,
        )
    )
    deps.clear_packaged_desktop_session_state(runtime_env)
    try:
        process, stdout_handle, stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=runtime_data_dir,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            open_path=str(probe.get("openPath") or "jobs.html"),
            startup_probe=startup_probe,
            env=runtime_env,
        )
        deps.wait_for_packaged_runtime(
            process,
            site_base_url=site_base_url,
            bridge_base_url=bridge_base_url,
            timeout_s=runtime_timeout_s,
            open_path=str(probe.get("openPath") or "jobs.html"),
            required_events=deps.STARTUP_REQUIRED_EVENTS,
            require_managed_window=startup_probe,
            require_page_ready=not startup_probe,
        )
        page_name = Path(str(probe.get("openPath") or "jobs.html")).stem or "jobs"
        required_runtime_events = tuple(probe.get("requiredEvents") or ())
        if startup_probe:
            required_runtime_events = tuple(
                dict.fromkeys(
                    deps.startup_profile_required_events(page_name) + required_runtime_events
                )
            )
        metrics_rows = deps.wait_for_runtime_events(
            bridge_base_url,
            required_runtime_events,
            timeout_s=max(5.0, runtime_timeout_s),
        )
        deps.write_json(probe_dir / "startup-metrics.json", {"rows": metrics_rows})
        summary: dict[str, Any] = {}
        status = "passed"
        error = ""
        if startup_probe:
            summary = deps.summarize_startup_metrics(
                metrics_rows,
                page=page_name,
                profile_mode=profile_mode,
            )
            deps.write_startup_summary(probe_dir / "startup-profile-summary.json", summary)
            if str(summary.get("status")) != "passed":
                status = "failed"
                error = str(summary.get("classification") or "startup profile threshold exceeded")
        return {
            "name": str(probe.get("name") or "Embedded Probe"),
            "slug": slug,
            "status": status,
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": error,
            "startupProfile": summary,
        }
    except Exception as exc:
        return {
            "name": str(probe.get("name") or "Embedded Probe"),
            "slug": slug,
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
            "startupProfile": {},
        }
    finally:
        deps.terminate_process_tree(process)
        if deps.os.name == "nt":
            deps.time.sleep(0.25)
        deps.cleanup_orphaned_desktop_ports_nt(site_port, bridge_port)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()


def build_failure_payload(
    step: str, error: Exception | str, *, category: str = ""
) -> dict[str, Any]:
    payload = {
        "step": str(step or "unknown"),
        "message": str(error),
    }
    if category:
        payload["category"] = str(category)
    return payload


def run_warmup_launch(
    deps: Any,
    exe_path: Path,
    *,
    artifacts_root: Path,
    open_path: str,
    runtime_timeout_s: float,
    startup_probe: bool,
    env: dict[str, str] | None = None,
) -> None:
    warmup_root = Path(artifacts_root).expanduser().resolve() / "warmup"
    warmup_root.mkdir(parents=True, exist_ok=True)
    runtime_env = dict(env or deps.os.environ)
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=warmup_root,
            session_scope="runtime",
            startup_probe=startup_probe,
            profile_mode="warm",
        )
    )
    deps.clear_packaged_desktop_session_state(runtime_env)
    process = None
    stdout_handle = None
    stderr_handle = None
    site_port = 0
    bridge_port = 0
    try:
        site_port = deps.choose_free_port()
        bridge_port = deps.choose_free_port()
        process, stdout_handle, stderr_handle = deps.launch_packaged_exe(
            exe_path,
            site_port=site_port,
            bridge_port=bridge_port,
            data_dir=warmup_root / "runtime-data",
            stdout_path=warmup_root / "desktop-exe.stdout.log",
            stderr_path=warmup_root / "desktop-exe.stderr.log",
            open_path=open_path,
            startup_probe=startup_probe,
            env=runtime_env,
        )
        deps.wait_for_packaged_runtime(
            process,
            site_base_url=f"http://127.0.0.1:{site_port}",
            bridge_base_url=f"http://127.0.0.1:{bridge_port}",
            timeout_s=runtime_timeout_s,
            open_path=open_path,
            require_managed_window=startup_probe,
            require_page_ready=not startup_probe,
        )
        deps.time.sleep(1.0)
    finally:
        deps.terminate_process_tree(process)
        if deps.os.name == "nt":
            deps.time.sleep(0.25)
        if site_port and bridge_port:
            deps.cleanup_orphaned_desktop_ports_nt(site_port, bridge_port)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
