from __future__ import annotations

"""Node-based packaged-smoke runner helpers."""

import json
import subprocess
from pathlib import Path
from typing import Any


def as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def parse_packaged_node_smoke_report(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    rows = as_list(payload.get("scenarios")) if isinstance(payload, dict) else []
    return [dict(row) for row in rows if isinstance(row, dict)]


def read_packaged_node_smoke_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    return payload if isinstance(payload, dict) else {}


def run_packaged_node_smoke(
    deps: Any,
    *,
    requested_exe_path: Path,
    exe_path: Path,
    site_base_url: str,
    bridge_base_url: str,
    artifacts_dir: Path,
    node_smoke_script: Path,
    headed: bool,
    pause_on_failure: bool,
    timeout_s: float,
) -> dict[str, Any]:
    output_dir = artifacts_dir / "smoke-output"
    report_path = artifacts_dir / "smoke-report.json"
    command = [*deps.resolve_node_command(), str(Path(node_smoke_script).expanduser().resolve())]
    env = deps.build_packaged_smoke_env(
        site_base_url=site_base_url,
        bridge_base_url=bridge_base_url,
        artifacts_dir=artifacts_dir,
        headed=headed,
        pause_on_failure=pause_on_failure,
    )
    env.update(deps.packaged_runtime_env_overrides(node_smoke_script))
    diagnostics = deps.collect_packaged_smoke_env_diagnostics(
        artifacts_dir=artifacts_dir,
        requested_exe_path=requested_exe_path,
        exe_path=exe_path,
        node_smoke_script=Path(node_smoke_script).expanduser().resolve(),
        node_command=command,
        env=env,
    )
    try:
        completed = deps.subprocess.run(
            command,
            cwd=deps.ROOT,
            env=env,
            timeout=max(30.0, float(timeout_s)),
            check=False,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        diagnostics["runnerStdout"] = ""
        diagnostics["runnerStderr"] = str(exc)
        deps.write_text(artifacts_dir / "smoke-runner-stdout.log", "")
        deps.write_text(artifacts_dir / "smoke-runner-stderr.log", str(exc))
        return {
            "exitCode": 1,
            "reportPath": str(report_path),
            "outputDir": str(output_dir),
            "scenarios": [],
            "failureCategory": deps.classify_subprocess_error(exc),
            "runnerError": str(exc),
            "environment": diagnostics,
        }
    deps.write_text(artifacts_dir / "smoke-runner-stdout.log", str(completed.stdout or ""))
    deps.write_text(artifacts_dir / "smoke-runner-stderr.log", str(completed.stderr or ""))
    diagnostics["runnerStdout"] = str(completed.stdout or "")
    diagnostics["runnerStderr"] = str(completed.stderr or "")
    report_payload = deps.read_packaged_node_smoke_payload(report_path)
    scenarios = deps.parse_packaged_node_smoke_report(report_path)
    report_errors = (
        [str(item) for item in report_payload.get("errors", []) if str(item or "").strip()]
        if isinstance(report_payload.get("errors"), list)
        else []
    )
    failure_category = ""
    runner_error = str(completed.stderr or completed.stdout or "")
    if report_errors:
        runner_error = report_errors[0]
    if int(completed.returncode) != 0:
        failure_category = deps.classify_subprocess_error(runner_error)
    return {
        "exitCode": int(completed.returncode),
        "reportPath": str(report_path),
        "outputDir": str(output_dir),
        "scenarios": scenarios,
        "failureCategory": failure_category,
        "runnerError": runner_error,
        "environment": diagnostics,
    }
