"""Discovery bootstrap helpers for report priming and path resolution.

These helpers stay hookable so the orchestrator can keep its save/load seams
patchable in tests and bridge-driven runs.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src import source_registry as source_registry_module
from src.contracts import SCHEMA_VERSION

from .reporting import build_discovery_task_progress


def discovery_report_write_path() -> Path:
    """Resolve the live discovery report path used by the worker."""
    env_path = str(os.environ.get("BALUFFO_DISCOVERY_REPORT_PATH") or "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()
    raw = str(os.environ.get("BALUFFO_DATA_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve() / "source-discovery-report.json"
    return Path(str(source_registry_module.DISCOVERY_REPORT_PATH))


def prime_bridge_discovery_report(
    *,
    run_id: str,
    started_at: str,
    mode: str,
    save_json_atomic: Callable[[Path, Any], None],
    now_iso: Callable[[], str],
) -> None:
    """Publish a minimal in-progress discovery report before heavy setup."""
    rid = str(run_id or "").strip()
    sat = str(started_at or "").strip()
    if not rid or not sat:
        return
    summary: dict[str, Any] = {
        "foundEndpointCount": 0,
        "probedCandidateCount": 0,
        "queuedCandidateCount": 0,
        "failedProbeCount": 0,
        "skippedDuplicateCount": 0,
        "skippedLowEvidenceProbeCount": 0,
        "phase": "starting",
        "phaseKey": "starting",
        "phaseLabel": "Initializing scan",
    }
    report_path = discovery_report_write_path()
    task_progress = build_discovery_task_progress(summary=summary, finished=False)
    save_json_atomic(
        report_path,
        {
            "schemaVersion": SCHEMA_VERSION,
            "runId": rid,
            "mode": str(mode or "dynamic"),
            "startedAt": sat,
            "finishedAt": "",
            "summary": summary,
            "runtime": {
                "lifecycle": {
                    "owner": "discovery_report",
                    "heartbeatAt": now_iso(),
                },
            },
            "taskProgress": task_progress,
            "candidates": [],
            "failures": [],
            "topFailures": [],
            "outputs": {
                "report": str(report_path),
                "candidates": str(source_registry_module.DISCOVERY_CANDIDATES_PATH),
                "pending": str(source_registry_module.PENDING_PATH),
                "urlPatches": str(source_registry_module.URL_PATCH_MANIFEST_PATH),
            },
        },
    )


__all__ = ["discovery_report_write_path", "prime_bridge_discovery_report"]
