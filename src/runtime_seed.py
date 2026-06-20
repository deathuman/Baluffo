from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.active_task_snapshot import empty_snapshot
from src.shared.json_io import (
    copy_json_file_to_storage,
    existing_json_candidate,
    gzip_backed_json_storage_path,
    write_json_text,
)
from src.source_discovery.config import DEFAULT_DISCOVERY_CONFIG
from src.storage_json_metrics import record_json_text_write

APP_VERSION_CONTRACT_FILES = (
    "contracts/country_acceptance.json",
    "contracts/city_noise_contract.json",
)
APP_RUNTIME_DATA_FILES = APP_VERSION_CONTRACT_FILES + (
    "defaults/source-registry-active.seed.json",
    "defaults/source-registry-pending.seed.json",
    "source-discovery-config.json",
)


def default_fetch_report_payload(report_path: Path) -> dict[str, Any]:
    return {
        "schemaVersion": 1,
        "runId": "",
        "startedAt": "",
        "finishedAt": "",
        "runtime": {"lifecycle": {"owner": "fetch_report", "heartbeatAt": ""}},
        "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
        "taskProgress": {
            "active": False,
            "phaseKey": "",
            "phaseLabel": "",
            "mode": "indeterminate",
            "ratio": 0.0,
            "counts": {},
        },
        "sources": [],
        "outputs": {"report": str(report_path)},
    }


def _write_json_if_allowed(
    target: Path,
    payload: Any,
    *,
    overwrite: bool,
) -> Path | None:
    if not overwrite and (gzip_backed_json_storage_path(target).exists() or target.exists()):
        return None
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    return write_json_text(target, text, on_write=record_json_text_write)


def seed_runtime_data(
    data_dir: Path,
    *,
    source_root: Path,
    overwrite: bool = False,
) -> dict[str, list[str]]:
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)
    source_data = Path(source_root) / "data"
    copied: list[str] = []
    created: list[str] = []
    skipped: list[str] = []

    for name in APP_RUNTIME_DATA_FILES:
        source = existing_json_candidate(source_data / name) or source_data / name
        target = data_path / name
        if not source.exists():
            continue
        if not overwrite and (gzip_backed_json_storage_path(target).exists() or target.exists()):
            skipped.append(name)
            continue
        copied_path = copy_json_file_to_storage(
            source,
            target,
            on_write=record_json_text_write,
        )
        copied.append(str(copied_path.relative_to(data_path)))

    payloads: dict[str, Any] = {
        "source-registry-rejected.json": [],
        "source-discovery-candidates.json": [],
        "source-discovery-report.json": {"summary": {}, "candidates": [], "failures": []},
        "jobs-fetch-tasks.json": {"summary": {}, "tasks": [], "outputs": {}},
        "admin-active-task-snapshot.json": empty_snapshot(),
        "jobs-source-state.json": {"schemaVersion": 1, "updatedAt": "", "sources": {}},
        "jobs-success-cache.json": {"updatedAt": "", "successfulSources": []},
        "admin-task-state.json": {},
        "admin-alert-state.json": {"schemaVersion": 1, "acked": {}, "updatedAt": ""},
        "admin-run-history.json": [],
    }
    if not (data_path / "source-discovery-config.json").exists():
        payloads["source-discovery-config.json"] = DEFAULT_DISCOVERY_CONFIG
    fetch_report_path = data_path / "jobs-fetch-report.json"
    payloads["jobs-fetch-report.json"] = default_fetch_report_payload(fetch_report_path)

    for name, payload in payloads.items():
        target = data_path / name
        written = _write_json_if_allowed(target, payload, overwrite=overwrite)
        if written is None:
            skipped.append(name)
            continue
        created.append(str(written.relative_to(data_path)))

    return {"copied": copied, "created": created, "skipped": skipped}
