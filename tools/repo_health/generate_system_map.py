#!/usr/bin/env python3
"""Generate a system map JSON data file from the route inventory and module structure."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

OUTPUT_PATH = ROOT / "data" / "system-map.json"

FRONTEND_PAGES = [
    {
        "id": "jobs",
        "name": "Jobs",
        "path": "jobs.html",
        "bridge_calls": [
            "GET /ops/health",
            "GET /ops/fetch-report",
            "GET /ops/task-live/fetch",
            "GET /ops/task-state",
            "POST /tasks/run-jobs-bootstrap",
            "GET /ops/dashboard-health",
        ],
        "description": "Browse and filter game development job listings. Handles cold-start bootstrap, auto-refresh, and desktop update notifications.",
    },
    {
        "id": "saved",
        "name": "Saved Jobs",
        "path": "saved.html",
        "bridge_calls": [
            "POST /saved-jobs/status",
            "POST /saved-jobs/activity/snapshot",
            "GET /saved-jobs/status",
        ],
        "description": "Track saved jobs through the application pipeline (bookmark, applied, screening, interview, offer).",
    },
    {
        "id": "admin",
        "name": "Admin",
        "path": "admin.html",
        "bridge_calls": [
            "GET /ops/health",
            "GET /ops/dashboard-health",
            "GET /ops/fetch-report",
            "GET /ops/fetcher-metrics",
            "GET /ops/task-state",
            "GET /ops/task-live/*",
            "GET /ops/history",
            "GET /ops/storage-health",
            "GET /sync/status",
            "GET /discovery/report",
            "GET /discovery/candidates",
            "GET /registry/pending",
            "GET /registry/active",
            "GET /registry/rejected",
            "GET /registry/conflicts",
            "GET /source-policy/recommendations",
            "POST /tasks/run-fetcher",
            "POST /tasks/run-discovery",
            "POST /tasks/run-jobs-pipeline",
            "POST /tasks/run-sync-pull",
            "POST /tasks/run-sync-push",
            "POST /sync/test",
            "POST /registry/approve",
            "POST /registry/reject",
            "POST /registry/rollback",
            "POST /registry/restore-rejected",
            "POST /registry/restore-deleted",
            "POST /registry/delete",
            "POST /ops/alerts/ack",
            "POST /discovery/config",
        ],
        "description": "Administration panel for operations health, fetcher control, source discovery, registry management, and sync.",
    },
]

TASK_FLOWS = [
    {
        "id": "fetch",
        "name": "Jobs Fetch",
        "entry": "POST /tasks/run-fetcher",
        "steps": [
            "Admin/user triggers fetch → POST /tasks/run-fetcher",
            "Task launch API validates admission (not already running)",
            "Fetcher CLI spawned with preset (default/incremental/retry_failed/force_full)",
            "Source loaders determine active sources",
            "Scraper workers fetch job listings per source",
            "Dedup pipeline merges/collapses duplicates",
            "Sanitizer filters non-game-development jobs",
            "Pipeline finalize writes jobs-unified.json/lights/csv to runtime data",
            "Fetch report updated with run stats (sources, output, failures)",
            "Auto-refresh signal sent to Jobs page",
        ],
    },
    {
        "id": "discovery",
        "name": "Source Discovery",
        "entry": "POST /tasks/run-discovery",
        "steps": [
            "Admin triggers discovery → POST /tasks/run-discovery",
            "Discovery engine probes candidate URLs",
            "Endpoint detection identifies known providers (Greenhouse, Lever, etc.)",
            "Auto-approval for healthy candidates matching known patterns",
            "Candidates queued as pending/rejected in source registry",
            "Admin reviews and approves/rejects pending sources",
        ],
    },
    {
        "id": "sync",
        "name": "Source Sync",
        "entry": "POST /tasks/run-sync-pull | /tasks/run-sync-push",
        "steps": [
            "Admin enables sync with GitHub App credentials",
            "Pull: fetches remote source registry from GitHub",
            "Registry reconciliation merges remote/local changes",
            "Push: publishes local registry state to GitHub",
            "Conflicts detected and queued for admin review",
        ],
    },
    {
        "id": "bootstrap",
        "name": "First-Run Bootstrap",
        "entry": "POST /tasks/run-jobs-bootstrap",
        "steps": [
            "Cold start detected (no successful runtime report)",
            "Google Sheets bootstrap auto-starts on first run",
            "Bootstrap runs only 3 canonical sheet sources",
            "Output promoted atomically to runtime data",
            "pipeline_never_run alert visible until full pipeline completes",
            "Once full pipeline succeeds, bootstrap is gated (no-op)",
        ],
    },
    {
        "id": "pipeline",
        "name": "Full Jobs Pipeline",
        "entry": "POST /tasks/run-jobs-pipeline",
        "steps": [
            "Runs discovery → fetch → dedup → sanitize → finalize in sequence",
            "Full pipeline clears pipeline_never_run alert",
        ],
    },
    {
        "id": "storage",
        "name": "Storage & Authority",
        "description": "Manages runtime data persistence and authority modes.",
    },
]

RISK_MARKERS = [
    {
        "path": "src/ship/desktop_app/",
        "label": "Desktop App",
        "risk": "compatibility",
        "note": "Platform-specific launcher code (_linux.py, _windows.py). Changes must be mirrored across platforms.",
    },
    {
        "path": "src/bridge/routes/",
        "label": "Bridge Routes",
        "risk": "compatibility",
        "note": "Route signature changes are compatibility work. Must search all frontend call sites before changing.",
    },
    {
        "path": "scripts/build_ship_bundle.py",
        "label": "Ship Bundle",
        "risk": "release",
        "note": "Packaging and release build path. Changes affect portable distribution.",
    },
    {
        "path": "scripts/build_portable_exe.py",
        "label": "Portable EXE",
        "risk": "release",
        "note": "Portable Windows executable build. Changes affect the packaged desktop app.",
    },
    {
        "path": "src/ship/update_manager*.py",
        "label": "Update Manager",
        "risk": "release",
        "note": "Desktop update management. Verify release-critical path before changes.",
    },
    {
        "path": "tools/repo_health/bridge_route_inventory.py",
        "label": "Route Inventory",
        "risk": "compatibility",
        "note": "Authoritative route inventory. Must sync with implementation and docs.",
    },
    {
        "path": "docs/DATA_CONTRACT.md",
        "label": "Data Contracts",
        "risk": "contracts",
        "note": "Public data contracts. Preserve job text, locations, and user-facing data shapes.",
    },
    {
        "path": "docs/admin-bridge-api.md",
        "label": "Bridge API Docs",
        "risk": "contracts",
        "note": "Canonical bridge API documentation. Must match implemented routes.",
    },
    {
        "path": "frontend/local-data/",
        "label": "Local Data",
        "risk": "contracts",
        "note": "Local storage and profile management. Saved-job contracts must stay compatible.",
    },
    {
        "path": "src/bridge/ops_health.py",
        "label": "Ops Health",
        "risk": "internal",
        "note": "Core health evaluation and alerting. Changes affect the Action Center and admin dashboards.",
    },
    {
        "path": "src/jobs/pipeline*.py",
        "label": "Jobs Pipeline",
        "risk": "internal",
        "note": "Fetch pipeline orchestration. Pipeline step ordering and output contracts are critical.",
    },
    {
        "path": "src/bridge/storage_health.py",
        "label": "Storage Health",
        "risk": "internal",
        "note": "SQLite storage diagnostics. Authority mode transitions must be handled carefully.",
    },
]

EVIDENCE_FILES = [
    {
        "name": "jobs-fetch-report.json",
        "role": "Latest fetch run report (output, sources, failures)",
    },
    {"name": "jobs-fetch-tasks.json", "role": "Active and completed fetch task state"},
    {"name": "jobs-source-state.json", "role": "Per-source cached state (last fetch, success)"},
    {"name": "jobs-success-cache.json", "role": "Recently successful source names cache"},
    {"name": "jobs-lifecycle-state.json", "role": "Task lifecycle run records"},
    {
        "name": "jobs-unified.json / light / csv",
        "role": "Unified job listing output (runtime feed)",
    },
    {"name": "admin-task-state.json", "role": "Admin task state and scheduling"},
    {"name": "admin-alert-state.json", "role": "Acknowledged alert state"},
    {"name": "admin-run-history.json", "role": "Run history for operational visibility"},
    {"name": "source-discovery-config.json", "role": "Discovery engine configuration"},
    {"name": "source-discovery-candidates.json", "role": "Discovery candidate queue"},
    {"name": "source-discovery-report.json", "role": "Latest discovery run report"},
    {"name": "source-registry-active.seed.json", "role": "Active source registry seed"},
    {"name": "source-registry-pending.seed.json", "role": "Pending source registry seed"},
    {"name": "baluffo.db", "role": "SQLite runtime storage (source runs, jobs feed mirror)"},
    {"name": "system-map.json", "role": "This file — generated system map data"},
]


def _read_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _find_modules(path: str, pattern: str) -> list[str]:
    base = ROOT / path
    if not base.exists():
        return []
    return sorted(
        {
            str(p.relative_to(ROOT).with_suffix("").as_posix()).replace("/", ".")
            for p in base.rglob(pattern)
            if p.is_file()
        }
    )


def generate() -> None:
    frontend_modules = _find_modules("frontend", "*.js")
    src_modules = _find_modules("src", "*.py")
    all_modules = sorted(frontend_modules + src_modules)

    payload = {
        "generatedAt": __import__("datetime").datetime.now().isoformat(),
        "frontendPages": FRONTEND_PAGES,
        "taskFlows": TASK_FLOWS,
        "riskMarkers": RISK_MARKERS,
        "evidenceFiles": EVIDENCE_FILES,
        "moduleCount": len(all_modules),
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"System map generated: {OUTPUT_PATH}")


if __name__ == "__main__":
    generate()
