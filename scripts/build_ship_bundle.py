#!/usr/bin/env python3
"""Build a Windows-first ship bundle for local Baluffo operations."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist" / "baluffo-ship"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_frontend_runtime_config import (
    build_frontend_runtime_config_payload,
    render_frontend_runtime_config_js,
    write_frontend_runtime_config,
)
from src.app_version import APP_VERSION
from src.baluffo_config import get_sync_defaults
from src.python_version_guard import ensure_required_python
from src.ship.update_manager import ShipPaths, refresh_runtime_bootstrap

DEFAULT_BUNDLE_VERSION = APP_VERSION

APP_RUNTIME_FILES = (
    "baluffo.config.json",
    "admin.html",
    "frontend-runtime-config.js",
    "desktop-probe-css.html",
    "desktop-probe.html",
    "desktop-probe-head.html",
    "desktop-probe-inline.html",
    "index.html",
    "jobs.html",
    "saved.html",
    "styles.css",
    "theme.js",
    "startup-probe.js",
    "README.md",
)
APP_RUNTIME_SCRIPTS = (
    "__init__.py",
    "admin_bridge.py",
    "app_version.py",
    "baluffo_config.py",
    "contracts.py",
    "exceptions.py",
    "fetcher_metrics.py",
    "jobs_fetcher.py",
    "jobs_fetcher_registry.py",
    "pipeline_io.py",
    "source_discovery.py",
    "source_registry.py",
    "source_sync_config.py",
    "source_sync_crypto.py",
    "source_sync_snapshot.py",
    "source_sync.py",
    "local_data_store.py",
    "discovery_seed_catalog.json",
    "python_version_guard.py",
)
APP_RUNTIME_SCRIPT_DIRS = (
    "bridge",
    "core",
    "jobs",
    "scrapers",
    "shared",
    "source_discovery",
)
APP_RUNTIME_SHIP_FILES = (
    "__init__.py",
    "desktop_update.py",
)
APP_RUNTIME_ASSET_DIRS = ("probes",)
PACKAGING_FILES = (
    "README.md",
    "github-app-sync-config.template.json",
)
APP_RUNTIME_DATA_FILES = (
    "contracts/country_acceptance.json",
    "contracts/city_noise_contract.json",
    "jobs-unified-startup.json",
    "jobs-unified-light.json",
    "jobs-unified.json",
    "jobs-unified.csv",
    "jobs-fetch-report.json",
    "source-registry-active.json",
    "source-discovery-config.json",
)
STARTUP_PREVIEW_LIMIT = 240
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SYNC_DEFAULTS = get_sync_defaults()
PACKAGED_SYNC_CONFIG_PATH = Path(SYNC_DEFAULTS["packaged_config_path"])
PACKAGED_SYNC_CONFIG_TEMPLATE_PATH = ROOT / "packaging" / "github-app-sync-config.template.json"
PACKAGED_SYNC_LOCAL_CONFIG_ENV = "BALUFFO_SYNC_BUILD_CONFIG_PATH"
PACKAGED_SYNC_LOCAL_CANDIDATE_PATHS = (ROOT / "packaging" / "github-app-sync-config.localkey.json",)
DESKTOP_UPDATE_PUBLIC_KEYS_PATH = ROOT / "packaging" / "desktop-update-public-keys.json"
DESKTOP_UPDATE_PUBLIC_KEYS_ENV = "BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON"
DESKTOP_UPDATE_PUBLIC_KEYS_PATH_ENV = "BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_PATH"
PACKAGED_SYNC_BUILD_ENV = {
    "app_id": "BALUFFO_SYNC_BUILD_APP_ID",
    "installation_id": "BALUFFO_SYNC_BUILD_INSTALLATION_ID",
    "repo": "BALUFFO_SYNC_BUILD_REPO",
    "branch": "BALUFFO_SYNC_BUILD_BRANCH",
    "path": "BALUFFO_SYNC_BUILD_PATH",
    "allowed_repo": "BALUFFO_SYNC_BUILD_ALLOWED_REPO",
    "allowed_branch": "BALUFFO_SYNC_BUILD_ALLOWED_BRANCH",
    "allowed_path_prefix": "BALUFFO_SYNC_BUILD_ALLOWED_PATH_PREFIX",
    "private_key_path": "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PATH",
    "private_key_pem": "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM",
    "key_derivation": "BALUFFO_SYNC_BUILD_KEY_DERIVATION",
    "portable_passphrase_env": "BALUFFO_SYNC_BUILD_PASSPHRASE_ENV",
    "embedded_key_hint": "BALUFFO_SYNC_BUILD_EMBEDDED_KEY_HINT",
    "embedded_key_version": "BALUFFO_SYNC_BUILD_EMBEDDED_KEY_VERSION",
    "salt": "BALUFFO_SYNC_BUILD_KEY_SALT",
}


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _resolve_runtime_asset_source(rel: str) -> Path:
    direct = ROOT / rel
    if direct.exists():
        return direct
    probe_fallback = ROOT / "probes" / rel
    if probe_fallback.exists():
        return probe_fallback
    return direct


def _resolve_desktop_update_public_keys_payload() -> dict[str, str]:
    raw = str(os.environ.get(DESKTOP_UPDATE_PUBLIC_KEYS_ENV) or "").strip()
    if raw:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Invalid {DESKTOP_UPDATE_PUBLIC_KEYS_ENV} payload: {exc}."
            ) from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"{DESKTOP_UPDATE_PUBLIC_KEYS_ENV} must be a JSON object.")
        return {
            str(key): str(value)
            for key, value in payload.items()
            if str(key).strip() and str(value).strip()
        }
    path_token = str(os.environ.get(DESKTOP_UPDATE_PUBLIC_KEYS_PATH_ENV) or "").strip()
    candidate_path = Path(path_token).expanduser().resolve() if path_token else DESKTOP_UPDATE_PUBLIC_KEYS_PATH
    if not candidate_path.is_file():
        return {}
    try:
        payload = json.loads(candidate_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid desktop update public keys file {candidate_path}: {exc}.") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Desktop update public keys file {candidate_path} must contain a JSON object.")
    return {
        str(key): str(value)
        for key, value in payload.items()
        if str(key).strip() and str(value).strip()
    }


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _state_payload(version: str) -> dict:
    return {
        "current_version": version,
        "previous_version": "",
        "last_update_status": "ready",
        "last_error_code": "",
        "updated_at": _iso_now(),
    }


def _manifest_payload(version: str, sha256: str) -> dict:
    return {
        "version": version,
        "artifact_url": "",
        "sha256": sha256,
        "signature": "",
        "min_updater_version": "1.0.0",
        "migration_plan": [],
        "rollback_allowed": False,
        "generated_at": _iso_now(),
    }


def _seed_runtime_data(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    for name in APP_RUNTIME_DATA_FILES:
        src = ROOT / "data" / name
        if src.exists():
            _copy_file(src, data_dir / name)
    fetch_report_path = data_dir / "jobs-fetch-report.json"
    payloads = {
        "source-registry-pending.json": [],
        "source-registry-rejected.json": [],
        "source-discovery-candidates.json": [],
        "source-discovery-report.json": {"summary": {}, "candidates": [], "failures": []},
        "source-discovery-config.json": __import__(
            "src.source_discovery", fromlist=["DEFAULT_DISCOVERY_CONFIG"]
        ).DEFAULT_DISCOVERY_CONFIG,
        "source-approval-state.json": {"approvedSinceLastRun": 0},
        "jobs-fetch-tasks.json": {"summary": {}, "tasks": [], "outputs": {}},
        "jobs-source-state.json": {"schemaVersion": 1, "updatedAt": "", "sources": {}},
        "jobs-success-cache.json": {"updatedAt": "", "successfulSources": []},
        "admin-task-state.json": {},
        "admin-alert-state.json": {"schemaVersion": 1, "acked": {}, "updatedAt": ""},
        "admin-run-history.json": [],
    }
    for name, payload in payloads.items():
        target = data_dir / name
        if target.exists():
            continue
        _write_text(target, json.dumps(payload, indent=2, ensure_ascii=False))
    _write_text(
        fetch_report_path,
        json.dumps(
            {
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
                "outputs": {"report": str(fetch_report_path)},
            },
            indent=2,
            ensure_ascii=False,
        ),
    )


def _resolve_packaged_sync_config() -> Path | None:
    if PACKAGED_SYNC_CONFIG_PATH.exists():
        return PACKAGED_SYNC_CONFIG_PATH
    restored_path = _maybe_restore_packaged_sync_config_from_local()
    if restored_path is not None and restored_path.exists():
        return restored_path
    generated_path = _maybe_generate_packaged_sync_config()
    if generated_path is not None and generated_path.exists():
        return generated_path
    searched_paths = ", ".join(str(path) for path in _candidate_local_packaged_sync_config_paths())
    print(
        f"WARNING: Packaged GitHub App sync config not found. "
        f"Sync will be disabled in this build. "
        f"Searched: {PACKAGED_SYNC_CONFIG_PATH}, "
        f"{PACKAGED_SYNC_LOCAL_CONFIG_ENV} + defaults: {searched_paths}. "
        f"To enable sync, provide a config file or build env vars "
        f"({PACKAGED_SYNC_BUILD_ENV['app_id']}, {PACKAGED_SYNC_BUILD_ENV['installation_id']}, "
        f"{PACKAGED_SYNC_BUILD_ENV['repo']}, and {PACKAGED_SYNC_BUILD_ENV['private_key_path']} or "
        f"{PACKAGED_SYNC_BUILD_ENV['private_key_pem']}). "
        f"See {PACKAGED_SYNC_CONFIG_TEMPLATE_PATH}."
    )
    return None


def _env_value(name: str) -> str:
    return str(os.environ.get(name) or "").strip()


def _normalize_private_key_pem(raw_value: str) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    if "\\n" in text and "\n" not in text:
        text = text.replace("\\n", "\n")
    return text


def _validate_private_key_pem(private_key_pem: str, *, source: str) -> None:
    from src import source_sync

    normalized = _normalize_private_key_pem(private_key_pem)
    if not normalized:
        raise RuntimeError(f"Packaged sync private key from {source} is empty.")
    try:
        source_sync._parse_rsa_private_key_der(source_sync._pem_to_der(normalized))  # noqa: SLF001
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"Invalid packaged sync private key from {source}: {exc}. "
            "If using BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM, ensure it contains the full PEM "
            "or escaped newlines (\\n)."
        ) from exc


def _candidate_local_packaged_sync_config_paths() -> list[Path]:
    candidates: list[Path] = []
    env_path = _env_value(PACKAGED_SYNC_LOCAL_CONFIG_ENV)
    if env_path:
        candidates.append(Path(env_path).expanduser())
    for path in PACKAGED_SYNC_LOCAL_CANDIDATE_PATHS:
        candidates.append(path)
    appdata = str(os.environ.get("APPDATA") or "").strip()
    if appdata:
        candidates.append(Path(appdata) / "Baluffo" / "github-app-sync-config.json")
    local_appdata = str(os.environ.get("LOCALAPPDATA") or "").strip()
    if local_appdata:
        candidates.append(Path(local_appdata) / "Baluffo" / "github-app-sync-config.json")
    candidates.append(Path.home() / ".baluffo" / "github-app-sync-config.json")

    normalized: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(path)
    return normalized


def _maybe_restore_packaged_sync_config_from_local() -> Path | None:
    for source_path in _candidate_local_packaged_sync_config_paths():
        resolved_source = source_path.expanduser().resolve()
        if not resolved_source.exists():
            continue
        _copy_file(resolved_source, PACKAGED_SYNC_CONFIG_PATH)
        return PACKAGED_SYNC_CONFIG_PATH
    return None


def _maybe_generate_packaged_sync_config() -> Path | None:
    app_id = _env_value(PACKAGED_SYNC_BUILD_ENV["app_id"])
    installation_id = _env_value(PACKAGED_SYNC_BUILD_ENV["installation_id"])
    repo = _env_value(PACKAGED_SYNC_BUILD_ENV["repo"])
    private_key_path_value = _env_value(PACKAGED_SYNC_BUILD_ENV["private_key_path"])
    private_key_pem = str(os.environ.get(PACKAGED_SYNC_BUILD_ENV["private_key_pem"]) or "").strip()
    has_private_key_input = bool(private_key_path_value or private_key_pem)
    if not (app_id or installation_id or repo or has_private_key_input):
        return None

    missing = []
    if not app_id:
        missing.append(PACKAGED_SYNC_BUILD_ENV["app_id"])
    if not installation_id:
        missing.append(PACKAGED_SYNC_BUILD_ENV["installation_id"])
    if not repo:
        missing.append(PACKAGED_SYNC_BUILD_ENV["repo"])
    if not has_private_key_input:
        missing.append(
            f"{PACKAGED_SYNC_BUILD_ENV['private_key_path']}|{PACKAGED_SYNC_BUILD_ENV['private_key_pem']}"
        )
    if missing:
        raise RuntimeError(
            "Incomplete packaged sync build configuration. Missing: " + ", ".join(missing)
        )

    if private_key_path_value:
        private_key_path = Path(private_key_path_value).expanduser().resolve()
        if not private_key_path.exists():
            raise RuntimeError(f"Packaged sync private key file not found: {private_key_path}")
        private_key_pem = _normalize_private_key_pem(private_key_path.read_text(encoding="utf-8"))
        _validate_private_key_pem(private_key_pem, source=str(private_key_path))
    else:
        private_key_pem = _normalize_private_key_pem(private_key_pem)
        _validate_private_key_pem(
            private_key_pem,
            source=PACKAGED_SYNC_BUILD_ENV["private_key_pem"],
        )

    if not private_key_pem:
        raise RuntimeError(
            "Packaged sync build configuration did not provide a usable private key PEM."
        )

    from scripts.build_sync_app_config import (
        build_packaged_sync_payload,
        write_packaged_sync_config,
    )
    from src import source_sync

    branch = _env_value(PACKAGED_SYNC_BUILD_ENV["branch"]) or str(SYNC_DEFAULTS["default_branch"])
    remote_path = _env_value(PACKAGED_SYNC_BUILD_ENV["path"]) or str(SYNC_DEFAULTS["default_path"])
    allowed_repo = _env_value(PACKAGED_SYNC_BUILD_ENV["allowed_repo"]) or str(
        SYNC_DEFAULTS["default_allowed_repo"] or repo
    )
    allowed_branch = _env_value(PACKAGED_SYNC_BUILD_ENV["allowed_branch"]) or str(
        SYNC_DEFAULTS["default_allowed_branch"] or branch
    )
    allowed_path_prefix = _env_value(PACKAGED_SYNC_BUILD_ENV["allowed_path_prefix"]) or str(
        SYNC_DEFAULTS["default_allowed_path_prefix"] or remote_path
    )
    key_derivation = _env_value(PACKAGED_SYNC_BUILD_ENV["key_derivation"]) or str(
        SYNC_DEFAULTS["build_key_derivation_default"] or source_sync.KEY_DERIVATION_EMBEDDED
    )
    payload = build_packaged_sync_payload(
        app_id=app_id,
        installation_id=installation_id,
        repo=repo,
        branch=branch,
        path=remote_path,
        allowed_repo=allowed_repo,
        allowed_branch=allowed_branch,
        allowed_path_prefix=allowed_path_prefix,
        private_key_pem=private_key_pem,
        salt=_env_value(PACKAGED_SYNC_BUILD_ENV["salt"]),
        key_derivation=key_derivation,
        portable_passphrase_env=_env_value(PACKAGED_SYNC_BUILD_ENV["portable_passphrase_env"]),
        embedded_key_hint=_env_value(PACKAGED_SYNC_BUILD_ENV["embedded_key_hint"]),
        embedded_key_version=_env_value(PACKAGED_SYNC_BUILD_ENV["embedded_key_version"])
        or str(SYNC_DEFAULTS["build_embedded_key_version"]),
    )
    return write_packaged_sync_config(PACKAGED_SYNC_CONFIG_PATH, payload)


def _copy_app_version(version_dir: Path) -> None:
    packaged_sync_config = _resolve_packaged_sync_config()
    _write_text(
        version_dir / "frontend-runtime-config.js",
        render_frontend_runtime_config_js(build_frontend_runtime_config_payload()),
    )
    # Static frontend/runtime assets.
    for rel in APP_RUNTIME_FILES:
        _copy_file(_resolve_runtime_asset_source(rel), version_dir / rel)

    _copy_tree(ROOT / "frontend", version_dir / "frontend")
    for rel in APP_RUNTIME_ASSET_DIRS:
        _copy_tree(ROOT / rel, version_dir / rel)
    for rel in APP_RUNTIME_SCRIPTS:
        _copy_file(ROOT / "src" / rel, version_dir / "src" / rel)
    for rel in APP_RUNTIME_SCRIPT_DIRS:
        _copy_tree(ROOT / "src" / rel, version_dir / "src" / rel)
    for rel in APP_RUNTIME_SHIP_FILES:
        _copy_file(ROOT / "src" / "ship" / rel, version_dir / "src" / "ship" / rel)
    for rel in PACKAGING_FILES:
        _copy_file(ROOT / "packaging" / rel, version_dir / "packaging" / rel)
    if packaged_sync_config is not None:
        _copy_file(packaged_sync_config, version_dir / "packaging" / "github-app-sync-config.json")

    for rel in APP_RUNTIME_DATA_FILES:
        src = ROOT / "data" / rel
        if not src.exists():
            continue
        _copy_file(src, version_dir / "data" / rel)
    _generate_startup_preview(version_dir / "data")


def _generate_startup_preview(data_dir: Path) -> None:
    light_path = data_dir / "jobs-unified-light.json"
    startup_path = data_dir / "jobs-unified-startup.json"
    if not light_path.exists():
        return
    try:
        payload = json.loads(light_path.read_text(encoding="utf-8"))
    except Exception:
        return
    rows = payload if isinstance(payload, list) else []
    startup_rows = rows[:STARTUP_PREVIEW_LIMIT]
    _write_text(startup_path, json.dumps(startup_rows, ensure_ascii=False))


def build_bundle(output_dir: Path, version: str) -> Path:
    write_frontend_runtime_config()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    app_dir = output_dir / "app"
    versions_dir = app_dir / "versions"
    version_dir = versions_dir / version
    staging_dir = app_dir / "staging"
    tooling_dir = output_dir / "src" / "ship"
    logs_dir = output_dir / "logs"

    version_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)
    tooling_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    _copy_app_version(version_dir)
    if not (version_dir / "src" / "admin_bridge.py").exists():
        raise RuntimeError(
            "Ship bundle build failed: admin bridge missing at "
            f"{version_dir / 'src' / 'admin_bridge.py'}"
        )
    refresh_runtime_bootstrap(ShipPaths.from_root(output_dir), version_dir, version_name=version)
    _copy_file(ROOT / "src" / "ship" / "update_manager.py", tooling_dir / "update_manager.py")
    _copy_file(ROOT / "src" / "ship" / "migrations.py", tooling_dir / "migrations.py")
    _copy_file(ROOT / "src" / "ship" / "runtime_launcher.py", tooling_dir / "runtime_launcher.py")
    _copy_file(ROOT / "src" / "ship" / "desktop_update.py", tooling_dir / "desktop_update.py")
    _copy_file(ROOT / "src" / "ship" / "desktop_updater.py", tooling_dir / "desktop_updater.py")

    # Launcher scripts + ship runbook.
    _copy_file(ROOT / "src" / "ship" / "run-bridge.ps1", output_dir / "run-bridge.ps1")
    _copy_file(ROOT / "src" / "ship" / "run-site.ps1", output_dir / "run-site.ps1")
    _copy_file(ROOT / "src" / "ship" / "run-all.ps1", output_dir / "run-all.ps1")
    _copy_file(ROOT / "src" / "ship" / "apply-update.ps1", output_dir / "apply-update.ps1")
    _copy_file(ROOT / "src" / "ship" / "recover-previous.ps1", output_dir / "recover-previous.ps1")
    _copy_file(
        ROOT / "src" / "ship" / "create-support-bundle.ps1",
        output_dir / "create-support-bundle.ps1",
    )
    _copy_file(ROOT / "docs" / "RELEASE.md", output_dir / "RELEASE_GUIDE.md")
    _copy_file(
        ROOT / "docs" / "update-manifest.schema.json", output_dir / "UPDATE_MANIFEST_SCHEMA.json"
    )
    _copy_file(
        ROOT / "docs" / "desktop-update-manifest.schema.json",
        output_dir / "DESKTOP_UPDATE_MANIFEST_SCHEMA.json",
    )
    desktop_update_public_keys = _resolve_desktop_update_public_keys_payload()
    if desktop_update_public_keys:
        public_keys_payload = json.dumps(desktop_update_public_keys, indent=2, ensure_ascii=False)
        _write_text(app_dir / "desktop-update-public-keys.json", public_keys_payload)
        _write_text(
            version_dir / "packaging" / "desktop-update-public-keys.json",
            public_keys_payload,
        )

    data_dir = output_dir / "data"
    _seed_runtime_data(data_dir)
    (data_dir / "backups").mkdir(parents=True, exist_ok=True)
    (data_dir / "migration-reports").mkdir(parents=True, exist_ok=True)

    _write_text(app_dir / "current.txt", f"{version}\n")
    _write_text(
        app_dir / "update-state.json",
        json.dumps(_state_payload(version), indent=2, ensure_ascii=False),
    )

    # Hashing current version folder enables local integrity checks and a baseline manifest.
    version_hashes = {}
    for path in sorted(p for p in version_dir.rglob("*") if p.is_file()):
        rel = str(path.relative_to(version_dir)).replace("\\", "/")
        version_hashes[rel] = _hash_file(path)
    _write_text(
        app_dir / "version-hashes.json",
        json.dumps({"version": version, "files": version_hashes}, indent=2, ensure_ascii=False),
    )
    aggregate_digest = hashlib.sha256(
        json.dumps(version_hashes, sort_keys=True).encode("utf-8")
    ).hexdigest()
    _write_text(
        app_dir / "update-manifest.json",
        json.dumps(_manifest_payload(version, aggregate_digest), indent=2, ensure_ascii=False),
    )
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dist/baluffo-ship bundle.")
    parser.add_argument("--output-dir", default=str(DIST_DIR))
    parser.add_argument("--bundle-version", default=DEFAULT_BUNDLE_VERSION)
    return parser.parse_args()


def main() -> int:
    ensure_required_python()
    args = parse_args()
    version = str(args.bundle_version).strip() or DEFAULT_BUNDLE_VERSION
    out = build_bundle(Path(args.output_dir).expanduser().resolve(), version)
    print(f"Ship bundle ready: {out}")
    print(f"Launchers: {out / 'run-bridge.ps1'} | {out / 'run-site.ps1'} | {out / 'run-all.ps1'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
