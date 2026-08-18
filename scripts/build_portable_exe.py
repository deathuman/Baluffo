#!/usr/bin/env python3
"""Build a portable Windows executable wrapper around the Baluffo ship bundle."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist" / "baluffo-portable"
LATEST_PORTABLE_DIR = ROOT / "_out" / "latest" / "build" / "portable"
BUILD_CACHE_ROOT = ROOT / "_out" / "portable-build-cache"
DEFAULT_EXE_NAME = "Baluffo"
DEFAULT_ICON_PATH = ROOT / "favicon.ico"
PORTABLE_BUILD_PROVENANCE_FILE = ".baluffo-portable-build.json"
CACHE_MANIFEST_FILE = "manifest.json"
CACHE_SCHEMA_VERSION = 1
DEFAULT_CACHE_RETAIN_ENTRIES = 5
PORTABLE_BUILD_VERSION_ENV = "BALUFFO_PORTABLE_BUILD_VERSION"
PORTABLE_BUILD_CACHE_RETAIN_ENV = "BALUFFO_PORTABLE_BUILD_CACHE_RETAIN"
PLAYWRIGHT_BROWSER_NAME = "chromium-headless-shell"
PLAYWRIGHT_BROWSER_CACHE_PREFIX = "chromium_headless_shell"
PLAYWRIGHT_BROWSER_EXE_NAME = "chrome-headless-shell.exe"
BUILD_INPUT_FILES = (
    "baluffo.config.json",
    "baluffo.config.local.json",
    "favicon.ico",
    "index.html",
    "jobs.html",
    "saved.html",
    "admin.html",
    "theme.js",
    "startup-probe.js",
    "desktop-probe-css.html",
    "desktop-probe.html",
    "desktop-probe-head.html",
    "desktop-probe-inline.html",
    "README.md",
    "package.json",
    "package-lock.json",
    "requirements-lock.txt",
    "docs/RELEASE.md",
    "docs/update-manifest.schema.json",
    "docs/desktop-update-manifest.schema.json",
    "packaging/README.md",
    "packaging/github-app-sync-config.template.json",
    "packaging/github-app-sync-config.json",
    "packaging/github-app-sync-config.localkey.json",
    "packaging/desktop-update-public-keys.json",
    "scripts/build_portable_exe.py",
    "scripts/build_ship_bundle.py",
    "scripts/build_frontend_runtime_config.py",
    "scripts/build_sync_app_config.py",
)
BUILD_INPUT_DIRS = (
    "src",
    "frontend",
    "probes",
    "styles",
    "data/contracts",
    "data/defaults",
)
BUILD_ENV_VALUE_NAMES = (
    "BALUFFO_SYNC_APP_CONFIG_PATH",
    "BALUFFO_SYNC_BUILD_APP_ID",
    "BALUFFO_SYNC_BUILD_INSTALLATION_ID",
    "BALUFFO_SYNC_BUILD_REPO",
    "BALUFFO_SYNC_BUILD_BRANCH",
    "BALUFFO_SYNC_BUILD_PATH",
    "BALUFFO_SYNC_BUILD_ALLOWED_REPO",
    "BALUFFO_SYNC_BUILD_ALLOWED_BRANCH",
    "BALUFFO_SYNC_BUILD_ALLOWED_PATH_PREFIX",
    "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PATH",
    "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM",
    "BALUFFO_SYNC_BUILD_KEY_DERIVATION",
    "BALUFFO_SYNC_BUILD_PASSPHRASE_ENV",
    "BALUFFO_SYNC_BUILD_EMBEDDED_KEY_HINT",
    "BALUFFO_SYNC_BUILD_EMBEDDED_KEY_VERSION",
    "BALUFFO_SYNC_BUILD_KEY_SALT",
    "BALUFFO_SYNC_KEY_PASSPHRASE",
    "BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON",
    "BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_PATH",
    "BALUFFO_DESKTOP_UPDATE_REPO",
)
BUILD_ENV_FILE_VALUE_NAMES = (
    "BALUFFO_SYNC_APP_CONFIG_PATH",
    "BALUFFO_SYNC_BUILD_CONFIG_PATH",
    "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PATH",
    "BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_PATH",
)
PACKAGE_VERSION_NAMES = (
    "PyInstaller",
    "playwright",
    "pydantic",
    "scrapy",
    "scrapy-playwright",
    "twisted",
    "certifi",
)


def _available_metadata_packages(package_names: tuple[str, ...]) -> tuple[str, ...]:
    available: list[str] = []
    for package_name in package_names:
        try:
            importlib.metadata.version(package_name)
        except importlib.metadata.PackageNotFoundError:
            continue
        available.append(package_name)
    return tuple(available)


BUILD_SKIP_DIR_NAMES = {"__pycache__", ".pytest_cache", "node_modules"}
BUILD_SKIP_FILE_SUFFIXES = {".pyc", ".pyo"}
OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES = tuple(
    package_name
    for package_name in ("certifi",)
    if importlib.util.find_spec(package_name) is not None
)
STORAGE_RUNTIME_COLLECT_DATA_PACKAGES = ("src.storage",)
OPTIONAL_SCRAPY_RUNTIME_PACKAGES = tuple(
    package_name
    for package_name in (
        "scrapy",
        "scrapy_playwright",
        "twisted",
        "parsel",
        "w3lib",
        "queuelib",
        "itemadapter",
        "itemloaders",
        "cssselect",
        "protego",
        "service_identity",
        "pydispatch",
    )
    if importlib.util.find_spec(package_name) is not None
)
MAIN_RUNTIME_HIDDEN_IMPORTS = (
    "encodings.idna",
    "src.admin_bridge",
    "src.app_version",
    "src.baluffo_config",
    "src.contracts",
    "src.exceptions",
    "src.fetcher_metrics",
    "src.jobs_fetcher",
    "src.jobs_fetcher_registry",
    "src.local_data_store_attachments",
    "src.local_data_store_backup",
    "src.local_data_store_profiles",
    "src.local_data_store_saved_jobs",
    "src.local_data_store_shared",
    "src.local_data_store_tracking",
    "src.local_data_store",
    "src.pipeline_io",
    "src.shared",
    "src.shared.github_https",
    "src.shared.regex",
    "src.shared.utils",
    "src.ship.migrations",
    "src.ship.desktop_update",
    "src.ship.desktop_updater",
    "src.ship.runtime_launcher",
    "src.ship.startup_profile",
    "src.ship.update_manager",
    "src.source_discovery",
    "src.source_registry",
    "src.source_sync_config",
    "src.source_sync_crypto",
    "src.source_sync_runtime",
    "src.source_sync_snapshot",
    "src.source_sync",
    *OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES,
    *OPTIONAL_SCRAPY_RUNTIME_PACKAGES,
    "sqlite3",
    "tkinter",
    "tkinter.ttk",
)
MAIN_RUNTIME_COLLECT_DATA_PACKAGES = (
    *OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES,
    *STORAGE_RUNTIME_COLLECT_DATA_PACKAGES,
)
MAIN_RUNTIME_COLLECT_ALL_PACKAGES = tuple(
    package_name
    for package_name in OPTIONAL_SCRAPY_RUNTIME_PACKAGES
    if package_name not in {"twisted", "queuelib"}
)
SCRAPY_VERSION_METADATA_PACKAGES = (
    "lxml",
    "cssselect",
    "parsel",
    "w3lib",
    "Twisted",
    "cryptography",
)
MAIN_RUNTIME_COPY_METADATA_PACKAGES = _available_metadata_packages(SCRAPY_VERSION_METADATA_PACKAGES)
# PyInstaller's broad Scrapy/Twisted collection otherwise walks test-only dependency
# trees and reports missing optional test plugins; keep runtime packages, exclude tests.
MAIN_RUNTIME_EXCLUDED_MODULES = (
    "twisted.trial.test",
    "twisted.trial._dist.test",
    "twisted.test",
    "twisted.internet.test",
    "twisted.python.test",
    "twisted.protocols.test",
    "twisted.conch.test",
    "twisted.application.test",
    "twisted.web.test",
    "twisted.words.test",
    "twisted._threads.test",
    "queuelib.tests",
    "pytest",
    "_pytest",
    "hypothesis",
    "hypothesis.strategies",
    "pydantic.v1._hypothesis_plugin",
    "hamcrest",
)
UPDATER_HELPER_HIDDEN_IMPORTS = (
    "encodings.idna",
    "src.shared.github_https",
    *OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES,
    "tkinter",
    "tkinter.ttk",
)
UPDATER_HELPER_COLLECT_DATA_PACKAGES = OPTIONAL_GITHUB_TLS_RUNTIME_PACKAGES
# The desktop_app facade statically imports both platform modules; PyInstaller
# must bundle them or the packaged EXE crashes at startup. Kept as a build-time
# constant so the frozen-bundle validation below and the regression tests share
# one contract.
REQUIRED_FROZEN_DESKTOP_MODULES = (
    "src.ship.desktop_app._windows",
    "src.ship.desktop_app._linux",
)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_ship_bundle import DEFAULT_BUNDLE_VERSION, build_bundle
from src.python_version_guard import ensure_required_python
from src.ship.update_manager_paths import REQUIRED_VERSION_FILES


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_text(text: str) -> str:
    return _sha256_bytes(str(text).encode("utf-8", errors="surrogatepass"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _relative_key(path: Path) -> str:
    resolved = Path(path).expanduser().resolve()
    try:
        return resolved.relative_to(ROOT).as_posix()
    except ValueError:
        return resolved.name


def _iter_build_input_paths() -> list[Path]:
    paths: list[Path] = []
    seen: set[Path] = set()

    def add(candidate: Path) -> None:
        try:
            resolved = candidate.expanduser().resolve()
        except OSError:
            return
        if resolved in seen or not resolved.is_file():
            return
        seen.add(resolved)
        paths.append(resolved)

    for rel_path in BUILD_INPUT_FILES:
        add(ROOT / rel_path)
    for rel_dir in BUILD_INPUT_DIRS:
        root_dir = ROOT / rel_dir
        if not root_dir.is_dir():
            continue
        for path in root_dir.rglob("*"):
            if not path.is_file():
                continue
            parts = set(path.parts)
            if parts.intersection(BUILD_SKIP_DIR_NAMES):
                continue
            if path.suffix.lower() in BUILD_SKIP_FILE_SUFFIXES:
                continue
            add(path)
    return sorted(paths, key=_relative_key)


def _safe_package_version(package_name: str) -> str:
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return ""


def _installed_playwright_browser_revision() -> str:
    try:
        import playwright

        browsers_json = (
            Path(playwright.__file__).resolve().parent / "driver" / "package" / "browsers.json"
        )
        payload = json.loads(browsers_json.read_text(encoding="utf-8"))
    except Exception:
        return ""
    browsers = payload.get("browsers") if isinstance(payload, dict) else None
    if not isinstance(browsers, list):
        return ""
    for browser in browsers:
        if isinstance(browser, dict) and browser.get("name") == PLAYWRIGHT_BROWSER_NAME:
            return str(browser.get("revision") or "").strip()
    return ""


def _git_remote_digest() -> str:
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return ""
    if result.returncode != 0:
        return ""
    return _sha256_text(str(result.stdout or "").strip())


def _env_digests(env: dict[str, str] | None = None) -> dict[str, dict[str, str | bool]]:
    env_map = env if env is not None else os.environ
    rows: dict[str, dict[str, str | bool]] = {}
    for name in sorted(set(BUILD_ENV_VALUE_NAMES).union(BUILD_ENV_FILE_VALUE_NAMES)):
        raw_value = str(env_map.get(name) or "")
        row: dict[str, str | bool] = {
            "present": bool(raw_value),
            "valueSha256": _sha256_text(raw_value) if raw_value else "",
        }
        if name in BUILD_ENV_FILE_VALUE_NAMES and raw_value:
            candidate = Path(raw_value).expanduser()
            try:
                resolved = candidate.resolve()
                row["filePresent"] = resolved.is_file()
                row["fileSha256"] = _sha256_file(resolved) if resolved.is_file() else ""
            except OSError:
                row["filePresent"] = False
                row["fileSha256"] = ""
        rows[name] = row
    return rows


def _file_digests() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in _iter_build_input_paths():
        rows.append({"path": _relative_key(path), "sha256": _sha256_file(path)})
    return rows


def portable_build_fingerprint(
    *,
    version: str,
    exe_name: str = DEFAULT_EXE_NAME,
    icon_path: Path | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, object]:
    resolved_icon = Path(icon_path or DEFAULT_ICON_PATH).expanduser().resolve()
    inputs: dict[str, object] = {
        "schemaVersion": CACHE_SCHEMA_VERSION,
        "bundleVersion": str(version or DEFAULT_BUNDLE_VERSION).strip(),
        "exeName": str(exe_name or DEFAULT_EXE_NAME).strip(),
        "icon": {
            "present": resolved_icon.is_file(),
            "sha256": _sha256_file(resolved_icon) if resolved_icon.is_file() else "",
            "suffix": resolved_icon.suffix.lower(),
        },
        "python": {
            "version": sys.version,
            "executableName": Path(sys.executable).name,
            "platform": sys.platform,
        },
        "packages": {name: _safe_package_version(name) for name in PACKAGE_VERSION_NAMES},
        "playwrightBrowserRevision": _installed_playwright_browser_revision(),
        "gitRemoteSha256": _git_remote_digest(),
        "pyinstaller": {
            "hiddenImports": list(MAIN_RUNTIME_HIDDEN_IMPORTS),
            "collectData": list(MAIN_RUNTIME_COLLECT_DATA_PACKAGES),
            "collectAll": list(MAIN_RUNTIME_COLLECT_ALL_PACKAGES),
            "copyMetadata": list(MAIN_RUNTIME_COPY_METADATA_PACKAGES),
            "excludedModules": list(MAIN_RUNTIME_EXCLUDED_MODULES),
            "updaterHiddenImports": list(UPDATER_HELPER_HIDDEN_IMPORTS),
            "updaterCollectData": list(UPDATER_HELPER_COLLECT_DATA_PACKAGES),
        },
        "envDigests": _env_digests(env),
        "files": _file_digests(),
    }
    fingerprint = _sha256_text(json.dumps(inputs, sort_keys=True, separators=(",", ":")))
    return {"fingerprint": fingerprint, "inputs": inputs}


def _cache_entry_dir(fingerprint: str, cache_root: Path = BUILD_CACHE_ROOT) -> Path:
    return Path(cache_root).expanduser().resolve() / str(fingerprint)


def _cache_portable_dir(fingerprint: str, cache_root: Path = BUILD_CACHE_ROOT) -> Path:
    return _cache_entry_dir(fingerprint, cache_root) / "portable"


def _is_portable_cache_entry(path: Path) -> bool:
    name = str(path.name or "")
    return bool(path.is_dir() and len(name) == 64 and all(ch in "0123456789abcdef" for ch in name))


def _cache_entry_sort_key(path: Path) -> float:
    manifest_path = path / CACHE_MANIFEST_FILE
    try:
        return manifest_path.stat().st_mtime
    except OSError:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0


def _prune_portable_build_cache(
    *,
    cache_root: Path = BUILD_CACHE_ROOT,
    retain_entries: int = DEFAULT_CACHE_RETAIN_ENTRIES,
    keep_fingerprints: tuple[str, ...] = (),
) -> list[Path]:
    retain_count = max(0, int(retain_entries or 0))
    if retain_count <= 0:
        return []
    root = Path(cache_root).expanduser().resolve()
    if not root.is_dir():
        return []
    keep_names = {str(fingerprint) for fingerprint in keep_fingerprints if fingerprint}
    entries = sorted(
        (entry for entry in root.iterdir() if _is_portable_cache_entry(entry)),
        key=_cache_entry_sort_key,
        reverse=True,
    )
    retained: set[str] = set(keep_names)
    for entry in entries:
        if len(retained) >= retain_count:
            break
        retained.add(entry.name)
    removed: list[Path] = []
    for entry in entries:
        if entry.name in retained:
            continue
        _remove_path_with_retry(entry)
        removed.append(entry)
    return removed


def _remove_path_with_retry(path: Path) -> None:
    if not path.exists():
        return
    last_error: Exception | None = None
    removal_path = path
    for _ in range(10):
        try:
            if removal_path == path:
                stale_path = path.with_name(f"{path.name}.delete-{os.getpid()}-{time.time_ns()}")
                path.replace(stale_path)
                removal_path = stale_path
            if removal_path.is_dir():
                shutil.rmtree(removal_path)
            else:
                removal_path.unlink()
            last_error = None
            break
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.25)
    if removal_path.exists():
        raise RuntimeError(f"Could not clear path due to a file lock: {path}") from last_error


def _copy_portable_tree(source: Path, target: Path) -> None:
    resolved_source = Path(source).expanduser().resolve()
    resolved_target = Path(target).expanduser().resolve()
    if resolved_source == resolved_target:
        return
    _remove_path_with_retry(resolved_target)
    resolved_target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(resolved_source, resolved_target)


def _portable_tree_valid(portable_root: Path, *, exe_name: str = DEFAULT_EXE_NAME) -> bool:
    root = Path(portable_root).expanduser().resolve()
    if not (root / f"{exe_name}.exe").is_file():
        return False
    if not (root / "BaluffoUpdater.exe").is_file():
        return False
    if not (root / "ship").is_dir():
        return False
    try:
        validate_playwright_browser_payload(root)
    except RuntimeError:
        return False
    return True


def _cache_entry_valid(
    *,
    fingerprint: str,
    cache_root: Path = BUILD_CACHE_ROOT,
    exe_name: str = DEFAULT_EXE_NAME,
) -> bool:
    entry = _cache_entry_dir(fingerprint, cache_root)
    manifest_path = entry / CACHE_MANIFEST_FILE
    portable = entry / "portable"
    if not manifest_path.is_file() or not portable.is_dir():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if str(manifest.get("fingerprint") or "") != str(fingerprint):
        return False
    return _portable_tree_valid(portable, exe_name=exe_name)


def portable_build_provenance_path(portable_root: Path) -> Path:
    return Path(portable_root).expanduser().resolve() / PORTABLE_BUILD_PROVENANCE_FILE


def read_portable_build_provenance(portable_root: Path) -> dict[str, object]:
    path = portable_build_provenance_path(portable_root)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_portable_build_provenance(
    portable_root: Path,
    *,
    fingerprint: str,
    version: str,
    exe_name: str,
    cache_status: str,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schemaVersion": CACHE_SCHEMA_VERSION,
        "fingerprint": str(fingerprint),
        "bundleVersion": str(version),
        "exeName": str(exe_name),
        "cacheStatus": str(cache_status),
        "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    portable_build_provenance_path(portable_root).write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def _store_cache_entry(
    output_dir: Path,
    *,
    fingerprint_payload: dict[str, object],
    cache_root: Path = BUILD_CACHE_ROOT,
    exe_name: str = DEFAULT_EXE_NAME,
) -> Path:
    fingerprint = str(fingerprint_payload["fingerprint"])
    if not _portable_tree_valid(output_dir, exe_name=exe_name):
        raise RuntimeError(f"Portable build is incomplete; refusing to cache: {output_dir}")
    entry = _cache_entry_dir(fingerprint, cache_root)
    temp_entry = entry.with_name(f"{entry.name}.tmp-{os.getpid()}")
    _remove_path_with_retry(temp_entry)
    temp_entry.mkdir(parents=True, exist_ok=True)
    shutil.copytree(Path(output_dir).expanduser().resolve(), temp_entry / "portable")
    manifest = {
        "schemaVersion": CACHE_SCHEMA_VERSION,
        "fingerprint": fingerprint,
        "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "inputs": fingerprint_payload.get("inputs") or {},
    }
    (temp_entry / CACHE_MANIFEST_FILE).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _remove_path_with_retry(entry)
    try:
        temp_entry.replace(entry)
    except PermissionError:
        _remove_path_with_retry(entry)
        entry.mkdir(parents=True, exist_ok=True)
        shutil.copytree(temp_entry / "portable", entry / "portable")
        shutil.copy2(temp_entry / CACHE_MANIFEST_FILE, entry / CACHE_MANIFEST_FILE)
        _remove_path_with_retry(temp_entry)
    return entry


def portable_build_status(
    portable_root: Path,
    *,
    version: str = DEFAULT_BUNDLE_VERSION,
    exe_name: str = DEFAULT_EXE_NAME,
    icon_path: Path | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, object]:
    resolved_root = Path(portable_root).expanduser().resolve()
    fingerprint_payload = portable_build_fingerprint(
        version=version,
        exe_name=exe_name,
        icon_path=icon_path or DEFAULT_ICON_PATH,
        env=env,
    )
    expected = str(fingerprint_payload["fingerprint"])
    provenance = read_portable_build_provenance(resolved_root)
    actual = str(provenance.get("fingerprint") or "")
    exe_missing = not (resolved_root / f"{exe_name}.exe").is_file()
    tree_valid = bool(not exe_missing and _portable_tree_valid(resolved_root, exe_name=exe_name))
    fresh = bool(tree_valid and actual and actual == expected)
    if exe_missing:
        status = "missing"
    elif not tree_valid:
        status = "unusable"
    elif not actual:
        status = "unproven"
    elif actual != expected:
        status = "stale"
    else:
        status = "fresh"
    return {
        "fresh": fresh,
        "status": status,
        "expectedFingerprint": expected,
        "actualFingerprint": actual,
        "provenance": provenance,
    }


def _playwright_browser_cache_candidates() -> list[Path]:
    candidates: list[Path] = []
    env_path = str(os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").strip()
    if env_path and env_path != "0":
        candidates.append(Path(env_path).expanduser())
    local_app_data = str(os.environ.get("LOCALAPPDATA") or "").strip()
    if local_app_data:
        candidates.append(Path(local_app_data) / "ms-playwright")
    candidates.append(Path.home() / "AppData" / "Local" / "ms-playwright")
    try:
        import playwright

        package_cache = (
            Path(playwright.__file__).resolve().parent / "driver" / "package" / ".local-browsers"
        )
        candidates.append(package_cache)
    except Exception:
        pass

    out: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        out.append(resolved)
    return out


def _playwright_package_dir(output_dir: Path) -> Path:
    return output_dir / "_internal" / "playwright" / "driver" / "package"


def _required_playwright_browser_cache_name(package_dir: Path) -> str:
    browsers_json = package_dir / "browsers.json"
    if not browsers_json.is_file():
        raise RuntimeError(f"Packaged Playwright browsers.json missing: {browsers_json}")
    try:
        payload = json.loads(browsers_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Could not read packaged Playwright browsers.json: {browsers_json}"
        ) from exc
    browsers = payload.get("browsers") if isinstance(payload, dict) else None
    if not isinstance(browsers, list):
        raise RuntimeError(
            f"Packaged Playwright browsers.json has no browsers list: {browsers_json}"
        )
    for browser in browsers:
        if not isinstance(browser, dict) or browser.get("name") != PLAYWRIGHT_BROWSER_NAME:
            continue
        revision = str(browser.get("revision") or "").strip()
        if not revision:
            raise RuntimeError(
                f"Packaged Playwright {PLAYWRIGHT_BROWSER_NAME} entry has no revision: "
                f"{browsers_json}"
            )
        return f"{PLAYWRIGHT_BROWSER_CACHE_PREFIX}-{revision}"
    raise RuntimeError(
        f"Packaged Playwright browsers.json has no {PLAYWRIGHT_BROWSER_NAME} entry: {browsers_json}"
    )


def _has_chromium_headless_shell(
    cache_dir: Path, *, required_browser_cache_name: str | None = None
) -> bool:
    if not cache_dir.is_dir():
        return False
    candidates = (
        [cache_dir / required_browser_cache_name]
        if required_browser_cache_name
        else [
            child
            for child in cache_dir.iterdir()
            if child.is_dir() and child.name.startswith(f"{PLAYWRIGHT_BROWSER_CACHE_PREFIX}-")
        ]
    )
    for child in candidates:
        if child.is_dir() and any(child.rglob(PLAYWRIGHT_BROWSER_EXE_NAME)):
            return True
    return False


def resolve_playwright_browser_cache(required_browser_cache_name: str | None = None) -> Path | None:
    for candidate in _playwright_browser_cache_candidates():
        if _has_chromium_headless_shell(
            candidate, required_browser_cache_name=required_browser_cache_name
        ):
            return candidate
    return None


def copy_playwright_browser_cache(
    output_dir: Path, *, source_cache: Path | None = None
) -> Path | None:
    package_dir = _playwright_package_dir(output_dir)
    if not package_dir.is_dir():
        raise RuntimeError(f"Packaged Playwright driver directory missing: {package_dir}")
    required_browser_cache_name = _required_playwright_browser_cache_name(package_dir)
    cache = (
        Path(source_cache).expanduser().resolve()
        if source_cache
        else resolve_playwright_browser_cache(required_browser_cache_name)
    )
    if cache is None or not _has_chromium_headless_shell(
        cache, required_browser_cache_name=required_browser_cache_name
    ):
        searched = ", ".join(str(path) for path in _playwright_browser_cache_candidates())
        raise RuntimeError(
            "Portable build requires the packaged Playwright Chromium Headless Shell "
            f"browser cache entry {required_browser_cache_name}. "
            f"Could not find it in: {searched}. "
            "Run `python -m playwright install chromium` and rebuild."
        )
    target = package_dir / ".local-browsers"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    shutil.copytree(cache / required_browser_cache_name, target / required_browser_cache_name)
    validate_playwright_browser_payload(output_dir)
    return target


def _copy_tree_contents(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(item, target)
        else:
            shutil.copy2(item, target)


def resolve_icon_path(icon_arg: str = "") -> Path:
    if str(icon_arg or "").strip():
        icon_path = Path(icon_arg).expanduser().resolve()
        if not icon_path.exists():
            raise RuntimeError(f"Icon file not found: {icon_path}")
        return icon_path
    if not DEFAULT_ICON_PATH.exists():
        raise RuntimeError(f"Default icon file not found: {DEFAULT_ICON_PATH}")
    return DEFAULT_ICON_PATH


def build_portable_layout(output_dir: Path, version: str) -> Path:
    if output_dir.exists():
        _remove_path_with_retry(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    build_bundle(output_dir / "ship", version)
    return output_dir


def run_pyinstaller(
    output_dir: Path, *, exe_name: str, icon_path: Path, bundle_version: str
) -> Path:
    pyinstaller_dist = output_dir.parent / ".pyinstaller-dist"
    pyinstaller_work = output_dir.parent / ".pyinstaller-work"
    pyinstaller_spec = output_dir.parent / ".pyinstaller-spec"
    for path in (pyinstaller_dist, pyinstaller_work, pyinstaller_spec):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onedir",
        "--windowed",
        "--name",
        exe_name,
        "--icon",
        str(icon_path),
        "--distpath",
        str(pyinstaller_dist),
        "--workpath",
        str(pyinstaller_work),
        "--specpath",
        str(pyinstaller_spec),
    ]
    for module_name in MAIN_RUNTIME_HIDDEN_IMPORTS:
        command.extend(["--hidden-import", module_name])
    for package_name in MAIN_RUNTIME_COLLECT_DATA_PACKAGES:
        command.extend(["--collect-data", package_name])
    for package_name in MAIN_RUNTIME_COLLECT_ALL_PACKAGES:
        command.extend(["--collect-all", package_name])
    for package_name in MAIN_RUNTIME_COPY_METADATA_PACKAGES:
        command.extend(["--copy-metadata", package_name])
    for module_name in MAIN_RUNTIME_EXCLUDED_MODULES:
        command.extend(["--exclude-module", module_name])
    ship_version_dir = output_dir / "ship" / "app" / "versions" / bundle_version
    for rel in REQUIRED_VERSION_FILES:
        src_file = (ship_version_dir / rel).resolve()
        if not src_file.is_file():
            raise RuntimeError(f"Portable build cannot embed missing ship file: {src_file}")
        data_dest = "baluffo_embed/src" if rel.startswith("src/") else "baluffo_embed"
        command.extend(["--add-data", f"{src_file}{os.pathsep}{data_dest}"])
    # Ship desktop app is a package; PyInstaller needs a real script entrypoint.
    command.append(str(ROOT / "src" / "ship" / "desktop_app" / "__main__.py"))
    subprocess.run(command, check=True, cwd=str(ROOT))
    built_dir = pyinstaller_dist / exe_name
    if not built_dir.exists():
        raise RuntimeError(f"PyInstaller output not found: {built_dir}")
    _copy_tree_contents(built_dir, output_dir)
    copy_playwright_browser_cache(output_dir)
    exe_path = output_dir / f"{exe_name}.exe"
    if not exe_path.exists():
        raise RuntimeError(f"Portable executable not found: {exe_path}")
    return exe_path


def read_frozen_pyz_modules(exe_path: Path) -> set[str]:
    """Return the pure-Python module names embedded in a PyInstaller EXE's PYZ.

    PyInstaller 6.x onedir builds embed the module archive inside the bootloader
    executable itself rather than as a separate ``.pkg``/``.pyz`` file; the
    layout is pinned by requirements.txt.
    """
    from PyInstaller.archive.readers import CArchiveReader, ZlibArchiveReader

    resolved = Path(exe_path).expanduser().resolve()
    archive = CArchiveReader(str(resolved))
    if "PYZ.pyz" not in archive.toc:
        raise RuntimeError(
            f"Bundled executable {resolved} has no PYZ.pyz entry; "
            "PyInstaller archive layout may have changed."
        )
    pyz_data = archive.extract("PYZ.pyz")
    fd, temp_name = tempfile.mkstemp(prefix="baluffo-frozen-", suffix=".pyz")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(pyz_data)
        return set(ZlibArchiveReader(temp_name).toc)
    finally:
        try:
            os.unlink(temp_name)
        except OSError:
            pass


def validate_frozen_desktop_platform_modules(exe_path: Path) -> None:
    """Fail fast when a frozen EXE bundle is missing desktop platform modules.

    The desktop_app facade statically imports both platform modules; if that
    contract regresses to a dynamic importlib dispatch, PyInstaller cannot see
    the inactive module and the packaged EXE crashes at startup with
    ModuleNotFoundError. The portable build calls this right after PyInstaller
    (and on cache hits) so ``npm run verify`` aborts at the PortableEXE stage
    instead of after the long test suites.
    """
    frozen_modules = read_frozen_pyz_modules(exe_path)
    missing = [
        module_name
        for module_name in REQUIRED_FROZEN_DESKTOP_MODULES
        if module_name not in frozen_modules
    ]
    if missing:
        raise RuntimeError(
            "Frozen EXE bundle is missing required desktop platform modules: "
            f"{missing}. The desktop_app facade must statically import both "
            "_windows and _linux so PyInstaller bundles them."
        )


def run_helper_pyinstaller(output_dir: Path, *, icon_path: Path) -> Path:
    helper_dist = output_dir.parent / ".pyinstaller-helper-dist"
    helper_work = output_dir.parent / ".pyinstaller-helper-work"
    helper_spec = output_dir.parent / ".pyinstaller-helper-spec"
    for path in (helper_dist, helper_work, helper_spec):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onefile",
        "--windowed",
        "--name",
        "BaluffoUpdater",
        "--icon",
        str(icon_path),
        "--distpath",
        str(helper_dist),
        "--workpath",
        str(helper_work),
        "--specpath",
        str(helper_spec),
    ]
    for module_name in UPDATER_HELPER_HIDDEN_IMPORTS:
        command.extend(["--hidden-import", module_name])
    for package_name in UPDATER_HELPER_COLLECT_DATA_PACKAGES:
        command.extend(["--collect-data", package_name])
    command.append(str(ROOT / "src" / "ship" / "desktop_updater.py"))
    subprocess.run(command, check=True, cwd=str(ROOT))
    helper_exe = helper_dist / "BaluffoUpdater.exe"
    if not helper_exe.exists():
        raise RuntimeError(f"Updater helper executable not found: {helper_exe}")
    target = output_dir / "BaluffoUpdater.exe"
    shutil.copy2(helper_exe, target)
    return target


def create_zip(output_dir: Path, *, version: str) -> Path:
    validate_playwright_browser_payload(output_dir)
    archive_base = output_dir.parent / f"{output_dir.name}-{version}"
    archive_path = archive_base.parent / f"{archive_base.name}.zip"
    if archive_path.exists():
        archive_path.unlink()
    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED, compresslevel=9) as zip_file:
        for path in sorted(output_dir.rglob("*")):
            rel = path.relative_to(output_dir).as_posix()
            if path.is_dir():
                zip_file.write(path, f"{rel}/")
            else:
                zip_file.write(path, rel)
    return archive_path


def validate_playwright_browser_payload(output_dir: Path) -> None:
    package_dir = _playwright_package_dir(output_dir)
    if not package_dir.is_dir():
        return
    required_browser_cache_name = _required_playwright_browser_cache_name(package_dir)
    local_browsers = package_dir / ".local-browsers"
    if not local_browsers.is_dir():
        raise RuntimeError(f"Portable Playwright browser payload missing: {local_browsers}")
    unexpected = sorted(
        child.name
        for child in local_browsers.iterdir()
        if child.name != required_browser_cache_name
    )
    if unexpected:
        raise RuntimeError(
            "Portable Playwright browser payload must contain only "
            f"{required_browser_cache_name}; found unexpected entries: {', '.join(unexpected)}"
        )
    if not _has_chromium_headless_shell(
        local_browsers, required_browser_cache_name=required_browser_cache_name
    ):
        raise RuntimeError(
            "Portable Playwright browser payload is missing "
            f"{required_browser_cache_name}/{PLAYWRIGHT_BROWSER_EXE_NAME}"
        )


def mirror_latest_portable(output_dir: Path, latest_dir: Path = LATEST_PORTABLE_DIR) -> Path:
    """Mirror the successful portable output to the familiar latest artifact path."""
    source = Path(output_dir).expanduser().resolve()
    target = Path(latest_dir).expanduser().resolve()
    if not (source / "Baluffo.exe").is_file():
        raise RuntimeError(f"Portable executable missing; refusing latest mirror: {source}")
    if source == target:
        return target
    _copy_portable_tree(source, target)
    return target


def build_or_reuse_portable(
    *,
    output_dir: Path,
    version: str,
    exe_name: str,
    icon_path: Path,
    force: bool = False,
    cache_root: Path = BUILD_CACHE_ROOT,
    cache_retain_entries: int = DEFAULT_CACHE_RETAIN_ENTRIES,
) -> tuple[Path, Path, dict[str, object]]:
    fingerprint_payload = portable_build_fingerprint(
        version=version,
        exe_name=exe_name,
        icon_path=icon_path,
    )
    fingerprint = str(fingerprint_payload["fingerprint"])
    cache_hit = (not force) and _cache_entry_valid(
        fingerprint=fingerprint,
        cache_root=cache_root,
        exe_name=exe_name,
    )
    if cache_hit:
        cache_portable = _cache_portable_dir(fingerprint, cache_root)
        _copy_portable_tree(cache_portable, output_dir)
        provenance = _write_portable_build_provenance(
            output_dir,
            fingerprint=fingerprint,
            version=version,
            exe_name=exe_name,
            cache_status="hit",
        )
        print(f"Portable build cache hit: {fingerprint}")
        removed_entries = _prune_portable_build_cache(
            cache_root=cache_root,
            retain_entries=cache_retain_entries,
            keep_fingerprints=(fingerprint,),
        )
        if removed_entries:
            print(f"Portable build cache pruned: {len(removed_entries)} old entrie(s)")
        exe_path = output_dir / f"{exe_name}.exe"
        validate_frozen_desktop_platform_modules(exe_path)
        return exe_path, output_dir / "BaluffoUpdater.exe", provenance

    portable_root = build_portable_layout(output_dir, version)
    exe_path = run_pyinstaller(
        portable_root,
        exe_name=exe_name,
        icon_path=icon_path,
        bundle_version=version,
    )
    validate_frozen_desktop_platform_modules(exe_path)
    helper_path = run_helper_pyinstaller(portable_root, icon_path=icon_path)
    provenance = _write_portable_build_provenance(
        portable_root,
        fingerprint=fingerprint,
        version=version,
        exe_name=exe_name,
        cache_status="force-rebuild" if force else "miss",
    )
    _store_cache_entry(
        portable_root,
        fingerprint_payload=fingerprint_payload,
        cache_root=cache_root,
        exe_name=exe_name,
    )
    print(f"Portable build cache stored: {fingerprint}")
    removed_entries = _prune_portable_build_cache(
        cache_root=cache_root,
        retain_entries=cache_retain_entries,
        keep_fingerprints=(fingerprint,),
    )
    if removed_entries:
        print(f"Portable build cache pruned: {len(removed_entries)} old entrie(s)")
    return exe_path, helper_path, provenance


def _resolve_cache_retain_entries(cli_value: int | None, env: dict[str, str] | None = None) -> int:
    if cli_value is not None:
        return max(0, int(cli_value))
    env_map = env if env is not None else os.environ
    raw = str(env_map.get(PORTABLE_BUILD_CACHE_RETAIN_ENV) or "").strip()
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            raise RuntimeError(
                f"{PORTABLE_BUILD_CACHE_RETAIN_ENV} must be a non-negative integer."
            ) from None
    return DEFAULT_CACHE_RETAIN_ENTRIES


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dist/baluffo-portable executable wrapper.")
    parser.add_argument("--output-dir", default=str(DIST_DIR))
    parser.add_argument("--bundle-version", default=DEFAULT_BUNDLE_VERSION)
    parser.add_argument("--exe-name", default=DEFAULT_EXE_NAME)
    parser.add_argument("--icon", default="")
    parser.add_argument("--skip-zip", action="store_true")
    parser.add_argument("--skip-latest-mirror", action="store_true")
    parser.add_argument("--force", action="store_true", help="Bypass the portable build cache.")
    parser.add_argument(
        "--cache-retain",
        type=int,
        default=None,
        help=(
            "Keep this many content-addressed portable build cache entries after the build "
            f"(default: ${PORTABLE_BUILD_CACHE_RETAIN_ENV} or {DEFAULT_CACHE_RETAIN_ENTRIES}; "
            "0 disables pruning)."
        ),
    )
    return parser.parse_args()


def main() -> int:
    ensure_required_python()
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    version = str(args.bundle_version).strip() or DEFAULT_BUNDLE_VERSION
    exe_name = str(args.exe_name).strip() or DEFAULT_EXE_NAME
    icon_path = resolve_icon_path(str(args.icon or ""))
    exe_path, helper_path, provenance = build_or_reuse_portable(
        output_dir=output_dir,
        version=version,
        exe_name=exe_name,
        icon_path=icon_path,
        force=bool(args.force),
        cache_retain_entries=_resolve_cache_retain_entries(args.cache_retain),
    )
    print(f"Portable executable ready: {exe_path}")
    print(f"Updater helper ready: {helper_path}")
    print(f"Ship bundle root: {output_dir / 'ship'}")
    print(f"Executable icon: {icon_path}")
    print(f"Portable build fingerprint: {provenance.get('fingerprint')}")
    print(f"Portable build cache status: {provenance.get('cacheStatus')}")
    if not args.skip_zip:
        archive = create_zip(output_dir, version=version)
        print(f"Portable archive: {archive}")
    if not args.skip_latest_mirror:
        latest_path = mirror_latest_portable(output_dir)
        print(f"Latest portable mirror: {latest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
