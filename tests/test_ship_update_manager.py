import json
from pathlib import Path
from unittest import mock
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from src.ship import update_manager as um
from tests.helpers.temp_paths import workspace_tmpdir


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_root(root: Path, version: str = "1.0.0") -> None:
    _write(root / "app" / "current.txt", f"{version}\n")
    _write(
        root / "app" / "update-state.json",
        json.dumps(
            {
                "current_version": version,
                "previous_version": "",
                "last_update_status": "ready",
                "last_error_code": "",
                "updated_at": um.iso_now(),
            }
        ),
    )
    (root / "app" / "versions" / version / "src").mkdir(parents=True, exist_ok=True)
    _write(root / "app" / "versions" / version / "src" / "admin_bridge.py", "print('ok')\n")
    _write(root / "app" / "versions" / version / "index.html", "<html></html>\n")
    _write(root / "app" / "versions" / version / "jobs.html", "<html></html>\n")
    _write(root / "app" / "versions" / version / "saved.html", "<html></html>\n")
    (root / "app" / "staging").mkdir(parents=True, exist_ok=True)
    (root / "data" / "backups").mkdir(parents=True, exist_ok=True)
    (root / "data" / "migration-reports").mkdir(parents=True, exist_ok=True)
    _write(root / "data" / "user-settings.json", '{"theme":"dark"}\n')


def _build_update_zip(work: Path, version: str) -> Path:
    source = work / "payload" / "app" / "versions" / version
    _write(source / "src" / "admin_bridge.py", "print('updated')\n")
    _write(source / "index.html", "<html>new</html>\n")
    _write(source / "jobs.html", "<html>new</html>\n")
    _write(source / "saved.html", "<html>new</html>\n")
    bundle = work / f"baluffo-{version}.zip"
    with ZipFile(bundle, "w", compression=ZIP_DEFLATED) as archive:
        for path in source.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(work / "payload").as_posix())
    return bundle


def test_write_json_atomic_retries_transient_permission_error() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        target = Path(tmp) / "ship" / "app" / "update-state.json"
        calls = {"count": 0}
        original_replace = um.os.replace

        def flaky_replace(src, dst):  # noqa: ANN001
            calls["count"] += 1
            if calls["count"] == 1:
                raise PermissionError(32, "sharing violation")
            return original_replace(src, dst)

        with mock.patch.object(um.os, "replace", side_effect=flaky_replace):
            um.write_json_atomic(target, {"ok": True})

        assert json.loads(target.read_text(encoding="utf-8"))["ok"] is True
        assert calls["count"] == 2


def test_ensure_state_resyncs_current_version_from_current_txt() -> None:
    """Stale update-state.json must not point startup at the wrong versions/* folder."""
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="1.0.0")
        _write(
            root / "app" / "update-state.json",
            json.dumps(
                {
                    "current_version": "9.9.9",
                    "previous_version": "",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": um.iso_now(),
                }
            ),
        )
        paths = um.ShipPaths.from_root(root)
        state = um.ensure_state(paths)
        assert state["current_version"] == "1.0.0"
        reparsed = json.loads((root / "app" / "update-state.json").read_text(encoding="utf-8"))
        assert reparsed["current_version"] == "1.0.0"
        result = um.startup_check(root, root / "data")
        assert result["ok"]


@pytest.mark.parametrize("pointer_payload", [None, ""])
def test_ensure_state_repairs_missing_or_empty_current_from_state_current(pointer_payload) -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="1.0.0")
        current_path = root / "app" / "current.txt"
        if pointer_payload is None:
            current_path.unlink()
        else:
            _write(current_path, pointer_payload)

        paths = um.ShipPaths.from_root(root)
        state = um.ensure_state(paths)

        assert state["current_version"] == "1.0.0"
        assert current_path.read_text(encoding="utf-8").strip() == "1.0.0"
        reparsed = json.loads((root / "app" / "update-state.json").read_text(encoding="utf-8"))
        assert reparsed["current_version"] == "1.0.0"
        assert reparsed["previous_version"] == ""


def test_ensure_state_repairs_missing_current_from_healthy_previous_version() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="1.0.0")
        _seed_full_version(root, "0.9.0")
        (root / "app" / "current.txt").unlink()
        _write(
            root / "app" / "update-state.json",
            json.dumps(
                {
                    "current_version": "9.9.9",
                    "previous_version": "0.9.0",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": um.iso_now(),
                }
            ),
        )

        paths = um.ShipPaths.from_root(root)
        state = um.ensure_state(paths)

        assert state["current_version"] == "0.9.0"
        assert state["previous_version"] == ""
        assert (root / "app" / "current.txt").read_text(encoding="utf-8").strip() == "0.9.0"


def test_ensure_state_repairs_missing_current_from_highest_healthy_version() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="0.8.0")
        _seed_full_version(root, "1.1.0")
        _seed_full_version(root, "2.4.0")
        (root / "app" / "current.txt").unlink()
        _write(
            root / "app" / "update-state.json",
            json.dumps(
                {
                    "current_version": "9.9.9",
                    "previous_version": "8.8.8",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": um.iso_now(),
                }
            ),
        )

        paths = um.ShipPaths.from_root(root)
        state = um.ensure_state(paths)

        assert state["current_version"] == "2.4.0"
        assert (root / "app" / "current.txt").read_text(encoding="utf-8").strip() == "2.4.0"


def test_ensure_state_raises_when_missing_current_has_no_healthy_recovery() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="1.0.0")
        (root / "app" / "current.txt").unlink()
        broken = root / "app" / "versions" / "1.0.0"
        (broken / "src" / "admin_bridge.py").unlink()
        _write(
            root / "app" / "update-state.json",
            json.dumps(
                {
                    "current_version": "9.9.9",
                    "previous_version": "8.8.8",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": um.iso_now(),
                }
            ),
        )

        paths = um.ShipPaths.from_root(root)
        with pytest.raises(RuntimeError, match="missing or empty.*no recoverable healthy version"):
            um.ensure_state(paths)


def test_startup_check_rejects_data_dir_inside_versions() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root)
        bad_data_dir = root / "app" / "versions" / "1.0.0" / "data"
        bad_data_dir.mkdir(parents=True, exist_ok=True)
        with pytest.raises(ValueError):
            um.startup_check(root, bad_data_dir)


def _seed_full_version(root: Path, version: str) -> None:
    base = root / "app" / "versions" / version
    (base / "src").mkdir(parents=True, exist_ok=True)
    _write(base / "src" / "admin_bridge.py", f"print('{version}')\n")
    _write(base / "index.html", "<html></html>\n")
    _write(base / "jobs.html", "<html></html>\n")
    _write(base / "saved.html", "<html></html>\n")


def test_startup_check_auto_selects_healthy_version_when_current_broken() -> None:
    """If current.txt points at an incomplete tree, use another healthy app/versions/* folder."""
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="0.9.0")
        vbroken = root / "app" / "versions" / "2.0.0"
        (vbroken / "src").mkdir(parents=True, exist_ok=True)
        _write(vbroken / "index.html", "<html></html>\n")
        _write(vbroken / "jobs.html", "<html></html>\n")
        _write(vbroken / "saved.html", "<html></html>\n")
        _write(root / "app" / "current.txt", "2.0.0\n")
        _write(
            root / "app" / "update-state.json",
            json.dumps(
                {
                    "current_version": "2.0.0",
                    "previous_version": "",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": um.iso_now(),
                }
            ),
        )
        result = um.startup_check(root, root / "data")
        assert result["ok"] is True
        assert result["current_version"] == "0.9.0"
        assert result.get("repaired_pointer") is True
        assert (root / "app" / "current.txt").read_text(encoding="utf-8").strip() == "0.9.0"


def test_startup_check_prefers_highest_healthy_semver_when_current_broken() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="0.9.0")
        _seed_full_version(root, "1.1.0")
        vbroken = root / "app" / "versions" / "3.0.0"
        (vbroken / "src").mkdir(parents=True, exist_ok=True)
        _write(vbroken / "index.html", "<html></html>\n")
        _write(vbroken / "jobs.html", "<html></html>\n")
        _write(vbroken / "saved.html", "<html></html>\n")
        _write(root / "app" / "current.txt", "3.0.0\n")
        _write(
            root / "app" / "update-state.json",
            json.dumps(
                {
                    "current_version": "3.0.0",
                    "previous_version": "",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": um.iso_now(),
                }
            ),
        )
        result = um.startup_check(root, root / "data")
        assert result["ok"] is True
        assert result["current_version"] == "1.1.0"


def test_startup_check_skips_unhealthy_previous_and_scans_versions() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="0.8.0")
        _write(root / "app" / "current.txt", "9.0.0\n")
        vbroken_prev = root / "app" / "versions" / "8.0.0"
        (vbroken_prev / "src").mkdir(parents=True, exist_ok=True)
        _write(vbroken_prev / "index.html", "<html></html>\n")
        _write(vbroken_prev / "jobs.html", "<html></html>\n")
        _write(vbroken_prev / "saved.html", "<html></html>\n")
        vbroken_cur = root / "app" / "versions" / "9.0.0"
        (vbroken_cur / "src").mkdir(parents=True, exist_ok=True)
        _write(vbroken_cur / "index.html", "<html></html>\n")
        _write(vbroken_cur / "jobs.html", "<html></html>\n")
        _write(vbroken_cur / "saved.html", "<html></html>\n")
        _write(
            root / "app" / "update-state.json",
            json.dumps(
                {
                    "current_version": "9.0.0",
                    "previous_version": "8.0.0",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": um.iso_now(),
                }
            ),
        )
        result = um.startup_check(root, root / "data")
        assert result["ok"] is True
        assert result["current_version"] == "0.8.0"
        assert result.get("repaired_pointer") is True


def test_bootstrap_repair_is_noop_when_canonical_tag_mismatches_active_version() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="1.0.0")
        paths = um.ShipPaths.from_root(root)
        um.refresh_runtime_bootstrap(
            paths, root / "app" / "versions" / "1.0.0", version_name="1.0.0"
        )
        broken = root / "app" / "versions" / "2.0.0"
        (broken / "src").mkdir(parents=True, exist_ok=True)
        assert um.repair_version_from_runtime_bootstrap(paths, broken, "2.0.0") == 0


def test_startup_check_repairs_current_from_runtime_bootstrap() -> None:
    """Missing files under the active version are restored from ``app/runtime-bootstrap``."""
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="1.0.0")
        paths = um.ShipPaths.from_root(root)
        canon = root / "app" / "versions" / "1.0.0"
        um.refresh_runtime_bootstrap(paths, canon, version_name="1.0.0")
        (canon / "src" / "admin_bridge.py").unlink()
        assert not (canon / "src" / "admin_bridge.py").exists()
        result = um.startup_check(root, root / "data")
        assert result["ok"] is True
        assert int(result.get("bootstrap_repair") or 0) >= 1
        assert (canon / "src" / "admin_bridge.py").exists()


def test_apply_update_success_switches_current_version_and_keeps_data() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="1.0.0")
        bundle = _build_update_zip(Path(tmp), "1.1.0")
        sha256 = um.compute_sha256(bundle)
        key = "test-key"
        manifest = {
            "version": "1.1.0",
            "artifact_url": "file://local",
            "sha256": sha256,
            "signature": um.sign_manifest("1.1.0", sha256, key),
            "min_updater_version": "1.0.0",
            "migration_plan": ["noop"],
            "rollback_allowed": False,
        }
        manifest_path = Path(tmp) / "update-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = um.apply_update(root, bundle, manifest_path, key)
        assert result["ok"]
        assert (root / "app" / "current.txt").read_text(encoding="utf-8").strip() == "1.1.0"
        assert (root / "app" / "runtime-bootstrap" / "src" / "admin_bridge.py").is_file()
        assert (
            json.loads((root / "data" / "user-settings.json").read_text(encoding="utf-8"))["theme"]
            == "dark"
        )


def test_apply_update_rejects_checksum_mismatch() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="1.0.0")
        bundle = _build_update_zip(Path(tmp), "1.1.0")
        key = "test-key"
        manifest = {
            "version": "1.1.0",
            "artifact_url": "file://local",
            "sha256": "0" * 64,
            "signature": um.sign_manifest("1.1.0", "0" * 64, key),
            "min_updater_version": "1.0.0",
            "migration_plan": [],
            "rollback_allowed": False,
        }
        manifest_path = Path(tmp) / "update-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        with pytest.raises(ValueError):
            um.apply_update(root, bundle, manifest_path, key)
        assert (root / "app" / "current.txt").read_text(encoding="utf-8").strip() == "1.0.0"


def test_recover_previous_swaps_versions() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="1.1.0")
        (root / "app" / "versions" / "1.0.0" / "src").mkdir(parents=True, exist_ok=True)
        _write(root / "app" / "versions" / "1.0.0" / "src" / "admin_bridge.py", "print('ok')\n")
        _write(root / "app" / "versions" / "1.0.0" / "index.html", "<html></html>\n")
        _write(root / "app" / "versions" / "1.0.0" / "jobs.html", "<html></html>\n")
        _write(root / "app" / "versions" / "1.0.0" / "saved.html", "<html></html>\n")
        state = json.loads((root / "app" / "update-state.json").read_text(encoding="utf-8"))
        state["previous_version"] = "1.0.0"
        (root / "app" / "update-state.json").write_text(json.dumps(state), encoding="utf-8")

        result = um.recover_previous(root)
        assert result["ok"]
        assert (root / "app" / "current.txt").read_text(encoding="utf-8").strip() == "1.0.0"


def test_validate_manifest_uses_numeric_semver_for_min_updater_version() -> None:
    previous = um.UPDATER_VERSION
    manifest = {
        "version": "2.0.0",
        "artifact_url": "file://local",
        "sha256": "1" * 64,
        "signature": "2" * 64,
        "min_updater_version": "1.0.10",
        "migration_plan": [],
        "rollback_allowed": False,
    }
    um.UPDATER_VERSION = "1.0.12"
    try:
        um.validate_manifest(manifest)
    finally:
        um.UPDATER_VERSION = previous
