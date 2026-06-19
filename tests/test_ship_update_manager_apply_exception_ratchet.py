import json
from pathlib import Path
from unittest import mock
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from src.ship.update_manager_apply import apply_update
from src.ship.update_manager_state import iso_now
from src.ship.update_manager_validation import compute_sha256, sign_manifest
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
                "updated_at": iso_now(),
            }
        ),
    )
    (root / "app" / "versions" / version / "src").mkdir(parents=True, exist_ok=True)
    _write(root / "app" / "versions" / version / "src" / "admin_bridge.py", "print('ok')\n")
    for name in ("index.html", "jobs.html", "saved.html", "admin.html"):
        _write(root / "app" / "versions" / version / name, "<html></html>\n")
    (root / "app" / "staging").mkdir(parents=True, exist_ok=True)
    (root / "data" / "backups").mkdir(parents=True, exist_ok=True)
    (root / "data" / "migration-reports").mkdir(parents=True, exist_ok=True)
    _write(root / "data" / "user-settings.json", '{"theme":"dark"}\n')


def _build_update_zip(work: Path, version: str) -> Path:
    source = work / "payload" / "app" / "versions" / version
    _write(source / "src" / "admin_bridge.py", "print('updated')\n")
    for name in ("index.html", "jobs.html", "saved.html", "admin.html"):
        _write(source / name, "<html>new</html>\n")
    bundle = work / f"baluffo-{version}.zip"
    with ZipFile(bundle, "w", compression=ZIP_DEFLATED) as archive:
        for path in source.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(work / "payload").as_posix())
    return bundle


def test_apply_update_interrupt_rolls_back_partial_install_before_propagating() -> None:
    with workspace_tmpdir("ship-update") as tmp:
        root = Path(tmp) / "ship"
        _seed_root(root, version="1.0.0")
        bundle = _build_update_zip(Path(tmp), "1.1.0")
        sha256 = compute_sha256(bundle)
        key = "test-key"
        manifest = {
            "version": "1.1.0",
            "artifact_url": "file://local",
            "sha256": sha256,
            "signature": sign_manifest("1.1.0", sha256, key),
            "min_updater_version": "1.0.0",
            "migration_plan": ["noop"],
            "rollback_allowed": False,
        }
        manifest_path = Path(tmp) / "update-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        with (
            mock.patch(
                "src.ship.update_manager_apply.health_check_version",
                side_effect=KeyboardInterrupt,
            ),
            pytest.raises(KeyboardInterrupt),
        ):
            apply_update(root, bundle, manifest_path, key)

        assert (root / "app" / "current.txt").read_text(encoding="utf-8").strip() == "1.0.0"
        assert not (root / "app" / "versions" / "1.1.0").exists()
        state = json.loads((root / "app" / "update-state.json").read_text(encoding="utf-8"))
        assert state["current_version"] == "1.0.0"
        assert state["last_update_status"] == "failed"
