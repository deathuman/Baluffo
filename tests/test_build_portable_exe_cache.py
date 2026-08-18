import json
import os
from pathlib import Path
from unittest import mock

import pytest

from scripts.build_portable_exe import (
    DEFAULT_ICON_PATH,
    PORTABLE_BUILD_CACHE_RETAIN_ENV,
    _resolve_cache_retain_entries,
    _store_cache_entry,
    build_or_reuse_portable,
    parse_args,
    portable_build_fingerprint,
    read_portable_build_provenance,
)
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = pytest.mark.packaging


def _write_minimal_portable_tree(portable_root: Path) -> None:
    portable_root.mkdir(parents=True, exist_ok=True)
    (portable_root / "Baluffo.exe").write_text("exe", encoding="utf-8")
    (portable_root / "BaluffoUpdater.exe").write_text("helper", encoding="utf-8")
    (portable_root / "ship").mkdir(exist_ok=True)


def _write_cache_entry(cache_root: Path, fingerprint: str, *, mtime: int) -> Path:
    entry = cache_root / fingerprint
    _write_minimal_portable_tree(entry / "portable")
    (entry / "manifest.json").write_text(
        json.dumps({"fingerprint": fingerprint, "inputs": {}}),
        encoding="utf-8",
    )
    os.utime(entry, (mtime, mtime))
    os.utime(entry / "manifest.json", (mtime, mtime))
    return entry


def test_parse_args_accepts_force_cache_bypass() -> None:
    with mock.patch("sys.argv", ["build_portable_exe.py", "--force"]):
        args = parse_args()
    assert args.force is True


def test_parse_args_accepts_cache_retention_limit() -> None:
    with mock.patch("sys.argv", ["build_portable_exe.py", "--cache-retain", "2"]):
        args = parse_args()
    assert args.cache_retain == 2


def test_cache_retention_uses_env_override() -> None:
    assert _resolve_cache_retain_entries(None, {PORTABLE_BUILD_CACHE_RETAIN_ENV: "3"}) == 3
    assert _resolve_cache_retain_entries(1, {PORTABLE_BUILD_CACHE_RETAIN_ENV: "3"}) == 1


def test_build_or_reuse_portable_materializes_cache_hit_without_rebuilding() -> None:
    with workspace_tmpdir("build-portable-cache") as tmp:
        root = Path(tmp)
        cache_root = root / "cache"
        output_dir = root / "dist" / "baluffo-portable"
        cache_portable = cache_root / "abc123" / "portable"
        _write_minimal_portable_tree(cache_portable)
        (cache_root / "abc123" / "manifest.json").write_text(
            json.dumps({"fingerprint": "abc123", "inputs": {}}),
            encoding="utf-8",
        )

        with (
            mock.patch(
                "scripts.build_portable_exe.portable_build_fingerprint",
                return_value={"fingerprint": "abc123", "inputs": {}},
            ),
            mock.patch("scripts.build_portable_exe.validate_playwright_browser_payload"),
            mock.patch(
                "scripts.build_portable_exe.validate_frozen_desktop_platform_modules"
            ) as validator_mock,
            mock.patch("scripts.build_portable_exe.build_portable_layout") as layout_mock,
            mock.patch("scripts.build_portable_exe.run_pyinstaller") as pyinstaller_mock,
            mock.patch("scripts.build_portable_exe.run_helper_pyinstaller") as helper_mock,
        ):
            exe_path, helper_path, provenance = build_or_reuse_portable(
                output_dir=output_dir,
                version="1.2.3",
                exe_name="Baluffo",
                icon_path=DEFAULT_ICON_PATH,
                cache_root=cache_root,
                cache_retain_entries=10,
            )

        assert exe_path == output_dir / "Baluffo.exe"
        assert helper_path == output_dir / "BaluffoUpdater.exe"
        assert (output_dir / "ship").is_dir()
        assert provenance["cacheStatus"] == "hit"
        assert read_portable_build_provenance(output_dir)["fingerprint"] == "abc123"
        validator_mock.assert_called_once_with(output_dir / "Baluffo.exe")
        layout_mock.assert_not_called()
        pyinstaller_mock.assert_not_called()
        helper_mock.assert_not_called()


def test_build_or_reuse_portable_stores_cache_on_miss() -> None:
    with workspace_tmpdir("build-portable-cache") as tmp:
        root = Path(tmp)
        cache_root = root / "cache"
        output_dir = root / "dist" / "baluffo-portable"

        def fake_build_layout(target_dir: Path, version: str) -> Path:
            assert version == "1.2.3"
            _write_minimal_portable_tree(target_dir)
            return target_dir

        with (
            mock.patch(
                "scripts.build_portable_exe.portable_build_fingerprint",
                return_value={"fingerprint": "miss123", "inputs": {"fixture": True}},
            ),
            mock.patch("scripts.build_portable_exe.validate_playwright_browser_payload"),
            mock.patch("scripts.build_portable_exe.validate_frozen_desktop_platform_modules"),
            mock.patch(
                "scripts.build_portable_exe.build_portable_layout",
                side_effect=fake_build_layout,
            ) as layout_mock,
            mock.patch(
                "scripts.build_portable_exe.run_pyinstaller",
                side_effect=lambda portable_root, **_kwargs: portable_root / "Baluffo.exe",
            ) as pyinstaller_mock,
            mock.patch(
                "scripts.build_portable_exe.run_helper_pyinstaller",
                side_effect=lambda portable_root, **_kwargs: portable_root / "BaluffoUpdater.exe",
            ) as helper_mock,
        ):
            _exe_path, _helper_path, provenance = build_or_reuse_portable(
                output_dir=output_dir,
                version="1.2.3",
                exe_name="Baluffo",
                icon_path=DEFAULT_ICON_PATH,
                cache_root=cache_root,
                cache_retain_entries=10,
            )

        assert provenance["cacheStatus"] == "miss"
        assert (cache_root / "miss123" / "portable" / "Baluffo.exe").is_file()
        manifest = json.loads((cache_root / "miss123" / "manifest.json").read_text())
        assert manifest["fingerprint"] == "miss123"
        assert manifest["inputs"] == {"fixture": True}
        layout_mock.assert_called_once_with(output_dir, "1.2.3")
        pyinstaller_mock.assert_called_once()
        helper_mock.assert_called_once()


def test_build_or_reuse_portable_prunes_old_cache_entries_on_miss() -> None:
    with workspace_tmpdir("build-portable-cache") as tmp:
        root = Path(tmp)
        cache_root = root / "cache"
        output_dir = root / "dist" / "baluffo-portable"
        oldest = "a" * 64
        newest = "b" * 64
        _write_cache_entry(cache_root, oldest, mtime=100)
        _write_cache_entry(cache_root, newest, mtime=200)

        def fake_build_layout(target_dir: Path, _version: str) -> Path:
            _write_minimal_portable_tree(target_dir)
            return target_dir

        with (
            mock.patch(
                "scripts.build_portable_exe.portable_build_fingerprint",
                return_value={"fingerprint": "c" * 64, "inputs": {"fixture": True}},
            ),
            mock.patch("scripts.build_portable_exe.validate_playwright_browser_payload"),
            mock.patch("scripts.build_portable_exe.validate_frozen_desktop_platform_modules"),
            mock.patch(
                "scripts.build_portable_exe.build_portable_layout", side_effect=fake_build_layout
            ),
            mock.patch(
                "scripts.build_portable_exe.run_pyinstaller",
                side_effect=lambda portable_root, **_kwargs: portable_root / "Baluffo.exe",
            ),
            mock.patch(
                "scripts.build_portable_exe.run_helper_pyinstaller",
                side_effect=lambda portable_root, **_kwargs: portable_root / "BaluffoUpdater.exe",
            ),
        ):
            build_or_reuse_portable(
                output_dir=output_dir,
                version="1.2.3",
                exe_name="Baluffo",
                icon_path=DEFAULT_ICON_PATH,
                cache_root=cache_root,
                cache_retain_entries=2,
            )

        assert not (cache_root / oldest).exists()
        assert (cache_root / newest).is_dir()
        assert (cache_root / ("c" * 64)).is_dir()


def test_build_or_reuse_portable_prunes_old_cache_entries_on_hit() -> None:
    with workspace_tmpdir("build-portable-cache") as tmp:
        root = Path(tmp)
        cache_root = root / "cache"
        output_dir = root / "dist" / "baluffo-portable"
        hit = "d" * 64
        old = "e" * 64
        _write_cache_entry(cache_root, hit, mtime=100)
        _write_cache_entry(cache_root, old, mtime=200)

        with (
            mock.patch(
                "scripts.build_portable_exe.portable_build_fingerprint",
                return_value={"fingerprint": hit, "inputs": {}},
            ),
            mock.patch("scripts.build_portable_exe.validate_playwright_browser_payload"),
            mock.patch("scripts.build_portable_exe.validate_frozen_desktop_platform_modules"),
        ):
            build_or_reuse_portable(
                output_dir=output_dir,
                version="1.2.3",
                exe_name="Baluffo",
                icon_path=DEFAULT_ICON_PATH,
                cache_root=cache_root,
                cache_retain_entries=1,
            )

        assert (cache_root / hit).is_dir()
        assert not (cache_root / old).exists()


def test_store_cache_entry_falls_back_when_windows_directory_replace_is_locked() -> None:
    with workspace_tmpdir("build-portable-cache") as tmp:
        root = Path(tmp)
        output_dir = root / "dist" / "baluffo-portable"
        cache_root = root / "cache"
        _write_minimal_portable_tree(output_dir)
        original_replace = Path.replace

        def fake_replace(source: Path, target: Path) -> Path:
            if source.name.startswith("locked123.tmp-") and Path(target).name == "locked123":
                raise PermissionError("locked")
            return original_replace(source, target)

        with (
            mock.patch("scripts.build_portable_exe.validate_playwright_browser_payload"),
            mock.patch("scripts.build_portable_exe.validate_frozen_desktop_platform_modules"),
            mock.patch(
                "scripts.build_portable_exe.Path.replace",
                autospec=True,
                side_effect=fake_replace,
            ) as replace_mock,
        ):
            entry = _store_cache_entry(
                output_dir,
                fingerprint_payload={"fingerprint": "locked123", "inputs": {"locked": True}},
                cache_root=cache_root,
            )

        assert (entry / "portable" / "Baluffo.exe").is_file()
        assert json.loads((entry / "manifest.json").read_text())["fingerprint"] == "locked123"
        assert not list(cache_root.glob("locked123.tmp-*"))
        assert any(Path(call.args[1]).name == "locked123" for call in replace_mock.call_args_list)


def test_portable_build_fingerprint_changes_when_icon_content_changes() -> None:
    with workspace_tmpdir("build-portable-cache") as tmp:
        icon = Path(tmp) / "app.ico"
        icon.write_bytes(b"old-icon")
        with (
            mock.patch("scripts.build_portable_exe._file_digests", return_value=[]),
            mock.patch("scripts.build_portable_exe._env_digests", return_value={}),
            mock.patch("scripts.build_portable_exe._safe_package_version", return_value=""),
            mock.patch(
                "scripts.build_portable_exe._installed_playwright_browser_revision",
                return_value="",
            ),
            mock.patch("scripts.build_portable_exe._git_remote_digest", return_value=""),
        ):
            first = portable_build_fingerprint(version="1.2.3", icon_path=icon)["fingerprint"]
            icon.write_bytes(b"new-icon")
            second = portable_build_fingerprint(version="1.2.3", icon_path=icon)["fingerprint"]

    assert first != second
