"""Regression tests: the packaged desktop EXE must bundle both platform modules.

The desktop_app facade selects its platform module at runtime. If that selection
is ever rewritten as a dynamic importlib call, PyInstaller's static module graph
cannot see the inactive platform module and the frozen EXE crashes at startup
with ModuleNotFoundError (fixed in commit c1d3a2ff after `npm run verify`
caught the gap in the packaged smoke test).

These tests pin the two collection surfaces that guard that gap:

  * the PyInstaller module graph for the real desktop entrypoint (hermetic,
    always runs), and
  * the frozen PYZ archive inside an actually built portable EXE (ground truth;
    skips when no build artifact exists, e.g. a plain `npm run test:py:extended`
    without a preceding portable build).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.helpers.temp_paths import workspace_tmpdir

pytest.importorskip("PyInstaller")

ROOT = Path(__file__).resolve().parents[1]
DESKTOP_APP_ENTRYPOINT = ROOT / "src" / "ship" / "desktop_app" / "__main__.py"
PLATFORM_MODULES = (
    "src.ship.desktop_app._windows",
    "src.ship.desktop_app._linux",
)

pytestmark = pytest.mark.packaging


def test_pyinstaller_analysis_collects_both_desktop_platform_modules() -> None:
    """The real entrypoint's PyInstaller module graph includes both platform modules.

    This is the hermetic guard: it reproduces the module-collection step of the
    portable build for `src/ship/desktop_app/__main__.py` and fails the moment
    the facade stops statically importing either platform module (e.g. reverts
    to an importlib dispatch, which PyInstaller cannot follow).
    """
    from PyInstaller.building.build_main import Analysis
    from PyInstaller.config import CONF

    with workspace_tmpdir("portable-desktop-modules-analysis") as tmp:
        work = Path(tmp) / "work"
        dist = Path(tmp) / "dist"
        cache = Path(tmp) / "cache"
        work.mkdir()
        dist.mkdir()
        cache.mkdir()

        saved_conf = dict(CONF)
        try:
            CONF.update(
                {
                    "spec": str(Path(tmp) / "baluffo.spec"),
                    "specpath": str(tmp),
                    "specnm": "baluffo",
                    "distpath": str(dist),
                    "workpath": str(work),
                    "hiddenimports": [],
                    "pathex": [str(ROOT)],
                    "main_script": str(DESKTOP_APP_ENTRYPOINT),
                    "cachedir": str(cache),
                    "code_cache": dict(),
                    "warnfile": str(Path(tmp) / "warn-baluffo.txt"),
                    "dot-file": str(Path(tmp) / "graph-baluffo.dot"),
                    "xref-file": str(Path(tmp) / "xref-baluffo.html"),
                }
            )
            analysis = Analysis(
                [str(DESKTOP_APP_ENTRYPOINT)],
                pathex=[str(ROOT)],
            )
            collected = {entry[0] for entry in analysis.pure}
        finally:
            CONF.clear()
            CONF.update(saved_conf)

        missing = [name for name in PLATFORM_MODULES if name not in collected]
        assert not missing, (
            "PyInstaller module graph for the desktop entrypoint is missing platform "
            f"modules: {missing}. The desktop_app facade must statically import both "
            "_windows and _linux so the frozen bundle contains them."
        )


def _newest_built_portable_exe() -> Path | None:
    candidates: list[Path] = []
    for direct in (
        ROOT / "dist" / "baluffo-portable" / "Baluffo.exe",
        ROOT / "_out" / "latest" / "build" / "portable" / "Baluffo.exe",
    ):
        if direct.is_file():
            candidates.append(direct)
    for root_dir, pattern in (
        (ROOT / "_out" / "runs", "*/build/portable/Baluffo.exe"),
        (ROOT / "_out" / "portable-build-cache", "*/portable/Baluffo.exe"),
    ):
        if root_dir.is_dir():
            candidates.extend(path for path in root_dir.glob(pattern) if path.is_file())
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def test_frozen_portable_exe_pyz_contains_both_desktop_platform_modules(
    tmp_path: Path,
) -> None:
    """An actually built portable EXE embeds both platform modules in its PYZ.

    Ground-truth check on the frozen artifact: PyInstaller 6.x (pinned in
    requirements.txt) embeds the pure-Python module archive inside the onedir
    bootloader executable. Skips when no built EXE exists.
    """
    from PyInstaller.archive.readers import CArchiveReader, ZlibArchiveReader

    exe_path = _newest_built_portable_exe()
    if exe_path is None:
        pytest.skip(
            "no built portable EXE found; run the portable build (e.g. "
            "`npm run verify` or scripts/build_portable_exe.py) first"
        )

    archive = CArchiveReader(str(exe_path))
    if "PYZ.pyz" not in archive.toc:
        pytest.fail(
            f"bundled executable {exe_path} has no PYZ.pyz entry; "
            "PyInstaller archive layout may have changed"
        )
    pyz_path = tmp_path / "Baluffo.pyz"
    pyz_path.write_bytes(archive.extract("PYZ.pyz"))
    frozen_modules = set(ZlibArchiveReader(str(pyz_path)).toc)

    missing = [name for name in PLATFORM_MODULES if name not in frozen_modules]
    assert not missing, (
        f"frozen EXE bundle {exe_path} is missing desktop platform modules: {missing}"
    )
