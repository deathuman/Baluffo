"""Regression tests: the packaged desktop EXE must bundle both platform modules.

The desktop_app facade selects its platform module at runtime. If that selection
is ever rewritten as a dynamic importlib call, PyInstaller's static module graph
cannot see the inactive platform module and the frozen EXE crashes at startup
with ModuleNotFoundError (fixed in commit c1d3a2ff after `npm run verify`
caught the gap in the packaged smoke test).

The portable build now validates the frozen PYZ as part of
`build_or_reuse_portable` so `npm run verify` aborts at the PortableEXE stage.
These tests pin the same contract from three angles:

  * the PyInstaller module graph for the real desktop entrypoint (hermetic,
    always runs),
  * the frozen PYZ archive inside an actually built portable EXE (ground truth;
    skips when no build artifact exists, e.g. a plain `npm run test:py:extended`
    without a preceding portable build), and
  * the build-time validator's failure behavior (missing modules / missing PYZ).
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


def test_frozen_portable_exe_pyz_contains_both_desktop_platform_modules() -> None:
    """An actually built portable EXE passes the build-time frozen-bundle check.

    Ground-truth check on the frozen artifact via the shared validator used by
    `build_or_reuse_portable`; PyInstaller 6.x (pinned in requirements.txt)
    embeds the pure-Python module archive inside the onedir bootloader
    executable. Skips when no built EXE exists.
    """
    from scripts.build_portable_exe import validate_frozen_desktop_platform_modules

    exe_path = _newest_built_portable_exe()
    if exe_path is None:
        pytest.skip(
            "no built portable EXE found; run the portable build (e.g. "
            "`npm run verify` or scripts/build_portable_exe.py) first"
        )

    validate_frozen_desktop_platform_modules(exe_path)


def test_validate_frozen_desktop_platform_modules_reports_missing_modules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The build-time validator fails loudly when a platform module is absent."""
    from scripts.build_portable_exe import validate_frozen_desktop_platform_modules

    monkeypatch.setattr(
        "scripts.build_portable_exe.read_frozen_pyz_modules",
        lambda exe_path: {"src.ship.desktop_app"},
    )
    with pytest.raises(RuntimeError, match="src.ship.desktop_app._windows"):
        validate_frozen_desktop_platform_modules(Path("fake.exe"))


def test_read_frozen_pyz_modules_rejects_archive_without_pyz_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A built EXE whose archive layout lost the PYZ is reported, not skipped."""
    from scripts.build_portable_exe import read_frozen_pyz_modules

    class _FakeArchiveWithoutPyz:
        toc: dict[str, object] = {}

    monkeypatch.setattr(
        "PyInstaller.archive.readers.CArchiveReader",
        lambda *args, **kwargs: _FakeArchiveWithoutPyz(),
    )
    with pytest.raises(RuntimeError, match="PYZ.pyz"):
        read_frozen_pyz_modules(Path("fake.exe"))
