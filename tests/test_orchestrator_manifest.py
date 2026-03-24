from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest import mock

from scripts import orchestrator
from tests.helpers.temp_paths import workspace_tmpdir


def _patch_orchestrator_paths(tmp_root: Path) -> None:
    orchestrator.ROOT = tmp_root
    orchestrator.SRC = tmp_root / "src"
    orchestrator.OUT = tmp_root / "_out"
    orchestrator.RUNS = orchestrator.OUT / "runs"
    orchestrator.LATEST = orchestrator.OUT / "latest"
    orchestrator.STATE = orchestrator.OUT / ".state"
    orchestrator.MANIFEST_PATH = orchestrator.OUT / "LATEST_MANIFEST.json"
    orchestrator.SRC.mkdir(parents=True, exist_ok=True)


def test_build_manifest_marks_test_lanes_not_run() -> None:
    with workspace_tmpdir("orchestrator-manifest-build") as tmp:
        tmp_root = Path(tmp)
        _patch_orchestrator_paths(tmp_root)
        args = argparse.Namespace(force=True)

        with (
            mock.patch.object(orchestrator, "get_src_hash", return_value="abc123"),
            mock.patch.object(orchestrator, "run_proc", return_value=(True, "ok")),
            mock.patch.object(orchestrator, "sync_latest"),
            mock.patch.object(orchestrator, "rotate_history"),
        ):
            ok, run_dir = orchestrator.build(args)

        assert ok is True
        assert run_dir is not None
        manifest = json.loads(orchestrator.MANIFEST_PATH.read_text(encoding="utf-8"))
        artifacts = manifest["artifacts"]
        assert artifacts["py_tests_status"] == "not_run"
        assert artifacts["node_tests_status"] == "not_run"
        assert artifacts["py_tests_ok"] is False
        assert artifacts["node_tests_ok"] is False


def test_verify_manifest_marks_passed_test_lanes() -> None:
    with workspace_tmpdir("orchestrator-manifest-verify-pass") as tmp:
        tmp_root = Path(tmp)
        _patch_orchestrator_paths(tmp_root)
        args = argparse.Namespace(force=True, full=False)
        run_dir = orchestrator.RUNS / "20260318_220500"
        exe_path = run_dir / "build" / "portable" / "Baluffo.exe"
        exe_path.parent.mkdir(parents=True, exist_ok=True)
        exe_path.write_text("stub", encoding="utf-8")

        with (
            mock.patch.object(orchestrator, "build", return_value=(True, run_dir)),
            mock.patch.object(
                orchestrator,
                "run_proc",
                side_effect=[(True, "py"), (True, "node"), (True, "smoke")],
            ),
            mock.patch.object(orchestrator, "sync_latest"),
            mock.patch.object(orchestrator, "rotate_history"),
            mock.patch.object(orchestrator, "get_src_hash", return_value="abc123"),
        ):
            orchestrator.verify(args)

        manifest = json.loads(orchestrator.MANIFEST_PATH.read_text(encoding="utf-8"))
        artifacts = manifest["artifacts"]
        assert artifacts["py_tests_status"] == "passed"
        assert artifacts["node_tests_status"] == "passed"
        assert artifacts["py_tests_ok"] is True
        assert artifacts["node_tests_ok"] is True


def test_verify_manifest_marks_failed_test_lanes() -> None:
    with workspace_tmpdir("orchestrator-manifest-verify-fail") as tmp:
        tmp_root = Path(tmp)
        _patch_orchestrator_paths(tmp_root)
        args = argparse.Namespace(force=True, full=False)
        run_dir = orchestrator.RUNS / "20260318_220600"
        exe_path = run_dir / "build" / "portable" / "Baluffo.exe"
        exe_path.parent.mkdir(parents=True, exist_ok=True)
        exe_path.write_text("stub", encoding="utf-8")

        with (
            mock.patch.object(orchestrator, "build", return_value=(True, run_dir)),
            mock.patch.object(
                orchestrator,
                "run_proc",
                side_effect=[(False, "py"), (True, "node"), (True, "smoke")],
            ),
            mock.patch.object(orchestrator, "sync_latest"),
            mock.patch.object(orchestrator, "rotate_history"),
            mock.patch.object(orchestrator, "get_src_hash", return_value="abc123"),
        ):
            orchestrator.verify(args)

        manifest = json.loads(orchestrator.MANIFEST_PATH.read_text(encoding="utf-8"))
        artifacts = manifest["artifacts"]
        assert artifacts["py_tests_status"] == "failed"
        assert artifacts["node_tests_status"] == "passed"
        assert artifacts["py_tests_ok"] is False
        assert artifacts["node_tests_ok"] is True
