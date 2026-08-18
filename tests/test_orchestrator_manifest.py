from __future__ import annotations

import argparse
import json
import locale
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


def test_build_forwards_force_to_portable_builder() -> None:
    with workspace_tmpdir("orchestrator-force-portable-build") as tmp:
        tmp_root = Path(tmp)
        _patch_orchestrator_paths(tmp_root)
        args = argparse.Namespace(force=True)

        with (
            mock.patch.object(orchestrator, "get_src_hash", return_value="abc123"),
            mock.patch.object(orchestrator, "run_proc", return_value=(True, "ok")) as run_proc_mock,
            mock.patch.object(orchestrator, "sync_latest"),
            mock.patch.object(orchestrator, "rotate_history"),
        ):
            ok, _run_dir = orchestrator.build(args)

    assert ok is True
    portable_command = run_proc_mock.call_args_list[1].args[0]
    assert portable_command[:2] == [orchestrator.sys.executable, "scripts/build_portable_exe.py"]
    assert "--force" in portable_command


def test_build_omits_portable_force_without_force_flag() -> None:
    with workspace_tmpdir("orchestrator-default-portable-build") as tmp:
        tmp_root = Path(tmp)
        _patch_orchestrator_paths(tmp_root)
        args = argparse.Namespace(force=False)

        with (
            mock.patch.object(orchestrator, "get_src_hash", return_value="abc123"),
            mock.patch.object(orchestrator, "run_proc", return_value=(True, "ok")) as run_proc_mock,
            mock.patch.object(orchestrator, "sync_latest"),
            mock.patch.object(orchestrator, "rotate_history"),
        ):
            ok, _run_dir = orchestrator.build(args)

    assert ok is True
    portable_command = run_proc_mock.call_args_list[1].args[0]
    assert portable_command[:2] == [orchestrator.sys.executable, "scripts/build_portable_exe.py"]
    assert "--force" not in portable_command


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
            mock.patch.object(orchestrator, "run_proc") as run_proc_mock,
            mock.patch.object(orchestrator, "sync_latest"),
            mock.patch.object(orchestrator, "rotate_history"),
            mock.patch.object(orchestrator, "get_src_hash", return_value="abc123"),
        ):
            run_proc_mock.side_effect = [
                (True, "precommit"),
                (True, "py"),
                (True, "node"),
                (True, "smoke"),
            ]
            orchestrator.verify(args)

        assert run_proc_mock.call_args_list[0].args[0] == ["npm", "run", "lint:precommit"]
        manifest = json.loads(orchestrator.MANIFEST_PATH.read_text(encoding="utf-8"))
        artifacts = manifest["artifacts"]
        assert artifacts["py_tests_status"] == "passed"
        assert artifacts["node_tests_status"] == "passed"
        assert artifacts["precommit_status"] == "passed"
        assert artifacts["precommit_ok"] is True
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
            mock.patch.object(orchestrator, "run_proc") as run_proc_mock,
            mock.patch.object(orchestrator, "sync_latest"),
            mock.patch.object(orchestrator, "rotate_history"),
            mock.patch.object(orchestrator, "get_src_hash", return_value="abc123"),
        ):
            run_proc_mock.side_effect = [
                (True, "precommit"),
                (False, "py"),
                (True, "node"),
                (True, "smoke"),
            ]
            orchestrator.verify(args)

        assert run_proc_mock.call_args_list[0].args[0] == ["npm", "run", "lint:precommit"]
        manifest = json.loads(orchestrator.MANIFEST_PATH.read_text(encoding="utf-8"))
        artifacts = manifest["artifacts"]
        assert artifacts["py_tests_status"] == "failed"
        assert artifacts["node_tests_status"] == "passed"
        assert artifacts["precommit_status"] == "passed"
        assert artifacts["precommit_ok"] is True
        assert artifacts["py_tests_ok"] is False
        assert artifacts["node_tests_ok"] is True


def test_run_proc_uses_replace_decode_for_streamed_output() -> None:
    class _FakeStdout:
        def __init__(self) -> None:
            self._chunks = iter(["o", "k", ""])
            self.finished = False

        def read(self, _size: int) -> str:
            chunk = next(self._chunks, "")
            if chunk == "":
                self.finished = True
            return chunk

    class _FakeProcess:
        def __init__(self) -> None:
            self.stdout = _FakeStdout()
            self.returncode = 0

        def poll(self) -> int | None:
            return 0 if self.stdout.finished else None

        def wait(self) -> int:
            return self.returncode

    with mock.patch.object(
        orchestrator.subprocess, "Popen", return_value=_FakeProcess()
    ) as popen_mock:
        ok, output = orchestrator.run_proc(["npm", "run", "lint:precommit"], "PreCommit")

    assert ok is True
    assert output == "ok"
    _, kwargs = popen_mock.call_args
    assert kwargs["text"] is True
    assert kwargs["errors"] == "replace"
    assert kwargs["encoding"] == (locale.getpreferredencoding(False) or "utf-8")


def _char_streaming_process(text: str) -> _CharStreamingProcess:
    return _CharStreamingProcess(text)


class _CharStreamingProcess:
    """Fake subprocess returning one character per read, like run_proc consumes."""

    def __init__(self, text: str) -> None:
        self.stdout = _CharStreamingStdout(text)
        self.returncode = 0

    def poll(self) -> int | None:
        return 0 if self.stdout.finished else None

    def wait(self) -> int:
        return self.returncode


class _CharStreamingStdout:
    def __init__(self, text: str) -> None:
        self._chars = iter(text)
        self.finished = False

    def read(self, _size: int) -> str:
        try:
            return next(self._chars)
        except StopIteration:
            self.finished = True
            return ""


def test_run_proc_streams_only_progress_line_chars(capsys) -> None:
    """Letters in prose (warnings, tracebacks, summaries) are not echoed."""
    stream = (
        "...... [ 92%]\n"
        '  File "C:\\python\\proactor_events.py", line 116, in __del__\n'
        '      _warn(f"unclosed transport {self!r}", ResourceWarning, source=self)\n'
        '  File "C:\\python\\base_subprocess.py", line 135, in __del__\n'
        '      _warn(f"unclosed transport {self!r}", ResourceWarning, source=self)\n'
        "ValueError: I/O operation on closed pipe\n"
        "4342 passed, 2 skipped, 6 warnings in 323.83s (0:05:23)\n"
    )
    with mock.patch.object(
        orchestrator.subprocess, "Popen", return_value=_char_streaming_process(stream)
    ):
        ok, output = orchestrator.run_proc(
            ["npm", "run", "test:py:extended"], "PyTests", allow_stream=True
        )

    assert ok is True
    assert output == stream
    out = capsys.readouterr().out
    assert "Running: npm run test:py:extended\n......\nOK" in out
    assert not any(c in out for c in "FE!")


def test_run_proc_streams_rewritten_progress_after_carriage_return(capsys) -> None:
    """A \r rewrite restarts the progress run so the new dots still stream."""
    stream = "....\r......\n"
    with mock.patch.object(
        orchestrator.subprocess, "Popen", return_value=_char_streaming_process(stream)
    ):
        ok, _output = orchestrator.run_proc(
            ["npm", "run", "test:py:extended"], "PyTests", allow_stream=True
        )

    assert ok is True
    out = capsys.readouterr().out
    assert "Running: npm run test:py:extended\n..........\nOK" in out


def test_run_proc_does_not_stream_when_allow_stream_disabled(capsys) -> None:
    stream = "....\n"
    with mock.patch.object(
        orchestrator.subprocess, "Popen", return_value=_char_streaming_process(stream)
    ):
        ok, _output = orchestrator.run_proc(["npm", "run", "test:py:extended"], "PyTests")

    assert ok is True
    assert "." not in capsys.readouterr().out
