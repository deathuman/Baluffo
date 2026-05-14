from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_startup_probe_pair.py"
SPEC = importlib.util.spec_from_file_location("run_startup_probe_pair", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
startup_probe_pair = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(startup_probe_pair)


def test_run_startup_probe_pair_reuses_cold_build_for_warm(tmp_path: Path, monkeypatch) -> None:
    pair_root = tmp_path / "packaged-desktop-smoke-pair"
    summary_path = tmp_path / "startup-summary.json"
    cold_report_paths: list[Path] = []
    commands: list[list[str]] = []
    fake_exe_path = tmp_path / "cold-build" / "Baluffo.exe"
    fake_exe_path.parent.mkdir(parents=True, exist_ok=True)
    fake_exe_path.write_bytes(b"MZ")

    monkeypatch.setattr(startup_probe_pair, "PAIR_ARTIFACT_ROOT", pair_root)
    monkeypatch.setattr(
        startup_probe_pair,
        "generate_pair_run_token",
        lambda now=None: "20260417-080000-123456",
    )

    def _fake_run(command: list[str], cwd: Path, check: bool) -> SimpleNamespace:
        assert cwd == startup_probe_pair.ROOT
        assert check is False
        commands.append(command)
        if len(commands) == 1:
            assert "--rebuild" in command
            cold_report_path = Path(command[command.index("--report-path") + 1])
            cold_report_path.parent.mkdir(parents=True, exist_ok=True)
            cold_report_path.write_text(
                json.dumps({"exePath": str(fake_exe_path)}), encoding="utf-8"
            )
            cold_report_paths.append(cold_report_path)
        else:
            assert "--rebuild" not in command
            assert "--exe-path" in command
            reused_exe = Path(command[command.index("--exe-path") + 1])
            assert reused_exe == fake_exe_path
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(startup_probe_pair.subprocess, "run", _fake_run)

    exit_code = startup_probe_pair.run_startup_probe_pair(
        runtime_timeout_s=45.0,
        artifact_root=pair_root,
        summary_path=summary_path,
    )

    assert exit_code == 0
    assert len(commands) == 2
    first_command = commands[0]
    second_command = commands[1]
    assert first_command[0] == startup_probe_pair.sys.executable
    assert first_command[1] == str(startup_probe_pair.PACKAGED_SMOKE_SCRIPT)
    assert first_command[first_command.index("--profile-mode") + 1] == "cold"
    assert second_command[second_command.index("--profile-mode") + 1] == "warm"
    assert first_command[first_command.index("--open-path") + 1] == "jobs.html"
    assert second_command[second_command.index("--open-path") + 1] == "jobs.html"
    assert (
        Path(first_command[first_command.index("--artifacts-dir") + 1])
        == pair_root / ("20260417-080000-123456") / "cold"
    )
    assert (
        Path(second_command[second_command.index("--artifacts-dir") + 1])
        == pair_root / ("20260417-080000-123456") / "warm"
    )
    assert cold_report_paths == [pair_root / "20260417-080000-123456" / "cold-report.json"]
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["ok"] is True
    assert summary["coldReportPath"] == str(
        pair_root / "20260417-080000-123456" / "cold-report.json"
    )
    assert summary["warmReportPath"] == str(
        pair_root / "20260417-080000-123456" / "warm-report.json"
    )


def test_run_startup_probe_pair_passes_admin_open_path_to_cold_and_warm(
    tmp_path: Path, monkeypatch
) -> None:
    pair_root = tmp_path / "packaged-desktop-smoke-pair"
    commands: list[list[str]] = []
    fake_exe_path = tmp_path / "cold-build" / "Baluffo.exe"
    fake_exe_path.parent.mkdir(parents=True, exist_ok=True)
    fake_exe_path.write_bytes(b"MZ")

    monkeypatch.setattr(
        startup_probe_pair,
        "generate_pair_run_token",
        lambda now=None: "20260417-090000-123456",
    )

    def _fake_run(command: list[str], cwd: Path, check: bool) -> SimpleNamespace:
        assert cwd == startup_probe_pair.ROOT
        assert check is False
        commands.append(command)
        if len(commands) == 1:
            cold_report_path = Path(command[command.index("--report-path") + 1])
            cold_report_path.parent.mkdir(parents=True, exist_ok=True)
            cold_report_path.write_text(
                json.dumps({"exePath": str(fake_exe_path)}), encoding="utf-8"
            )
        else:
            assert Path(command[command.index("--exe-path") + 1]) == fake_exe_path
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(startup_probe_pair.subprocess, "run", _fake_run)

    exit_code = startup_probe_pair.run_startup_probe_pair(
        runtime_timeout_s=45.0,
        artifact_root=pair_root,
        open_path="admin.html",
        profile_record_only=True,
    )

    assert exit_code == 0
    assert len(commands) == 2
    assert commands[0][commands[0].index("--open-path") + 1] == "admin.html"
    assert commands[1][commands[1].index("--open-path") + 1] == "admin.html"
    assert "--profile-record-only" in commands[0]
    assert "--profile-record-only" in commands[1]
    assert (
        Path(commands[0][commands[0].index("--artifacts-dir") + 1])
        == pair_root / "20260417-090000-123456" / "admin-cold"
    )
    assert (
        Path(commands[1][commands[1].index("--artifacts-dir") + 1])
        == pair_root / "20260417-090000-123456" / "admin-warm"
    )
    assert (
        Path(commands[0][commands[0].index("--report-path") + 1])
        == pair_root / "20260417-090000-123456" / "admin-cold-report.json"
    )


def test_startup_probe_pair_parse_args_accepts_artifact_root_and_summary_path(
    tmp_path: Path,
) -> None:
    args = startup_probe_pair.parse_args(
        [
            "--runtime-timeout",
            "12",
            "--artifact-root",
            str(tmp_path / "runs"),
            "--summary-path",
            str(tmp_path / "summary.json"),
            "--open-path",
            "admin.html",
            "--profile-record-only",
        ]
    )

    assert args.runtime_timeout == 12
    assert args.artifact_root == str(tmp_path / "runs")
    assert args.summary_path == str(tmp_path / "summary.json")
    assert args.open_path == "admin.html"
    assert args.profile_record_only is True
