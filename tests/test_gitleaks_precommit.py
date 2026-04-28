from __future__ import annotations

from unittest import mock

from scripts import gitleaks_precommit


def test_main_scans_existing_files_with_repo_relative_paths(tmp_path, monkeypatch) -> None:
    scanned_file = tmp_path / "nested" / "candidate.txt"
    scanned_file.parent.mkdir()
    scanned_file.write_text('api_key = "fake"', encoding="utf-8")
    config = tmp_path / ".gitleaks.toml"
    config.write_text('title = "test"\n', encoding="utf-8")
    calls: list[list[str]] = []

    def fake_run(command: list[str], **kwargs) -> mock.Mock:
        calls.append(command)
        assert kwargs["cwd"] == tmp_path
        return mock.Mock(returncode=0)

    monkeypatch.setattr(gitleaks_precommit, "ROOT", tmp_path)
    monkeypatch.setattr(gitleaks_precommit, "CONFIG", config)
    monkeypatch.setattr(gitleaks_precommit.subprocess, "run", fake_run)

    assert gitleaks_precommit.main(["nested/candidate.txt"]) == 0
    assert calls == [
        [
            "gitleaks",
            "dir",
            "--config",
            str(config),
            "--redact",
            "--no-banner",
            "--verbose",
            "nested/candidate.txt",
        ]
    ]


def test_main_fails_when_any_scan_fails(tmp_path, monkeypatch) -> None:
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("ok", encoding="utf-8")
    second.write_text("leak", encoding="utf-8")
    results = iter([0, 1])

    def fake_run(command: list[str], **kwargs) -> mock.Mock:
        return mock.Mock(returncode=next(results))

    monkeypatch.setattr(gitleaks_precommit, "ROOT", tmp_path)
    monkeypatch.setattr(gitleaks_precommit, "CONFIG", tmp_path / ".gitleaks.toml")
    monkeypatch.setattr(gitleaks_precommit.subprocess, "run", fake_run)

    assert gitleaks_precommit.main(["first.txt", "second.txt"]) == 1
