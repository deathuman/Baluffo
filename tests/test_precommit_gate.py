from __future__ import annotations

from scripts import precommit_gate


def test_collect_changed_files_includes_changed_and_untracked_files(tmp_path, monkeypatch) -> None:
    staged = ["docs/changed.md", "src/changed.py", "src/changed.py"]
    unstaged = ["src/changed.py", "tests/changed_test.py"]
    untracked = ["new_file.txt", "docs/missing.md"]

    def fake_git_lines(*args: str) -> list[str]:
        query = tuple(args)
        if query == ("diff", "--cached", "--name-only", "--diff-filter=ACMRTUXB"):
            return staged
        if query == ("diff", "--name-only", "--diff-filter=ACMRTUXB"):
            return unstaged
        if query == ("ls-files", "--others", "--exclude-standard"):
            return untracked
        raise AssertionError(f"Unexpected git query: {query}")

    monkeypatch.setattr(precommit_gate, "ROOT", tmp_path)
    monkeypatch.setattr(precommit_gate, "_git_lines", fake_git_lines)

    for rel_path in ("docs/changed.md", "src/changed.py", "tests/changed_test.py", "new_file.txt"):
        path = tmp_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    collected = precommit_gate.collect_changed_files()

    assert collected == [
        "docs/changed.md",
        "src/changed.py",
        "tests/changed_test.py",
        "new_file.txt",
    ]


def test_run_all_executes_precommit_and_prepush_commands(monkeypatch) -> None:
    commands: list[list[str]] = []

    def fake_run(command: list[str]) -> int:
        commands.append(command)
        return 0

    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_run)

    assert precommit_gate.run_all() == 0
    assert commands == [
        [
            precommit_gate.PYTHON,
            "-m",
            "pre_commit",
            "run",
            "--show-diff-on-failure",
            "--color=always",
            "--all-files",
        ],
        [
            precommit_gate.PYTHON,
            "-m",
            "pre_commit",
            "run",
            "--show-diff-on-failure",
            "--color=always",
            "--hook-stage",
            "pre-push",
            "--all-files",
        ],
    ]


def test_run_changed_skips_when_no_files(monkeypatch, capsys) -> None:
    monkeypatch.setattr(precommit_gate, "collect_changed_files", lambda: [])
    called = False

    def fake_run(command: list[str]) -> int:
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_run)

    assert precommit_gate.run_changed() == 0
    assert called is False
    assert "No changed files found for pre-commit; skipping." in capsys.readouterr().out
