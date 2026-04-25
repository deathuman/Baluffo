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


def test_collect_repo_files_excludes_requested_roots(tmp_path, monkeypatch) -> None:
    files = ["data/jobs-fetch-report.json", "docs/readme.md", "src/app.py"]

    def fake_git_lines(*args: str) -> list[str]:
        if args == ("ls-files",):
            return files
        raise AssertionError(f"Unexpected git query: {args}")

    monkeypatch.setattr(precommit_gate, "ROOT", tmp_path)
    monkeypatch.setattr(precommit_gate, "_git_lines", fake_git_lines)

    for rel_path in files:
        path = tmp_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    assert precommit_gate.collect_repo_files(("data",)) == ["docs/readme.md", "src/app.py"]


def test_collect_changed_files_excludes_generated_fetch_reports(tmp_path, monkeypatch) -> None:
    changed = [
        "data/jobs-fetch-report.json",
        "data/jobs-fetch-tasks.json",
        "docs/readme.md",
        "src/app.py",
    ]

    def fake_git_lines(*args: str) -> list[str]:
        query = tuple(args)
        if query == ("diff", "--cached", "--name-only", "--diff-filter=ACMRTUXB"):
            return changed
        if query == ("diff", "--name-only", "--diff-filter=ACMRTUXB"):
            return []
        if query == ("ls-files", "--others", "--exclude-standard"):
            return []
        raise AssertionError(f"Unexpected git query: {query}")

    monkeypatch.setattr(precommit_gate, "ROOT", tmp_path)
    monkeypatch.setattr(precommit_gate, "_git_lines", fake_git_lines)

    for rel_path in changed[2:]:
        path = tmp_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    assert precommit_gate.collect_changed_files() == ["docs/readme.md", "src/app.py"]


def test_run_all_executes_precommit_and_vulture_commands(monkeypatch) -> None:
    commands: list[list[str]] = []
    complexity_called = False

    def fake_run(command: list[str]) -> int:
        commands.append(command)
        return 0

    def fake_complexity() -> int:
        nonlocal complexity_called
        complexity_called = True
        return 0

    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_run)
    monkeypatch.setattr(precommit_gate, "run_complexity_baseline", fake_complexity)

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
            "vulture",
            "--all-files",
            "--hook-stage",
            "pre-push",
        ],
    ]
    assert complexity_called is True


def test_run_precommit_command_sets_repo_local_cache(tmp_path, monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(command: list[str], cwd=None, env=None):  # type: ignore[no-untyped-def]
        captured["command"] = command
        captured["cwd"] = cwd
        captured["env"] = env

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(precommit_gate, "PRE_COMMIT_HOME", tmp_path / "precommit-home")
    monkeypatch.setattr(precommit_gate.subprocess, "run", fake_run)

    assert precommit_gate._run_precommit_command(["python", "-m", "pre_commit", "run"]) == 0
    assert captured["command"] == ["python", "-m", "pre_commit", "run"]
    assert captured["cwd"] == precommit_gate.ROOT
    assert captured["env"]["PRE_COMMIT_HOME"] == str(tmp_path / "precommit-home")
    assert (tmp_path / "precommit-home").exists()


def test_run_all_with_exclusions_uses_filtered_repo_files(monkeypatch) -> None:
    commands: list[list[str]] = []
    complexity_called = False

    def fake_collect_repo_files(exclude_roots: tuple[str, ...] = ()) -> list[str]:
        assert exclude_roots == ("data",)
        return ["docs/readme.md", "src/app.py"]

    def fake_run(command: list[str]) -> int:
        commands.append(command)
        return 0

    def fake_complexity() -> int:
        nonlocal complexity_called
        complexity_called = True
        return 0

    monkeypatch.setattr(precommit_gate, "collect_repo_files", fake_collect_repo_files)
    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_run)
    monkeypatch.setattr(precommit_gate, "run_complexity_baseline", fake_complexity)

    assert precommit_gate.run_all(("data",)) == 0
    assert commands == [
        [
            precommit_gate.PYTHON,
            "-m",
            "pre_commit",
            "run",
            "--show-diff-on-failure",
            "--color=always",
            "--files",
            "docs/readme.md",
            "src/app.py",
        ],
        [
            precommit_gate.PYTHON,
            "-m",
            "pre_commit",
            "run",
            "--show-diff-on-failure",
            "--color=always",
            "vulture",
            "--all-files",
            "--hook-stage",
            "pre-push",
        ],
    ]
    assert complexity_called is True


def test_run_all_stops_before_complexity_when_precommit_fails(monkeypatch) -> None:
    complexity_called = False

    def fake_run(command: list[str]) -> int:
        return 1

    def fake_complexity() -> int:
        nonlocal complexity_called
        complexity_called = True
        return 0

    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_run)
    monkeypatch.setattr(precommit_gate, "run_complexity_baseline", fake_complexity)

    assert precommit_gate.run_all() == 1
    assert complexity_called is False


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
