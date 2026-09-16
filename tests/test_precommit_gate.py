from __future__ import annotations

from typing import Any

from scripts import precommit_gate


def _expected_pre_push_commands() -> list[list[str]]:
    """One `pre_commit run <hook-id> --hook-stage pre-push` per pre-push hook.

    Defined once so the three command-shape tests do not each restate the list
    and drift apart when a hook is added.
    """
    return [
        [
            precommit_gate.PYTHON,
            "-m",
            "pre_commit",
            "run",
            "--show-diff-on-failure",
            "--color=always",
            hook_id,
            "--all-files",
            "--hook-stage",
            "pre-push",
        ]
        for hook_id in precommit_gate.PRE_PUSH_HOOK_IDS
    ]


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
    guardrails_called = False
    complexity_called = False

    def fake_run(command: list[str]) -> int:
        commands.append(command)
        return 0

    def fake_complexity() -> int:
        nonlocal complexity_called
        complexity_called = True
        return 0

    def fake_guardrails(groups: tuple[str, ...] = ()) -> int:
        nonlocal guardrails_called
        assert groups == ()
        guardrails_called = True
        return 0

    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_run)
    monkeypatch.setattr(precommit_gate, "run_repo_guardrails", fake_guardrails)
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
        *_expected_pre_push_commands(),
    ]
    assert guardrails_called is True
    assert complexity_called is True


def test_pre_push_hooks_are_actually_selected(monkeypatch) -> None:
    """Every `stages: [pre-push]` hook must be requested explicitly.

    `pre_commit run` with no `--hook-stage` selects the default `pre-commit`
    stage, so a hook declared `stages: [pre-push]` is silently skipped unless it
    is named on the command line. That gap let a mypy error reach main in
    e696be64 while the local gate stayed green.
    """
    commands = precommit_gate.build_all_commands()

    default_stage = commands[0]
    assert "--hook-stage" not in default_stage

    selected = {
        # The hook id sits between the base flags and `--all-files`.
        command[command.index("--all-files") - 1]
        for command in commands[1:]
        if "--hook-stage" in command
    }
    assert selected == set(precommit_gate.PRE_PUSH_HOOK_IDS), (
        "these pre-push hooks are declared in .pre-commit-config.yaml but never "
        f"selected by the gate: {sorted(set(precommit_gate.PRE_PUSH_HOOK_IDS) - selected)}"
    )


def test_pre_push_hooks_match_the_config_declaration() -> None:
    """The gate's hook list must match what .pre-commit-config.yaml declares."""
    import re

    config_path = precommit_gate.ROOT / ".pre-commit-config.yaml"
    text = config_path.read_text(encoding="utf-8")

    declared = {
        block.split("\n", 1)[0].strip()
        for block in re.split(r"\n\s*-\s*id:\s*", text)[1:]
        if re.search(r"stages:\s*\[pre-push\]", block)
    }
    assert declared, "expected at least one pre-push hook in .pre-commit-config.yaml"
    assert declared == set(precommit_gate.PRE_PUSH_HOOK_IDS), (
        "PRE_PUSH_HOOK_IDS drifted from .pre-commit-config.yaml: "
        f"config={sorted(declared)} gate={sorted(precommit_gate.PRE_PUSH_HOOK_IDS)}"
    )


def test_run_precommit_command_sets_repo_local_cache(tmp_path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(command: list[str], cwd=None, env=None):
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
    guardrails_called = False
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

    def fake_guardrails(groups: tuple[str, ...] = ()) -> int:
        nonlocal guardrails_called
        assert groups == ()
        guardrails_called = True
        return 0

    monkeypatch.setattr(precommit_gate, "collect_repo_files", fake_collect_repo_files)
    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_run)
    monkeypatch.setattr(precommit_gate, "run_repo_guardrails", fake_guardrails)
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
        *_expected_pre_push_commands(),
    ]
    assert guardrails_called is True
    assert complexity_called is True


def test_build_all_commands_chunks_filtered_repo_files(monkeypatch) -> None:
    monkeypatch.setattr(precommit_gate, "MAX_PRECOMMIT_FILES_PER_COMMAND", 2)

    commands = precommit_gate.build_all_commands(["a.py", "b.py", "c.py"])

    assert commands == [
        [
            precommit_gate.PYTHON,
            "-m",
            "pre_commit",
            "run",
            "--show-diff-on-failure",
            "--color=always",
            "--files",
            "a.py",
            "b.py",
        ],
        [
            precommit_gate.PYTHON,
            "-m",
            "pre_commit",
            "run",
            "--show-diff-on-failure",
            "--color=always",
            "--files",
            "c.py",
        ],
        *_expected_pre_push_commands(),
    ]


def test_run_all_stops_before_complexity_when_precommit_fails(monkeypatch) -> None:
    guardrails_called = False
    complexity_called = False

    def fake_run(command: list[str]) -> int:
        return 1

    def fake_complexity() -> int:
        nonlocal complexity_called
        complexity_called = True
        return 0

    def fake_guardrails(groups: tuple[str, ...] = ()) -> int:
        nonlocal guardrails_called
        guardrails_called = True
        return 0

    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_run)
    monkeypatch.setattr(precommit_gate, "run_repo_guardrails", fake_guardrails)
    monkeypatch.setattr(precommit_gate, "run_complexity_baseline", fake_complexity)

    assert precommit_gate.run_all() == 1
    assert guardrails_called is False
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


def test_run_changed_runs_repo_guardrails_for_policy_sensitive_files(monkeypatch) -> None:
    guardrails_called = False

    monkeypatch.setattr(precommit_gate, "collect_changed_files", lambda: ["src/app.py"])
    monkeypatch.setattr(precommit_gate, "_run_precommit_command", lambda command: 0)

    def fake_guardrails(groups: tuple[str, ...] = ()) -> int:
        nonlocal guardrails_called
        assert groups == ()
        guardrails_called = True
        return 0

    monkeypatch.setattr(precommit_gate, "run_repo_guardrails", fake_guardrails)

    assert precommit_gate.run_changed() == 0
    assert guardrails_called is True
