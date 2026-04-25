from __future__ import annotations

from scripts import refactor_changed_gate


def test_collect_refactor_changed_files_falls_back_to_committed_diff(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(refactor_changed_gate.precommit_gate, "collect_changed_files", lambda: [])
    monkeypatch.setattr(refactor_changed_gate, "_resolve_diff_base", lambda: "abc123")
    monkeypatch.setattr(
        refactor_changed_gate.precommit_gate,
        "_is_excluded_root",
        lambda rel_path: rel_path.startswith("data/"),
    )
    monkeypatch.setattr(refactor_changed_gate.precommit_gate, "ROOT", tmp_path)

    def fake_git_lines(*args: str) -> list[str]:
        if args == ("diff", "--name-only", refactor_changed_gate.DIFF_FILTER, "abc123..HEAD"):
            return ["docs/CHANGELOG.md", "src/ship/desktop_app/startup.py", "data/ignored.json"]
        raise AssertionError(f"Unexpected git query: {args}")

    monkeypatch.setattr(refactor_changed_gate.precommit_gate, "_git_lines", fake_git_lines)

    for rel_path in ("docs/CHANGELOG.md", "src/ship/desktop_app/startup.py"):
        path = tmp_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    assert refactor_changed_gate.collect_refactor_changed_files() == [
        "docs/CHANGELOG.md",
        "src/ship/desktop_app/startup.py",
    ]


def test_build_verification_commands_routes_docs_changes_to_release_docs() -> None:
    assert refactor_changed_gate.build_verification_commands(["docs/CHANGELOG.md"]) == [
        refactor_changed_gate.DOCS_COMMAND
    ]


def test_build_verification_commands_routes_desktop_app_surface_to_contract_and_subsystem_tests() -> (
    None
):
    assert (
        refactor_changed_gate.build_verification_commands(["src/ship/desktop_app/startup.py"])
        == refactor_changed_gate.GROUP_COMMANDS["desktop_app"]
    )


def test_build_verification_commands_escalates_packaging_changes() -> None:
    assert refactor_changed_gate.build_verification_commands(
        ["src/ship/packaged_smoke/runtime.py"]
    ) == [refactor_changed_gate.EXTENDED_COMMAND]


def test_build_verification_commands_escalates_multiple_compatibility_groups() -> None:
    assert refactor_changed_gate.build_verification_commands(
        ["src/ship/desktop_app/startup.py", "src/source_sync.py"]
    ) == [refactor_changed_gate.EXTENDED_COMMAND]


def test_run_changed_skips_unrelated_changes(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        refactor_changed_gate,
        "collect_refactor_changed_files",
        lambda base_ref=None: ["frontend/jobs/app.js"],
    )
    called = False

    def fake_run(command: list[str]) -> int:
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(refactor_changed_gate, "_run_command", fake_run)

    assert refactor_changed_gate.run_changed() == 0
    assert called is False
    assert "No refactor-sensitive changes detected; skipping." in capsys.readouterr().out
