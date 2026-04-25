from __future__ import annotations

import json
from pathlib import Path

from tools.repo_health import repo_guardrails


def test_repo_guardrails_group_selection_runs_only_requested_group(monkeypatch) -> None:
    called: list[str] = []

    monkeypatch.setattr(
        repo_guardrails,
        "GROUP_RUNNERS",
        {
            "docs": lambda: called.append("docs") or [],
            "workflow": lambda: called.append("workflow") or [],
        },
    )

    assert repo_guardrails.run_groups(["workflow"]) == []
    assert called == ["workflow"]


def test_line_budget_fails_new_unbaselined_large_test(tmp_path: Path, monkeypatch) -> None:
    baseline = tmp_path / "baseline.json"
    baseline.write_text(
        json.dumps(
            {
                "default_budget": 10,
                "integration_budget": 20,
                "excluded_globs": [],
                "integration_globs": [],
                "files": {},
            }
        ),
        encoding="utf-8",
    )
    test_file = tmp_path / "tests" / "test_large.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("\n".join(["x = 1"] * 11), encoding="utf-8")

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "BASELINE_PATH", baseline)

    assert repo_guardrails.check_line_budget() == [
        "tests/test_large.py has 11 lines; budget is 10."
    ]


def test_line_budget_allows_existing_file_until_baseline_growth(
    tmp_path: Path, monkeypatch
) -> None:
    baseline = tmp_path / "baseline.json"
    baseline.write_text(
        json.dumps(
            {
                "default_budget": 10,
                "integration_budget": 20,
                "excluded_globs": [],
                "integration_globs": [],
                "files": {"tests/test_large.py": 12},
            }
        ),
        encoding="utf-8",
    )
    test_file = tmp_path / "tests" / "test_large.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("\n".join(["x = 1"] * 12), encoding="utf-8")

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "BASELINE_PATH", baseline)

    assert repo_guardrails.check_line_budget() == []

    test_file.write_text("\n".join(["x = 1"] * 13), encoding="utf-8")
    assert repo_guardrails.check_line_budget() == [
        "tests/test_large.py has 13 lines; baseline is 12."
    ]
