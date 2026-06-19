from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.repo_health import repo_guardrails, suite_contract_policy
from tools.repo_health.suppression_inventory import collect_suppressions


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


def test_routes_group_reports_inventory_failures(monkeypatch) -> None:
    monkeypatch.setattr(repo_guardrails, "check_bridge_route_inventory", lambda: ["route drift"])
    monkeypatch.setattr(repo_guardrails, "check_bridge_route_bridge_api_imports", lambda: [])
    assert "routes" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["routes"] is repo_guardrails.run_routes_group
    assert repo_guardrails.run_routes_group() == [
        repo_guardrails.GuardFailure("routes", "check_bridge_route_inventory", "route drift")
    ]


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


def test_deferred_source_line_budget_allows_exact_current_budget(
    tmp_path: Path, monkeypatch
) -> None:
    budget = tmp_path / "deferred.json"
    budget.write_text(
        json.dumps(
            {
                "files": [
                    {
                        "path": "src/deferred_owner.py",
                        "max_lines": 3,
                        "rationale": "Intentional specialized owner.",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    source_file = tmp_path / "src" / "deferred_owner.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("\n".join(["x = 1"] * 3), encoding="utf-8")

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "DEFERRED_SOURCE_BUDGET_PATH", budget)

    assert repo_guardrails.check_deferred_source_line_budget() == []


def test_deferred_source_line_budget_fails_growth_over_budget(tmp_path: Path, monkeypatch) -> None:
    budget = tmp_path / "deferred.json"
    budget.write_text(
        json.dumps(
            {
                "files": [
                    {
                        "path": "src/deferred_owner.py",
                        "max_lines": 3,
                        "rationale": "Intentional specialized owner.",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    source_file = tmp_path / "src" / "deferred_owner.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("\n".join(["x = 1"] * 4), encoding="utf-8")

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "DEFERRED_SOURCE_BUDGET_PATH", budget)

    assert repo_guardrails.check_deferred_source_line_budget() == [
        "src/deferred_owner.py has 4 lines; deferred source budget is 3."
    ]


def test_deferred_source_line_budget_requires_valid_metadata(tmp_path: Path, monkeypatch) -> None:
    budget = tmp_path / "deferred.json"
    budget.write_text(
        json.dumps(
            {
                "files": [
                    {"path": "src/missing_rationale.py", "max_lines": 3},
                    {
                        "path": "src/bad_budget.py",
                        "max_lines": 0,
                        "rationale": "Intentional specialized owner.",
                    },
                    {
                        "path": "src/missing_file.py",
                        "max_lines": 3,
                        "rationale": "Intentional specialized owner.",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "DEFERRED_SOURCE_BUDGET_PATH", budget)

    assert repo_guardrails.check_deferred_source_line_budget() == [
        "deferred source budget entry 0 for src/missing_rationale.py must include a non-empty rationale.",
        "deferred source budget entry 1 for src/bad_budget.py must include a positive integer max_lines.",
        "src/missing_file.py is listed in deferred source budget but does not exist.",
    ]


def test_source_suppression_budget_allows_current_budget(tmp_path: Path, monkeypatch) -> None:
    budget = tmp_path / "suppressions.json"
    budget.write_text(
        json.dumps(
            {
                "scope": "src",
                "max_total_comments": 2,
                "max_by_code": {"BLE001": 1, "SLF001": 1},
                "rationale": "Ratchet suppressions.",
            }
        ),
        encoding="utf-8",
    )
    source_file = tmp_path / "src" / "owner.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text(
        "try:\n"
        "    pass\n"
        "except Exception:  # noqa: BLE001\n"
        "    root._send_json({})  # noqa: SLF001\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "SOURCE_SUPPRESSION_BUDGET_PATH", budget)

    assert repo_guardrails.check_source_suppression_budget() == []


def test_source_suppression_budget_fails_growth_and_new_codes(tmp_path: Path, monkeypatch) -> None:
    budget = tmp_path / "suppressions.json"
    budget.write_text(
        json.dumps(
            {
                "scope": "src",
                "max_total_comments": 1,
                "max_by_code": {"BLE001": 1},
                "rationale": "Ratchet suppressions.",
            }
        ),
        encoding="utf-8",
    )
    source_file = tmp_path / "src" / "owner.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text(
        "except Exception:  # noqa: BLE001\nvalue = call()  # noqa: SLF001\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "SOURCE_SUPPRESSION_BUDGET_PATH", budget)

    assert repo_guardrails.check_source_suppression_budget() == [
        "src has 2 suppression comments; budget is 1.",
        "src has unbudgeted suppression code SLF001: 1.",
    ]


def test_source_suppression_budget_fails_type_ignore_regression(
    tmp_path: Path, monkeypatch
) -> None:
    budget = tmp_path / "suppressions.json"
    budget.write_text(
        json.dumps(
            {
                "scope": "src",
                "max_total_comments": 0,
                "max_by_code": {"BLE001": 0},
                "rationale": "Ratchet suppressions.",
            }
        ),
        encoding="utf-8",
    )
    source_file = tmp_path / "src" / "owner.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text(
        "value = dynamic_call()  # type: ignore[attr-defined]\n", encoding="utf-8"
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "SOURCE_SUPPRESSION_BUDGET_PATH", budget)

    assert repo_guardrails.check_source_suppression_budget() == [
        "src has 1 suppression comments; budget is 0.",
        "src has unbudgeted suppression code type-ignore: 1.",
    ]


def test_suppression_inventory_reports_code_file_hotspots(tmp_path: Path) -> None:
    source_file = tmp_path / "src" / "owner.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text(
        "except Exception:  # noqa: BLE001\nvalue = call()  # noqa: SLF001\n",
        encoding="utf-8",
    )

    inventory = collect_suppressions(tmp_path)

    assert inventory.total == 2
    assert inventory.by_code == {"BLE001": 1, "SLF001": 1}
    assert inventory.by_file == {"src/owner.py": 2}
    assert inventory.by_code_file[("BLE001", "src/owner.py")] == 1


def test_source_suppression_budget_requires_valid_metadata(tmp_path: Path, monkeypatch) -> None:
    budget = tmp_path / "suppressions.json"
    budget.write_text(
        json.dumps({"scope": "tests", "max_total_comments": -1, "max_by_code": {}}),
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "SOURCE_SUPPRESSION_BUDGET_PATH", budget)

    assert repo_guardrails.check_source_suppression_budget() == [
        "source suppression budget scope must be `src`.",
        "source suppression budget must include non-negative max_total_comments.",
        "source suppression budget must include a non-empty max_by_code object.",
        "source suppression budget must include a non-empty rationale.",
    ]


def test_fixture_reference_guard_allows_referenced_fixture(tmp_path: Path, monkeypatch) -> None:
    fixture_path = tmp_path / "tests" / "fixtures" / "referenced.json"
    fixture_path.parent.mkdir(parents=True)
    fixture_path.write_text("{}", encoding="utf-8")
    test_file = tmp_path / "tests" / "test_uses_fixture.py"
    test_file.write_text('_fixture_json("referenced.json")\n', encoding="utf-8")

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(
        repo_guardrails, "FIXTURE_REFERENCE_ALLOWLIST_PATH", tmp_path / "missing.json"
    )

    assert repo_guardrails.check_fixture_references() == []


def test_fixture_reference_guard_fails_unreferenced_fixture(tmp_path: Path, monkeypatch) -> None:
    fixture_path = tmp_path / "tests" / "fixtures" / "orphan.json"
    fixture_path.parent.mkdir(parents=True)
    fixture_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(
        repo_guardrails, "FIXTURE_REFERENCE_ALLOWLIST_PATH", tmp_path / "missing.json"
    )

    assert repo_guardrails.check_fixture_references() == [
        "tests/fixtures/orphan.json is not referenced by any test, helper, or source file."
    ]


def test_fixture_reference_allowlist_requires_reason(tmp_path: Path, monkeypatch) -> None:
    fixture_path = tmp_path / "tests" / "fixtures" / "manual.json"
    fixture_path.parent.mkdir(parents=True)
    fixture_path.write_text("{}", encoding="utf-8")
    allowlist_path = tmp_path / "allowlist.json"
    allowlist_path.write_text(
        json.dumps([{"path": "tests/fixtures/manual.json"}]),
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "FIXTURE_REFERENCE_ALLOWLIST_PATH", allowlist_path)

    assert repo_guardrails.check_fixture_references() == [
        "fixture allowlist entry 0 must include non-empty path and reason.",
        "tests/fixtures/manual.json is not referenced by any test, helper, or source file.",
    ]

    allowlist_path.write_text(
        json.dumps(
            [
                {
                    "path": "tests/fixtures/manual.json",
                    "reason": "External manual QA fixture.",
                }
            ]
        ),
        encoding="utf-8",
    )
    assert repo_guardrails.check_fixture_references() == []


def test_frontend_unit_shape_rejects_generated_manifest(tmp_path: Path, monkeypatch) -> None:
    frontend_unit_root = tmp_path / "tests" / "frontend" / "unit"
    frontend_unit_root.mkdir(parents=True)
    (frontend_unit_root / "real.test.mjs").write_text(
        'import test from "node:test";\ntest("real", () => {});\n',
        encoding="utf-8",
    )
    (frontend_unit_root / "all.test.mjs").write_text(
        'import "./real.test.mjs";\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(suite_contract_policy, "ROOT", tmp_path)

    with pytest.raises(AssertionError, match="all.test.mjs aggregators are retired"):
        suite_contract_policy.test_frontend_test_patterns_disallow_generated_manifest_aggregators()


def test_frontend_unit_shape_rejects_manifest_tool_import(tmp_path: Path, monkeypatch) -> None:
    frontend_unit_root = tmp_path / "tests" / "frontend" / "unit"
    frontend_unit_root.mkdir(parents=True)
    (frontend_unit_root / "real.test.mjs").write_text(
        'import test from "node:test";\n'
        'import "../../../scripts/sync_frontend_unit_manifest.mjs";\n'
        'test("real", () => {});\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(suite_contract_policy, "ROOT", tmp_path)

    with pytest.raises(AssertionError, match="retired frontend unit manifest tooling"):
        suite_contract_policy.test_frontend_test_patterns_disallow_generated_manifest_aggregators()
