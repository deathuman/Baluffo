from __future__ import annotations

import json
from pathlib import Path

from tools.repo_health import repo_guardrails


def test_source_suppression_budget_fails_unallowed_code_file(tmp_path: Path, monkeypatch) -> None:
    budget = tmp_path / "suppressions.json"
    budget.write_text(
        json.dumps(
            {
                "scope": "src",
                "max_total_comments": 1,
                "max_by_code": {"BLE001": 1},
                "allowed_by_code_file": {"BLE001": ["src/error_boundary.py"]},
                "rationale": "Ratchet suppressions.",
            }
        ),
        encoding="utf-8",
    )
    source_file = tmp_path / "src" / "owner.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("except Exception:  # noqa: BLE001\n", encoding="utf-8")

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)
    monkeypatch.setattr(repo_guardrails, "SOURCE_SUPPRESSION_BUDGET_PATH", budget)

    assert repo_guardrails.check_source_suppression_budget() == [
        "src has BLE001 suppression in src/owner.py; allowed files are ['src/error_boundary.py']."
    ]
