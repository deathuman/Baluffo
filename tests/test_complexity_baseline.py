from __future__ import annotations

import json

import pytest

from scripts import check_complexity_baseline as complexity


def test_load_baseline_rejects_duplicate_json_keys(tmp_path) -> None:
    baseline_path = tmp_path / "complexity_baseline.json"
    baseline_path.write_text(
        """
{
  "ruff_version": "0.15.9",
  "rule": "C901",
  "threshold": 10,
  "scope": ["src"],
  "entries": {
    "src/example.py::target": 11,
    "src/example.py::target": 12
  }
}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(complexity.ComplexityBaselineError, match="Duplicate JSON key"):
        complexity.load_baseline(baseline_path)


def test_validate_baseline_requires_explicit_metadata() -> None:
    baseline = {
        "ruff_version": "0.15.9",
        "rule": "C901",
        "threshold": 10,
        "scope": ["src"],
        "entries": {"src/example.py::target": 11},
    }

    assert complexity.validate_baseline(baseline, ruff_version="0.15.9") == {
        "src/example.py::target": 11
    }

    baseline["threshold"] = 11
    with pytest.raises(complexity.ComplexityBaselineError, match="threshold"):
        complexity.validate_baseline(baseline, ruff_version="0.15.9")


def test_findings_by_key_rejects_duplicate_effective_findings() -> None:
    finding = {
        "code": "C901",
        "filename": "src/example.py",
        "message": "`target` is too complex (12 > 10)",
    }

    with pytest.raises(complexity.ComplexityBaselineError, match="Duplicate Ruff"):
        complexity.findings_by_key([finding, dict(finding)])


def test_compare_findings_allows_equal_or_improved_complexity() -> None:
    baseline = {
        "src/example.py::equal": 12,
        "src/example.py::improved": 18,
    }
    current = {
        "src/example.py::equal": 12,
        "src/example.py::improved": 14,
    }

    assert complexity.compare_findings(baseline, current) == []


def test_compare_findings_fails_new_and_worsened_hotspots() -> None:
    failures = complexity.compare_findings(
        {"src/example.py::existing": 12},
        {
            "src/example.py::existing": 13,
            "src/example.py::new_target": 11,
        },
    )

    assert failures == [
        "Complexity increased: src/example.py::existing is 13; baseline allows 12",
        "New complexity hotspot: src/example.py::new_target is 11 > 10",
    ]


def test_checked_in_baseline_schema_is_valid() -> None:
    baseline = complexity.load_baseline()
    entries = complexity.validate_baseline(baseline, ruff_version="0.15.9")

    assert baseline["rule"] == "C901"
    assert baseline["threshold"] == 10
    assert baseline["scope"] == ["src"]
    assert len(entries) == len(set(entries))
    assert json.dumps(baseline)
