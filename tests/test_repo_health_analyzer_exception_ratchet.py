from __future__ import annotations

import sys
from unittest import mock

import pytest

from tools.repo_health.bin import analyze_refactorability, analyze_repo


def test_maturity_criterion_expected_failure_returns_unknown(tmp_path) -> None:
    config_path = tmp_path / "criteria.yaml"
    config_path.write_text("pillars: {}\n", encoding="utf-8")
    analyzer = analyze_repo.MaturityAnalyzer(str(tmp_path), str(config_path))

    with mock.patch.object(analyzer, "_check_file_exists", side_effect=OSError("unreadable")):
        state = analyzer._evaluate_criterion("docs", {"check": "file_exists_any"})  # noqa: SLF001

    assert state == analyze_repo.MaturityAnalyzer.STATE_UNKNOWN


def test_maturity_criterion_does_not_hide_programming_failures(tmp_path) -> None:
    config_path = tmp_path / "criteria.yaml"
    config_path.write_text("pillars: {}\n", encoding="utf-8")
    analyzer = analyze_repo.MaturityAnalyzer(str(tmp_path), str(config_path))

    with mock.patch.object(
        analyzer,
        "_check_file_exists",
        side_effect=AssertionError("bad criterion invariant"),
    ):
        with pytest.raises(AssertionError, match="bad criterion invariant"):
            analyzer._evaluate_criterion("docs", {"check": "file_exists_any"})  # noqa: SLF001


def test_maturity_main_reports_expected_operational_failure(monkeypatch) -> None:
    class FailingAnalyzer:
        def __init__(self, *_args, **_kwargs) -> None:
            raise OSError("config unavailable")

    monkeypatch.setattr(sys, "argv", ["analyze_repo.py"])
    monkeypatch.setattr(analyze_repo, "MaturityAnalyzer", FailingAnalyzer)

    assert analyze_repo.main() == 1


def test_maturity_main_does_not_hide_programming_failures(monkeypatch) -> None:
    class FailingAnalyzer:
        def __init__(self, *_args, **_kwargs) -> None:
            raise AssertionError("bad maturity invariant")

    monkeypatch.setattr(sys, "argv", ["analyze_repo.py"])
    monkeypatch.setattr(analyze_repo, "MaturityAnalyzer", FailingAnalyzer)

    with pytest.raises(AssertionError, match="bad maturity invariant"):
        analyze_repo.main()


def test_refactorability_main_reports_expected_operational_failure(monkeypatch) -> None:
    class FailingAnalyzer:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def analyze(self) -> dict[str, object]:
            raise OSError("scan unavailable")

    monkeypatch.setattr(sys, "argv", ["analyze_refactorability.py"])
    monkeypatch.setattr(analyze_refactorability, "RefactorabilityAnalyzer", FailingAnalyzer)

    assert analyze_refactorability.main() == 1


def test_refactorability_main_does_not_hide_programming_failures(monkeypatch) -> None:
    class FailingAnalyzer:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def analyze(self) -> dict[str, object]:
            raise AssertionError("bad refactorability invariant")

    monkeypatch.setattr(sys, "argv", ["analyze_refactorability.py"])
    monkeypatch.setattr(analyze_refactorability, "RefactorabilityAnalyzer", FailingAnalyzer)

    with pytest.raises(AssertionError, match="bad refactorability invariant"):
        analyze_refactorability.main()
