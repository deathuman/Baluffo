"""Tests for the duplicate-function-body guardrail.

Covers the two invariants (no uncovered pattern, no stale baseline entry), the
false-positive filters (trivial bodies, Protocol/ABC declarations), and the
degradation behaviour when the baseline is missing or malformed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.repo_health import duplicate_body_policy as policy

REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_module(root: Path, name: str, source: str) -> None:
    target = root / "src" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source, encoding="utf-8", newline="\n")


def _write_baseline(root: Path, digests: list[str]) -> None:
    path = root / policy.BASELINE_RELATIVE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"patterns": digests}), encoding="utf-8", newline="\n")


DUPLICATED_BODY = '''
def _helper_{i}(value, default=0):
    """A multi-line body that is repeated verbatim."""
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    return default
'''


def _duplicated_source(count: int = 3) -> str:
    return "".join(DUPLICATED_BODY.format(i=i) for i in range(count))


class TestDetection:
    def test_detects_repeated_bodies(self, tmp_path: Path) -> None:
        _write_module(tmp_path, "probe.py", _duplicated_source())
        patterns = policy.collect_duplicate_patterns(tmp_path)
        assert len(patterns) == 1
        assert patterns[0].copy_count == 3

    def test_ignores_fewer_than_min_copies(self, tmp_path: Path) -> None:
        _write_module(tmp_path, "probe.py", _duplicated_source(count=2))
        assert policy.collect_duplicate_patterns(tmp_path) == []

    def test_ignores_trivial_bodies(self, tmp_path: Path) -> None:
        """One-line shims are not worth coupling across packages."""
        source = "".join(
            f"def _tiny_{i}(value):\n    return dict(value) if isinstance(value, dict) else {{}}\n"
            for i in range(4)
        )
        _write_module(tmp_path, "probe.py", source)
        assert policy.collect_duplicate_patterns(tmp_path) == []

    def test_ignores_protocol_declarations(self, tmp_path: Path) -> None:
        """Interface members must be restated per implementer."""
        ellipsis_source = "".join(
            f"def method_{i}(self, *, detail: str = 'summary') -> dict:\n    ...\n"
            for i in range(4)
        )
        _write_module(tmp_path, "probe.py", ellipsis_source)
        assert policy.collect_duplicate_patterns(tmp_path) == []

        notimpl_source = "".join(
            f"def method_{i}(self, *, detail: str = 'summary') -> dict:\n"
            "    raise NotImplementedError\n"
            for i in range(4)
        )
        _write_module(tmp_path, "probe2.py", notimpl_source)
        assert policy.collect_duplicate_patterns(tmp_path) == []

    def test_ignores_tests_directory(self, tmp_path: Path) -> None:
        """Fixtures and stubs are duplicated by design in tests/."""
        target = tmp_path / "tests" / "test_probe.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_duplicated_source(), encoding="utf-8", newline="\n")
        assert policy.collect_duplicate_patterns(tmp_path) == []

    def test_ignores_unparseable_files(self, tmp_path: Path) -> None:
        _write_module(tmp_path, "broken.py", "def (:\n")
        _write_module(tmp_path, "probe.py", _duplicated_source())
        assert len(policy.collect_duplicate_patterns(tmp_path)) == 1


class TestInvariants:
    def test_new_pattern_is_uncovered(self, tmp_path: Path) -> None:
        _write_module(tmp_path, "probe.py", _duplicated_source())
        _write_baseline(tmp_path, [])
        assert len(policy.check_duplicate_function_bodies(tmp_path)) == 1

    def test_baselined_pattern_is_covered(self, tmp_path: Path) -> None:
        _write_module(tmp_path, "probe.py", _duplicated_source())
        digest = policy.collect_duplicate_patterns(tmp_path)[0].digest
        _write_baseline(tmp_path, [digest])
        assert policy.check_duplicate_function_bodies(tmp_path) == []

    def test_deduplicated_pattern_makes_baseline_stale(self, tmp_path: Path) -> None:
        _write_module(tmp_path, "probe.py", _duplicated_source())
        digest = policy.collect_duplicate_patterns(tmp_path)[0].digest
        _write_baseline(tmp_path, [digest])

        _write_module(tmp_path, "probe.py", "def _only_one(value):\n    return value\n")
        stale = policy.check_duplicate_bodies_stale_baseline(tmp_path)
        assert len(stale) == 1
        assert digest in stale[0]


class TestDegradation:
    def test_missing_baseline_is_reported_not_raised(self, tmp_path: Path) -> None:
        _write_module(tmp_path, "probe.py", _duplicated_source())
        # Must not raise even though the baseline file is absent.
        messages = policy.check_duplicate_bodies_stale_baseline(tmp_path)
        assert any("missing" in message for message in messages)

    def test_malformed_baseline_is_reported(self, tmp_path: Path) -> None:
        path = tmp_path / policy.BASELINE_RELATIVE_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json", encoding="utf-8", newline="\n")
        messages = policy.check_duplicate_bodies_stale_baseline(tmp_path)
        assert any("not valid JSON" in message for message in messages)

    def test_wrong_shape_baseline_is_reported(self, tmp_path: Path) -> None:
        path = tmp_path / policy.BASELINE_RELATIVE_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"nope": []}), encoding="utf-8", newline="\n")
        messages = policy.check_duplicate_bodies_stale_baseline(tmp_path)
        assert any("`patterns` list" in message for message in messages)

    def test_missing_baseline_does_not_mark_everything_uncovered(self, tmp_path: Path) -> None:
        """A missing baseline must not flood the warning with known patterns."""
        _write_module(tmp_path, "probe.py", _duplicated_source())
        assert policy.load_baseline(tmp_path) == set()


class TestRepositoryBaseline:
    """The committed baseline must be consistent with the repository."""

    def test_baseline_file_is_valid(self) -> None:
        assert policy.check_baseline_file(REPO_ROOT) == []

    def test_no_uncovered_patterns_in_repository(self) -> None:
        uncovered = policy.check_duplicate_function_bodies(REPO_ROOT)
        assert not uncovered, (
            "new duplicate function bodies detected; reuse the existing "
            "implementation or add a deliberate baseline entry:\n\n" + "\n\n".join(uncovered)
        )

    def test_no_stale_baseline_entries(self) -> None:
        assert policy.check_duplicate_bodies_stale_baseline(REPO_ROOT) == []


class TestGuardrailWiring:
    def test_group_is_registered(self) -> None:
        from tools.repo_health import repo_guardrails

        assert "duplication" in repo_guardrails.GROUPS
        assert repo_guardrails.GROUP_RUNNERS["duplication"] is repo_guardrails.run_duplication_group

    def test_repository_duplication_group_passes(self) -> None:
        from tools.repo_health import repo_guardrails

        assert repo_guardrails.run_duplication_group() == []

    def test_warn_only_until_enforcing(self) -> None:
        """Warn-only is the shipped default so the seeded baseline can be validated."""
        from tools.repo_health import repo_guardrails

        assert repo_guardrails.DUP_GATE_ENFORCING is False


@pytest.mark.parametrize(
    "attribute", ["collect_duplicate_patterns", "check_duplicate_function_bodies"]
)
def test_public_helpers_are_importable(attribute: str) -> None:
    assert hasattr(policy, attribute)
