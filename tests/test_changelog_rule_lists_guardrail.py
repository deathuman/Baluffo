"""Guardrail tests for the changelog rule-list audit in ``release_docs_policy``.

``test_changelog_rule_lists_match_registered_guardrails`` is the check that compares the rules
the docs state against the guardrails registered for them, so it is the one check that can be
broken by editing a doc alone. These tests pin both directions: the current tree passes, and a
doc or a registration that moves without the other fails with a message naming the offender.

Every rejection test works on a throwaway copy of the three docs and the two policy modules the
audit reads, so a failing assertion here cannot touch the tracked changelog -- the accept/reject
tests in ``tests/test_release_docs.py`` mutate the real files and restore them, which is a race
when another agent is editing them.
"""

from pathlib import Path

import pytest

from tools.repo_health.release_docs_policy import (
    test_changelog_rule_lists_match_registered_guardrails as audit,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]

# The audit reads the rule lists, the limit sentences, and the guardrail registration; it takes
# the policy module's own source from the same root so its tests can hand it a throwaway tree.
_COPIED_PATHS = (
    "docs/CHANGELOG.md",
    "docs/DOCS_WORKFLOW.md",
    "docs/RELEASE.md",
    "tools/repo_health/release_docs_policy.py",
    "tools/repo_health/repo_guardrails.py",
)


def _fake_repo(tmp_path: Path) -> Path:
    """A throwaway repo root holding everything the audit reads, copied byte for byte."""
    repo = tmp_path / "repo"
    for rel_path in _COPIED_PATHS:
        destination = repo / rel_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        source_text = (_REPO_ROOT / rel_path).read_text(encoding="utf-8")
        destination.write_text(source_text, encoding="utf-8", newline="\n")
    return repo


def _mutate(repo: Path, rel_path: str, old: str, new: str) -> None:
    """Rewrite one exact substring, asserting the precondition so a stale test fails loudly."""
    path = repo / rel_path
    text = path.read_text(encoding="utf-8")
    assert text.count(old) == 1, f"precondition: {rel_path} should hold {old!r} exactly once"
    path.write_text(text.replace(old, new), encoding="utf-8", newline="\n")


def test_rule_list_audit_accepts_the_current_tree(repo_root: Path) -> None:
    """The lists in the docs and the registered changelog guardrails agree today."""
    audit(repo_root)


def test_rule_list_audit_rejects_lists_that_disagree_on_the_rule_count(tmp_path: Path) -> None:
    """A doc whose list shrinks without the table is the drift this audit exists to catch."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "docs/CHANGELOG.md",
        "Four rules the guardrail enforces",
        "Three rules the guardrail enforces",
    )

    with pytest.raises(AssertionError, match="disagree about how many rules"):
        audit(repo)


def test_rule_list_audit_rejects_a_bullet_no_rule_declares(tmp_path: Path) -> None:
    """A rule reworded out of the table's phrases leaves a bullet nothing enforces."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "docs/CHANGELOG.md",
        "**No bullet appears in two sections.**",
        "**A change is mentioned only once.**",
    )

    with pytest.raises(AssertionError, match="states rules that CHANGELOG_RULES does not declare"):
        audit(repo)


def test_rule_list_audit_rejects_a_documented_guardrail_no_group_registers(tmp_path: Path) -> None:
    """A documented rule whose guardrail is unregistered is a rule nothing runs."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "tools/repo_health/repo_guardrails.py",
        '            "test_changelog_clamps_released_sections",\n',
        "",
    )

    with pytest.raises(AssertionError, match="no run_\\*_group registers"):
        audit(repo)


def test_rule_list_audit_rejects_a_release_guide_guardrail_no_group_runs(tmp_path: Path) -> None:
    """A guardrail docs/RELEASE.md leans on must still be one the runner executes."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "tools/repo_health/repo_guardrails.py",
        '            "test_top_release_notes_stay_user_readable",\n',
        "",
    )

    with pytest.raises(AssertionError, match="RELEASE.md names policy guardrails"):
        audit(repo)


def test_rule_list_audit_rejects_a_registered_guardrail_no_list_documents(tmp_path: Path) -> None:
    """A changelog guardrail that never reaches the lists is invisible to an editor."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "tools/repo_health/release_docs_policy.py",
        "def test_changelog_clamps_released_sections(repo_root: Path) -> None:",
        "def test_changelog_section_dates_are_iso(repo_root: Path) -> None:\n"
        '    """A new changelog rule the docs do not describe yet."""\n'
        '    (repo_root / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")\n\n\n'
        "def test_changelog_clamps_released_sections(repo_root: Path) -> None:",
    )
    _mutate(
        repo,
        "tools/repo_health/repo_guardrails.py",
        '            "test_changelog_clamps_released_sections",\n',
        '            "test_changelog_clamps_released_sections",\n'
        '            "test_changelog_section_dates_are_iso",\n',
    )

    with pytest.raises(AssertionError, match="appear in neither rule list"):
        audit(repo)


def test_rule_list_audit_rejects_a_budget_the_docs_state_differently(tmp_path: Path) -> None:
    """A doc advertising a clamp the guardrail does not enforce teaches the wrong number."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "docs/CHANGELOG.md",
        "**At most 5 versioned sections**",
        "**At most 6 versioned sections**",
    )

    with pytest.raises(AssertionError, match="states 6 where 5 is enforced"):
        audit(repo)


def test_rule_list_audit_rejects_a_rule_paired_with_a_check_that_ignores_the_changelog(
    tmp_path: Path,
) -> None:
    """The pairing has to be real: a guardrail that never opens the file enforces nothing."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "tools/repo_health/release_docs_policy.py",
        '    changelog_text = (repo_root / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")\n'
        "    versions = [version for version, _ in _changelog_sections(changelog_text)]",
        '    changelog_text = (repo_root / "docs" / "CHANGELOG.txt").read_text(encoding="utf-8")\n'
        "    versions = [version for version, _ in _changelog_sections(changelog_text)]",
    )

    with pytest.raises(AssertionError, match="never opens docs/CHANGELOG.md"):
        audit(repo)


def test_rule_list_audit_rejects_a_stale_exemption(tmp_path: Path) -> None:
    """An exemption for a check that stopped reading the changelog hides a real guardrail."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "tools/repo_health/release_docs_policy.py",
        '    changelog_text = (repo_root / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")\n'
        "    app_version = get_app_version()\n"
        '    top_release = _section(changelog_text, f"## [{app_version}]")',
        '    changelog_text = (repo_root / "docs" / "CHANGELOG.txt").read_text(encoding="utf-8")\n'
        "    app_version = get_app_version()\n"
        '    top_release = _section(changelog_text, f"## [{app_version}]")',
    )

    with pytest.raises(AssertionError, match="exempts checks that do not read the changelog"):
        audit(repo)


def test_rule_list_audit_rejects_a_positional_reference_past_the_list(tmp_path: Path) -> None:
    """DOCS_WORKFLOW.md explains rules by position, so a short list leaves prose pointing nowhere."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "docs/DOCS_WORKFLOW.md",
        "The third rule is what caught nothing",
        "The fifth rule is what caught nothing",
    )

    with pytest.raises(AssertionError, match="explains a rule by position"):
        audit(repo)
