"""Guardrail tests for the documented rule-count audit in ``rule_list_policy``.

``test_documented_rule_lists_name_their_enforcement`` is the check that holds every "how many
rules" sentence in the docs and module docstrings to the list beneath it and to the artifact
that keeps the rule true. The number is generated from the items rather than written by hand,
so these tests pin the accept path, each rejection path, and the ``--write`` mode that renders a
count an editor changed by hand, because the audit is the only thing standing between a
reworded doc and a rule nobody can find.

Rejection tests run against a throwaway copy of the files the audit reads, so a failing
assertion here cannot rewrite the tracked docs while another agent is editing them. The
generator is exercised on that copy for the same reason: ``--write`` edits tracked files.
"""

import shutil
from pathlib import Path

import pytest

from tools.repo_health import rule_list_policy
from tools.repo_health.rule_list_policy import (
    _planned_count_edits,
    _rendered_count,
    _resolves,
    main,
    write_generated_counts,
)
from tools.repo_health.rule_list_policy import (
    test_documented_rule_lists_name_their_enforcement as audit,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]

# Everything the audit reads: the docs that state counts, the modules whose docstrings do, the
# policy modules behind the enforcement it resolves, and the enforcement targets themselves.
# A new claim source outside this set makes the fixture-tree test fail rather than pass quietly.
_AUDIT_INPUTS = (
    "docs/*.md",
    "tools/*.py",
    "tools/repo_health/*.py",
    "src/local_data_store_tracking.py",
    "src/jobs/adapters/community/google_sheets.py",
    "tests/test_*.py",
    "tests/bridge/test_registry_conflict_demotion_provenance.py",
    "tests/tools/*.py",
)


def _fake_repo(tmp_path: Path) -> Path:
    """A throwaway repo root holding every file the audit reads, copied byte for byte."""
    repo = tmp_path / "repo"
    for pattern in _AUDIT_INPUTS:
        for source in sorted(_REPO_ROOT.glob(pattern)):
            if not source.is_file():
                continue
            destination = repo / source.relative_to(_REPO_ROOT)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
    return repo


def _mutate(repo: Path, rel_path: str, old: str, new: str) -> None:
    """Rewrite one exact substring, asserting the precondition so a stale test fails loudly."""
    path = repo / rel_path
    text = path.read_text(encoding="utf-8")
    assert text.count(old) == 1, f"precondition: {rel_path} should hold {old!r} exactly once"
    path.write_text(text.replace(old, new), encoding="utf-8", newline="\n")


def _with_entry(
    monkeypatch: pytest.MonkeyPatch,
    key: str,
    *,
    enforcement: tuple[str, ...] | None = None,
    reason: str | None = None,
) -> None:
    """Replace one registry entry's enforcement and reason, leaving the rest of it intact."""
    entries = tuple(
        rule_list_policy.RuleList(
            key=entry.key,
            source=entry.source,
            anchor=entry.anchor,
            items=entry.items,
            enforcement=entry.enforcement if enforcement is None else enforcement,
            enforcement_named_in=entry.enforcement_named_in,
            reason=entry.reason if reason is None else reason,
        )
        if entry.key == key
        else entry
        for entry in rule_list_policy.RULE_LISTS
    )
    monkeypatch.setattr(rule_list_policy, "RULE_LISTS", entries)


def test_enforcement_identifiers_resolve_to_real_checks(repo_root: Path) -> None:
    """Each identifier shape the registry allows has to name something that exists."""
    assert _resolves(repo_root, "test_documented_rule_lists_name_their_enforcement")
    assert _resolves(repo_root, "test_local_data_tracking_mirror_and_transition_guards")
    assert _resolves(repo_root, "tools/coverage_drain.py")
    assert not _resolves(repo_root, "tools/not_a_module.py")
    assert not _resolves(repo_root, "test_not_a_policy_or_suite_check")


def test_rule_list_audit_accepts_the_current_tree(repo_root: Path) -> None:
    """Every rule count in this repo adds up and names enforcement that exists."""
    audit(repo_root)


def test_rule_list_audit_accepts_the_fixture_tree(tmp_path: Path) -> None:
    """The fixture copy carries everything the audit reads, so its mutations are the only change."""
    audit(_fake_repo(tmp_path))


def test_rule_list_audit_rejects_a_count_the_items_contradict(tmp_path: Path) -> None:
    """Three claimed over two bullets is the drift the audit exists to catch."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "docs/DATA_CONTRACT.md",
        "Two rules are about not leaking storage semantics into tracking",
        "Three rules are about not leaking storage semantics into tracking",
    )

    with pytest.raises(AssertionError, match="states a count its own items contradict") as failure:
        audit(repo)

    # The failure has to name the command that renders the number, or the message leaves an
    # editor guessing whether the sentence or the list is the thing to edit.
    assert "rule_list_policy --write" in str(failure.value)


def test_rule_list_audit_rejects_an_unregistered_doc_claim(tmp_path: Path) -> None:
    """A new rule list in a doc has to be registered, not just written down."""
    repo = _fake_repo(tmp_path)
    path = repo / "docs/measurement-methods.md"
    path.write_text(
        path.read_text(encoding="utf-8")
        + "\nTwo rules govern the sampling window:\n\n- one\n- two\n",
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(
        AssertionError, match="not registered in tools/repo_health/rule_list_policy"
    ):
        audit(repo)


def test_rule_list_audit_rejects_an_unregistered_docstring_claim(tmp_path: Path) -> None:
    """A module docstring states counts too, so it is held to the same registry."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "tests/tools/test_corpus_store.py",
        '"""The corpus is only evidence if it round-trips exactly and never lies by omission.',
        '"""Two rules govern the corpus handle.\n\n* one\n* two\n\nThe corpus is only evidence if it '
        "round-trips exactly and never lies by omission.",
    )

    with pytest.raises(
        AssertionError, match="not registered in tools/repo_health/rule_list_policy"
    ):
        audit(repo)


def test_rule_list_audit_rejects_a_stale_registry_entry(tmp_path: Path) -> None:
    """An entry whose sentence is gone has to be re-anchored or deleted, not left behind."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "docs/scraping-pipeline.md",
        "Two rules follow, and both are structural rather than sheet-specific",
        "Several rules follow, and both are structural rather than sheet-specific",
    )

    with pytest.raises(
        AssertionError, match="registers rule lists that no longer state their count"
    ):
        audit(repo)


def test_rule_list_audit_rejects_enforcement_that_does_not_exist(
    repo_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A list pointing at a file nobody can find names nothing at all."""
    _with_entry(monkeypatch, "corpus_store_rules", enforcement=("tools/not_a_module.py",))

    with pytest.raises(AssertionError, match="names enforcement that does not exist"):
        audit(repo_root)


def test_rule_list_audit_rejects_enforcement_the_list_never_names(
    repo_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real enforcement a reader cannot see is enforcement the next editor will not preserve."""
    _with_entry(monkeypatch, "corpus_store_rules", enforcement=("tools/coverage_drain.py",))

    with pytest.raises(AssertionError, match="enforced by something it never names"):
        audit(repo_root)


def test_rule_list_audit_rejects_an_enforcement_name_the_registry_does_not_declare(
    tmp_path: Path,
) -> None:
    """A check named beside a rule that the registry omits is an enforcement claim nobody made."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "docs/measurement-methods.md",
        "- **A wrong instrument looks exactly like a missing thing.**",
        "- **A wrong instrument looks exactly like a missing thing** -- pinned by\n"
        "  `tests/test_not_registered.py`.",
    )

    with pytest.raises(AssertionError, match="names enforcement the registry does not declare"):
        audit(repo)


def test_rule_list_generator_renders_the_count_from_the_items(tmp_path: Path) -> None:
    """The number is a rendering of the list, so the generator is how a stale one moves."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "docs/DATA_CONTRACT.md",
        "Two rules are about not leaking storage semantics into tracking",
        "Five rules are about not leaking storage semantics into tracking",
    )

    edits = write_generated_counts(repo)

    assert [(edit.source, edit.was, edit.now) for edit in edits] == [
        ("docs/DATA_CONTRACT.md", "Five", "Two")
    ]
    repaired = (repo / "docs/DATA_CONTRACT.md").read_text(encoding="utf-8")
    assert "Two rules are about not leaking" in repaired
    assert "Five rules" not in repaired
    audit(repo)


def test_rule_list_generator_leaves_this_tree_alone(repo_root: Path) -> None:
    """Every count in this repo is already the one its own list renders."""
    edits, unregistered = _planned_count_edits(repo_root)

    assert edits == []
    assert unregistered == []


def test_rule_list_generator_refuses_a_claim_the_registry_does_not_describe(
    tmp_path: Path,
) -> None:
    """A number rendered for a list nobody enumerated would be a claim about nothing."""
    repo = _fake_repo(tmp_path)
    path = repo / "docs/measurement-methods.md"
    path.write_text(
        path.read_text(encoding="utf-8")
        + "\nTwo rules govern the sampling window:\n\n- one\n- two\n",
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(AssertionError, match="Cannot generate counts"):
        write_generated_counts(repo)


def test_rule_list_cli_repairs_a_drifted_count(tmp_path: Path) -> None:
    """The command the failure message names is the one that puts the number right."""
    repo = _fake_repo(tmp_path)
    _mutate(
        repo,
        "docs/scraping-pipeline.md",
        "Two rules follow, and both are structural rather than sheet-specific",
        "Nine rules follow, and both are structural rather than sheet-specific",
    )

    assert main(["--write", "--repo-root", str(repo)]) == 0

    repaired = (repo / "docs/scraping-pipeline.md").read_text(encoding="utf-8")
    assert "Two rules follow, and both are structural" in repaired
    assert "Nine rules" not in repaired
    audit(repo)


@pytest.mark.parametrize(
    ("count", "on_disk", "expected"),
    [(2, "Two", "Two"), (3, "three", "three"), (4, "FOUR", "FOUR"), (12, "12", "12")],
)
def test_rule_list_count_rendering_follows_the_spelling_on_disk(
    count: int, on_disk: str, expected: str
) -> None:
    """A claim starts a line, so the render keeps the writer's capitalization."""
    assert _rendered_count(count, on_disk) == expected


@pytest.mark.parametrize("key", ["corpus_store_rules", "thread_cpu_driver_rules"])
def test_rule_list_audit_rejects_a_list_with_no_enforcement_and_no_reason(
    repo_root: Path, monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    """Every list either names what decides it or says why nothing can.

    Parametrised over an enumerated list and a prose one, because a count stated inside a
    paragraph is registered for the same reason a bulleted list is.
    """
    _with_entry(monkeypatch, key, enforcement=(), reason="")

    with pytest.raises(AssertionError, match="name no enforcement and give no reason"):
        audit(repo_root)
