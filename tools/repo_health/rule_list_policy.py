"""Guardrail: a documented rule count is generated from the list beneath it.

A rule list is a claim in prose -- "three rules are load-bearing", "four rules carry the
weight" -- and prose counts are the cheapest kind of statement to leave stale. The list
grows a fourth bullet and the sentence still says three; the rule is deleted and the
sentence stays. Nothing failed for any of them, because nothing compared the sentence to
the list, and nothing asked what keeps the rule true.

Three properties hold for every count claim in ``docs/**/*.md`` and in every module
docstring under ``tools/``, ``tests/``, ``src/`` and ``scripts/``:

1. **Every claim is registered here.** The registry is the join between the sentence and
   the code, so a new rule list cannot appear unaccounted for. An entry states how the list
   is written and which real artifact keeps the rule true.
2. **The count is generated, not authored.** The number beside a list has to be the one
   ``_rendered_count`` writes from the list's own items, so no editor writes it by hand:
   ``python -m tools.repo_health.rule_list_policy --write`` rewrites every count that
   drifted, in the docs and in the module docstrings alike. A number that disagrees with the
   items fails the audit, and the failure names that command.
3. **The enforcement is exact and readable.** Names resolve to something that exists: a
   ``run_*_group``-registered policy check, a pytest test or module, or the module that
   implements the rule. They are also exactly the names the registry declares -- an
   identifier written beside a rule that the registry does not declare is an enforcement
   claim nobody registered, and a declared one the list never names is enforcement a reader
   cannot see. A list whose rules no check can decide must say so in a reason instead of
   inventing a guardrail that does not enforce it.

A count stated inside a paragraph (``ITEM_PROSE``) has no item block to render from, so its
number stays authored; only its registration and enforcement are audited.

Deliberately plain functions with no pytest import: the CI ``Lint`` lane runs the guardrail
groups without pytest installed, and a module-level ``import pytest`` there turns the whole
lane red.
"""

from __future__ import annotations

import argparse
import ast
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from tools.repo_health.policy_wiring_policy import registered_policy_checks
from tools.repo_health.release_docs_policy import CHANGELOG_RULES

ROOT = Path(__file__).resolve().parents[2]

# How the items of a list are enumerated, which is also what the stated count is compared
# against. ``PROSE`` marks a count stated inside a paragraph rather than a list; there is no
# item block to count, so the entry registers the claim and its enforcement without a count.
ITEM_BULLETS = "bullets"
ITEM_BOLD_LEADS = "bold-leads"
ITEM_TABLE_ROWS = "table-rows"
ITEM_PROSE = "prose"

_NUMBER_WORDS = (
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
)

# A count claim starts a line, optionally behind a markdown bullet or quote marker, because
# mid-sentence mentions ("selectors / 236 rule lines") are not claims about a list.
_COUNT_CLAIM = re.compile(
    r"^(?:[ \t]*[>*-][ \t]+)?(?:\*\*)?(" + "|".join(_NUMBER_WORDS) + r"|\d+)[ \t]+rules?\b",
    re.IGNORECASE | re.MULTILINE,
)
_CLAIM_SOURCES = ("tools", "tests", "src", "scripts")
_SKIP_SOURCE_PARTS = {"__pycache__", ".venv", "node_modules", "data", "_out", "archive"}

# A backticked identifier -- one backtick in prose, two in a docstring -- and the ``:400`` a
# reference sometimes carries after the path.
_IDENTIFIER = re.compile(r"`{1,2}([^`\n]+?)`{1,2}")
_LINE_REFERENCE = re.compile(r":\d+(?:-\d+)?")


@dataclass(frozen=True)
class RuleList:
    """One documented rule list, the count it states, and what keeps it true.

    ``anchor`` is a fragment of the sentence that states the count: it identifies the claim
    in its own file, so two lists in one doc are told apart without pinning line numbers. It
    must not itself hold the number, because the number is generated from the items and a
    stale anchor would not survive the rewrite.

    ``enforcement`` is the exact set of artifacts the list has to name. Any other identifier
    written beside the rules -- a backticked ``test_*`` or a Python path -- is an enforcement
    claim the registry does not declare, and fails the audit.
    """

    key: str
    source: str
    anchor: str
    items: str
    enforcement: tuple[str, ...] = ()
    enforcement_named_in: str = ""
    reason: str = ""


# The changelog rules are checked by functions in ``release_docs_policy``, and both statements
# of them name that module as well as the individual checks, so the module belongs to the
# declared set: the audit rejects an enforcement name beside a list that this set omits.
_CHANGELOG_ENFORCEMENT: tuple[str, ...] = (
    *(rule.guardrail for rule in CHANGELOG_RULES),
    "tools/repo_health/release_docs_policy.py",
)

# The registry. Reading it is the point: each row says which document states how many rules
# and which artifact decides them, so an editor can see both halves in one place.
RULE_LISTS: tuple[RuleList, ...] = (
    RuleList(
        key="changelog_rules",
        source="docs/CHANGELOG.md",
        anchor="rules the guardrail enforces",
        items=ITEM_BULLETS,
        enforcement=_CHANGELOG_ENFORCEMENT,
        enforcement_named_in="docs/DOCS_WORKFLOW.md",
    ),
    RuleList(
        key="changelog_rule_table",
        source="docs/DOCS_WORKFLOW.md",
        anchor="keep this from quietly regrowing",
        items=ITEM_TABLE_ROWS,
        enforcement=_CHANGELOG_ENFORCEMENT,
    ),
    RuleList(
        key="data_contract_tracking_rules",
        source="docs/DATA_CONTRACT.md",
        anchor="not leaking storage semantics into tracking",
        items=ITEM_BULLETS,
        enforcement=(
            "src/local_data_store_tracking.py",
            "tests/test_local_data_store_tracking.py",
        ),
    ),
    RuleList(
        key="startup_probe_client_stall_rules",
        source="docs/startup-probe-architecture.md",
        anchor="billing a client-side stall to the site",
        items=ITEM_BULLETS,
        enforcement=("tests/test_startup_probe_readiness.py",),
    ),
    RuleList(
        key="measurement_zero_signal_rules",
        source="docs/measurement-methods.md",
        anchor="rules follow",
        items=ITEM_BULLETS,
        reason=(
            "both rules are procedures a measurement run has to follow -- use the card extractor, "
            "and settle a board with a real isolated fetch -- so no check can decide whether one "
            "was followed; the instruments they name are covered by the corpus tests"
        ),
    ),
    RuleList(
        key="registry_retirement_rules",
        source="docs/scraping-pipeline.md",
        anchor="govern whether a source row may be removed",
        items=ITEM_BOLD_LEADS,
        enforcement=(
            "tools/coverage_drain.py",
            "tests/test_source_registry_preflight_board_coverage.py",
            "tests/bridge/test_registry_conflict_demotion_provenance.py",
        ),
    ),
    RuleList(
        key="sheet_parse_rules",
        source="docs/scraping-pipeline.md",
        anchor="structural rather than sheet-specific",
        items=ITEM_BULLETS,
        enforcement=(
            "src/jobs/adapters/community/google_sheets.py",
            "tests/test_jobs_fetcher_google_sheets.py",
        ),
    ),
    RuleList(
        key="board_identity_rules",
        source="tools/coverage_board_identity.py",
        anchor="load-bearing",
        items=ITEM_BOLD_LEADS,
        enforcement=("tests/tools/test_coverage_board_identity.py",),
    ),
    RuleList(
        key="corpus_store_rules",
        source="tests/tools/test_corpus_store.py",
        anchor="carry the weight",
        items=ITEM_BULLETS,
        enforcement=("tests/tools/test_corpus_store.py",),
    ),
    RuleList(
        key="cpu_attribution_rules",
        source="tests/tools/test_cpu_attribution.py",
        anchor="carry the most weight",
        items=ITEM_BULLETS,
        enforcement=("tests/tools/test_cpu_attribution.py",),
    ),
    RuleList(
        key="thread_cpu_driver_rules",
        source="tests/tools/test_thread_cpu.py",
        anchor="matter most",
        items=ITEM_PROSE,
        enforcement=("tests/tools/test_thread_cpu.py",),
    ),
    RuleList(
        key="thread_cpu_sampler_rules",
        source="tests/tools/test_thread_cpu_sampler.py",
        anchor="are load-bearing",
        items=ITEM_BULLETS,
        enforcement=("tests/test_profile_utils.py",),
    ),
)


@dataclass(frozen=True)
class _Claim:
    """One count claim, with the offset that maps its text back onto its file.

    A doc is audited whole, so ``offset`` is zero. A Python source is audited through its
    module docstring literal, which starts somewhere inside the file: ``offset`` is where, so
    the span of the number in ``text`` locates the same character in ``file_text``.
    """

    source: str
    match: re.Match[str]
    text: str
    file_text: str
    offset: int


@dataclass(frozen=True)
class _CountEdit:
    """One count the generator would rewrite, and what it would become."""

    source: str
    offset: int
    was: str
    now: str


def _docstring_literal(source: str) -> tuple[str, int] | None:
    """The module docstring's text and the offset it starts at in ``source``, or None.

    The literal is read as written rather than through ``ast.get_docstring``, which cleans
    indentation and escapes: the generator rewrites the characters on disk, so the audited
    text has to hold those same characters at those same offsets.
    """
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return None
    if not tree.body:
        return None
    first = tree.body[0]
    if not isinstance(first, ast.Expr) or not isinstance(first.value, ast.Constant):
        return None
    value = first.value
    if not isinstance(value.value, str) or value.end_lineno is None:
        return None
    line_starts = [0]
    for line in source.splitlines(keepends=True):
        line_starts.append(line_starts[-1] + len(line))
    start = line_starts[value.lineno - 1] + value.col_offset
    end = line_starts[value.end_lineno - 1] + value.end_col_offset
    literal = source[start:end]
    prefix = re.match(r"[rRbBuUfF]*", literal).group(0)
    body = literal[len(prefix) :]
    for quote in ('"""', "'''"):
        if body.startswith(quote):
            return body[len(quote) : -len(quote)], start + len(prefix) + len(quote)
    if body[:1] in {'"', "'"}:
        return body[1:-1], start + len(prefix) + 1
    return None


def _claim_texts(repo_root: Path) -> dict[str, tuple[str, str, int]]:
    """``(audited text, file text, offset)`` for each source that may state a count.

    Docs contribute the whole file; Python contributes the module docstring literal only,
    because a count claim inside a function docstring or a comment describes that function
    rather than a rule list the project maintains.
    """
    texts: dict[str, tuple[str, str, int]] = {}
    docs_root = repo_root / "docs"
    for path in sorted(docs_root.rglob("*.md")):
        if "archive" in path.relative_to(docs_root).parts:
            continue
        text = path.read_text(encoding="utf-8")
        texts[path.relative_to(repo_root).as_posix()] = (text, text, 0)
    for top in _CLAIM_SOURCES:
        for path in sorted((repo_root / top).rglob("*.py")):
            relative = path.relative_to(repo_root)
            if any(part in _SKIP_SOURCE_PARTS for part in relative.parts):
                continue
            file_text = path.read_text(encoding="utf-8")
            if not _COUNT_CLAIM.search(file_text):
                # Cheap pre-filter: only a file that might state a count needs its docstring.
                continue
            literal = _docstring_literal(file_text)
            if literal is None:
                continue
            docstring, offset = literal
            if not _COUNT_CLAIM.search(docstring):
                continue
            texts[relative.as_posix()] = (docstring, file_text, offset)
    return texts


def _claims(repo_root: Path) -> list[_Claim]:
    """Every count claim the audit reads, in source order."""
    return [
        _Claim(source, match, text, file_text, offset)
        for source, (text, file_text, offset) in _claim_texts(repo_root).items()
        for match in _COUNT_CLAIM.finditer(text)
    ]


def _after_sentence_line(text: str, match: re.Match[str]) -> int:
    """The offset where the claim's own line ends, for reading the sentence back."""
    line_end = text.find("\n", match.end())
    return len(text) if line_end == -1 else line_end + 1


def _list_start(text: str, match: re.Match[str]) -> int:
    """The offset where the list a claim introduces begins.

    A count sentence is often a paragraph rather than a line -- naming the check that keeps
    the rule true runs the sentence on -- so the list starts after the paragraph, or at the
    first item written inside it.
    """
    offset = _after_sentence_line(text, match)
    scan = offset
    for line in text[offset:].splitlines(keepends=True):
        stripped = line.strip()
        if not stripped:
            return scan
        if re.match(r"(?:[-*]|\d+[.)])[ \t]+\S", stripped) or stripped.startswith("**"):
            return scan
        scan += len(line)
    return scan


def _paragraph_end(text: str, match: re.Match[str]) -> int:
    """The offset where the paragraph holding the claim ends.

    A count stated inside a paragraph has no item block of its own, so what it names is
    written in that paragraph: reading on to the end of the file would hold it to every
    unrelated mention below it.
    """
    scan = match.start()
    for line in text[match.start() :].splitlines(keepends=True):
        if not line.strip():
            return scan
        scan += len(line)
    return len(text)


def _bullet_items(text: str, start: int) -> tuple[list[str], int]:
    """Every ``-``/``*`` bullet after ``start``, and where the block ends.

    Continuation lines are joined per bullet, so one rule stays one item however it wraps.
    """
    items: list[str] = []
    current: list[str] = []
    end = len(text)

    def flush() -> None:
        if current:
            items.append(" ".join(current))
            current.clear()

    cursor = start
    for line in text[start:].splitlines(keepends=True):
        line_start, cursor = cursor, cursor + len(line)
        stripped = line.strip()
        if re.match(r"(?:[-*]|\d+[.)])[ \t]+\S", stripped):
            flush()
            current.append(stripped)
        elif current and line[:1] in {" ", "\t"} and stripped:
            current.append(stripped)
        elif not stripped:
            continue
        else:
            flush()
            end = line_start
            break
    flush()
    return items, end


def _bold_lead_items(text: str, start: int) -> tuple[list[str], int]:
    """Every paragraph opening with a bold lead after ``start``, and where the block ends.

    Tables are transparent, and they are detected by their pipes rather than by a leading one:
    this repo writes table rows without the leading pipe, so a rule whose detail is tabulated
    keeps the table instead of ending at it.
    """
    items: list[str] = []
    paragraph_start = True
    end = len(text)
    cursor = start
    for line in text[start:].splitlines(keepends=True):
        line_start, cursor = cursor, cursor + len(line)
        stripped = line.strip()
        if not stripped:
            paragraph_start = True
            continue
        if stripped.startswith(("## ", "### ")) or stripped == "---":
            end = line_start
            break
        if paragraph_start and stripped.startswith("**"):
            items.append(stripped)
            paragraph_start = False
            continue
        if stripped.count("|") >= 2:
            continue
        if paragraph_start:
            end = line_start
            break
        paragraph_start = False
    return items, end


def _table_row_items(text: str, start: int) -> tuple[list[str], int]:
    """Every ``| rule | `test_...` |`` row after ``start``, and where the block ends."""
    items: list[str] = []
    end = len(text)
    cursor = start
    for line in text[start:].splitlines(keepends=True):
        line_start, cursor = cursor, cursor + len(line)
        stripped = line.strip()
        if stripped.startswith("|"):
            cells = [cell.strip() for cell in stripped.strip("|").split("|")]
            if len(cells) == 2 and re.search(r"`test_\w+`", cells[1]):
                items.append(cells[0])
            continue
        if items and stripped:
            end = line_start
            break
    return items, end


def _list_items(text: str, start: int, items: str) -> tuple[list[str], int]:
    """The list's items and the offset where they stop, by the entry's enumeration kind."""
    if items == ITEM_BULLETS:
        return _bullet_items(text, start)
    if items == ITEM_BOLD_LEADS:
        return _bold_lead_items(text, start)
    if items == ITEM_TABLE_ROWS:
        return _table_row_items(text, start)
    return [], len(text)


def _rendered_count(count: int, on_disk: str) -> str:
    """The number a list's own items render to, spelled the way the claim already is.

    The count is generated and the capitalization is not: a claim starts a line, so it is
    normally written "Two rules", while the same sentence shape appears lowercase inside a
    docstring paragraph. Ten is the last word; beyond that the digit stands, and a digit on
    disk is rendered as the word because that is how these docs count.
    """
    word = _NUMBER_WORDS[count - 1] if 1 <= count <= len(_NUMBER_WORDS) else str(count)
    if on_disk.isalpha() and on_disk.isupper():
        return word.upper()
    if word[:1].isalpha() and on_disk[:1].isupper():
        return word[:1].upper() + word[1:]
    return word


def _enforcement_ids(text: str) -> set[str]:
    """The identifiers in ``text`` that read as enforcement: a test name or a Python path."""
    names: set[str] = set()
    for token in _IDENTIFIER.findall(text):
        name = _LINE_REFERENCE.sub("", token.strip())
        if re.fullmatch(r"test_\w+", name) or name.endswith(".py"):
            names.add(name)
    return names


def _policy_checks(repo_root: Path) -> tuple[set[str], set[str]]:
    """The ``(defined, registered)`` policy check names under ``tools/repo_health``."""
    health = repo_root / "tools" / "repo_health"
    defined: set[str] = set()
    for module in sorted(health.glob("*_policy.py")):
        tree = ast.parse(module.read_text(encoding="utf-8"))
        defined.update(
            node.name
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name.startswith("test_")
        )
    registered = {name for names in registered_policy_checks(repo_root).values() for name in names}
    return defined, registered


def _pytest_test_names(repo_root: Path) -> set[str]:
    """Every ``def test_*`` in the suite, read only when an identifier needs it."""
    pattern = re.compile(r"^[ \t]*def (test_\w+)", re.MULTILINE)
    names: set[str] = set()
    for path in (repo_root / "tests").rglob("test_*.py"):
        try:
            names.update(pattern.findall(path.read_text(encoding="utf-8", errors="ignore")))
        except OSError:  # pragma: no cover - unreadable test file
            continue
    return names


def _resolves(repo_root: Path, identifier: str) -> bool:
    """Whether a documented enforcement identifier names something that exists."""
    if identifier.startswith("test_"):
        defined, registered = _policy_checks(repo_root)
        if identifier in defined and identifier in registered:
            return True
        return identifier in _pytest_test_names(repo_root)
    return (repo_root / identifier).exists()


def _matched_claims(repo_root: Path) -> tuple[dict[int, _Claim], list[str]]:
    """Each registry entry paired with its claim, plus the sentences no entry describes."""
    matched: dict[int, _Claim] = {}
    unregistered: list[str] = []
    for claim in _claims(repo_root):
        sentence = claim.text[
            claim.match.start() : _after_sentence_line(claim.text, claim.match)
        ].strip()
        entry = next(
            (
                index
                for index, candidate in enumerate(RULE_LISTS)
                if candidate.source == claim.source and candidate.anchor in sentence
            ),
            None,
        )
        if entry is None:
            unregistered.append(f"      {claim.source}: {sentence[:96]}")
        else:
            matched[entry] = claim
    return matched, unregistered


def _planned_count_edits(repo_root: Path) -> tuple[list[_CountEdit], list[str]]:
    """The count rewrites the generator would make, and the claims it cannot place.

    A claim the registry does not describe has no enumeration kind, so there is no list to
    render a number from: the generator reports it instead of guessing one.
    """
    matched, unregistered = _matched_claims(repo_root)
    edits: list[_CountEdit] = []
    for index, entry in enumerate(RULE_LISTS):
        if index not in matched or entry.items == ITEM_PROSE:
            continue
        claim = matched[index]
        items, _ = _list_items(claim.text, _list_start(claim.text, claim.match), entry.items)
        rendered = _rendered_count(len(items), claim.match.group(1))
        if claim.match.group(1) != rendered:
            edits.append(
                _CountEdit(
                    source=claim.source,
                    offset=claim.match.span(1)[0] + claim.offset,
                    was=claim.match.group(1),
                    now=rendered,
                )
            )
    return edits, unregistered


def write_generated_counts(repo_root: Path) -> list[_CountEdit]:
    """Rewrite every count the list's own items contradict, and report what was rewritten.

    Refuses when a claim is unregistered: a number rendered for a list nobody enumerated
    would be a generated claim about nothing.
    """
    edits, unregistered = _planned_count_edits(repo_root)
    assert not unregistered, (
        "Cannot generate counts for rule count claims the registry does not describe:\n"
        + "\n".join(unregistered)
        + "\n    Register the list first: without an enumeration kind there is nothing to "
        "count."
    )
    by_source: dict[str, list[_CountEdit]] = {}
    for edit in edits:
        by_source.setdefault(edit.source, []).append(edit)
    for source, source_edits in by_source.items():
        path = repo_root / source
        text = path.read_text(encoding="utf-8")
        for edit in sorted(source_edits, key=lambda item: item.offset, reverse=True):
            text = text[: edit.offset] + edit.now + text[edit.offset + len(edit.was) :]
        path.write_text(text, encoding="utf-8", newline="\n")
    return edits


def test_documented_rule_lists_name_their_enforcement(repo_root: Path) -> None:
    """Every documented rule count is generated from its list and names its enforcement.

    The registry is what makes a count claim accountable: discovery finds the claims, and any
    claim the registry does not describe fails, so a new rule list cannot slip in unwatched.
    In the other direction a registered entry whose sentence no longer exists fails too,
    because a stale entry is an exemption from a check nobody remembers granting.
    """
    matched, unregistered = _matched_claims(repo_root)

    assert not unregistered, (
        "These rule count claims are not registered in tools/repo_health/rule_list_policy.py:\n"
        + "\n".join(unregistered)
        + "\n    Add a RuleList naming the file, the phrase that states the count, how the items "
        "are written, and what keeps the rule true -- a registered guardrail, a pytest test or "
        "module, or the reason no check can decide it."
    )

    stale = [entry.key for index, entry in enumerate(RULE_LISTS) if index not in matched]
    assert not stale, (
        "RULE_LISTS registers rule lists that no longer state their count: "
        + ", ".join(stale)
        + ". Re-anchor the entry to the sentence that replaced it, or delete the entry."
    )

    wrong_counts: list[str] = []
    unresolved: list[str] = []
    invisible: list[str] = []
    undeclared: list[str] = []
    unexplained: list[str] = []
    for index, entry in enumerate(RULE_LISTS):
        claim = matched[index]
        text = claim.text
        items, block_end = _list_items(text, _list_start(text, claim.match), entry.items)
        if entry.items == ITEM_PROSE:
            block_end = _paragraph_end(text, claim.match)
        else:
            rendered = _rendered_count(len(items), claim.match.group(1))
            if claim.match.group(1) != rendered:
                wrong_counts.append(
                    f"      {claim.source}: says {claim.match.group(1)}, lists {len(items)} "
                    f"({entry.key})"
                )
        if not entry.enforcement and not entry.reason.strip():
            unexplained.append(f"      {claim.source} ({entry.key})")
            continue
        named_in = (
            (repo_root / entry.enforcement_named_in).read_text(encoding="utf-8")
            if entry.enforcement_named_in
            else ""
        )
        region = text[claim.match.start() : block_end]
        declared = set(entry.enforcement) | {entry.source}
        undeclared += [
            f"      {name} (for {entry.key})"
            for name in sorted(_enforcement_ids(region) - declared)
        ]
        for identifier in entry.enforcement:
            if not _resolves(repo_root, identifier):
                unresolved.append(f"      {identifier} (for {entry.key})")
            elif (
                identifier != entry.source
                and identifier not in region
                and identifier not in named_in
            ):
                invisible.append(f"      {identifier} (for {entry.key})")

    assert not wrong_counts, (
        "A rule list states a count its own items contradict:\n"
        + "\n".join(wrong_counts)
        + "\n    The number is generated from the list beneath it: run `python -m "
        "tools.repo_health.rule_list_policy --write` to render it, or fix the items if the "
        "count was the intent."
    )
    assert not unexplained, (
        "These rule lists name no enforcement and give no reason:\n"
        + "\n".join(unexplained)
        + "\n    Name the check that keeps the rule true, or write the reason no check can."
    )
    assert not undeclared, (
        "A rule list names enforcement the registry does not declare:\n"
        + "\n".join(undeclared)
        + "\n    Add the identifier to the entry -- a name beside a rule that nothing registered "
        "is an enforcement claim no reader can act on -- or drop the mention."
    )
    assert not unresolved, (
        "A rule list names enforcement that does not exist:\n"
        + "\n".join(unresolved)
        + "\n    Point at a registered policy check, a pytest test or module, or an existing file "
        "-- or replace it with the reason nothing enforces the rule."
    )
    assert not invisible, (
        "A rule list is enforced by something it never names, so a reader cannot tell:\n"
        + "\n".join(invisible)
        + "\n    Name the identifier beside the rule, or in the doc the entry names."
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Audit the rule lists, or ``--write`` the counts the lists' own items imply."""
    parser = argparse.ArgumentParser(description="Audit documented rule lists.")
    parser.add_argument(
        "--write",
        action="store_true",
        help="rewrite every count the list beneath it contradicts",
    )
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    try:
        if args.write:
            edits = write_generated_counts(args.repo_root)
            for edit in edits:
                print(f"{edit.source}: {edit.was} rules -> {edit.now} rules")
            if not edits:
                print("Every documented rule count already renders from its list.")
        test_documented_rule_lists_name_their_enforcement(args.repo_root)
    except AssertionError as failure:
        print(failure)
        return 1
    print("Documented rule counts and their enforcement match the registry.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
