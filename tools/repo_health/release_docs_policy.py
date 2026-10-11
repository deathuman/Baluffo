import ast
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from src.app_version import get_app_version
from tools.repo_health.policy_wiring_policy import registered_policy_checks

# Release notes are the only changelog surface a user is expected to read. These
# budgets stop a release section drifting back into a work log. Calibrated from
# the two real sections: 0.2.152 measured 4,131 words with a 7,054-character
# bullet, and 0.3.0 measured 1,143 words with a 773-character maximum, so the
# thresholds clear the good one with real headroom and reject the bad one.
RELEASE_NOTE_WORD_BUDGET = 2000
RELEASE_NOTE_BULLET_CHAR_BUDGET = 1200

# docs/CHANGELOG.md is a release log for the versions someone may install, not a
# history of every bump. `git log` is the source of truth for older entries.
CHANGELOG_MAX_RELEASED_SECTIONS = 5

# Advisory only. A jargon blocklist cannot be made false-positive-free, and a
# gate that is always red gets ignored -- the same failure mode that let the
# broken Playwright cache check look healthy. These patterns are the concrete
# "implementation detail leaked into release notes" smell; a human decides.
_ADVISORY_INTERNAL_LEAK_PATTERNS = (
    (r"`[^`]*\.(?:py|mjs|cjs|js|ts|css|yml|yaml|json|gz)`", "code filename"),
    (r"`(?:src|tools|tests|frontend|docs|scripts|deathuman-baluffo)/", "repo path"),
    (r"static_source::", "loader id"),
    (r"\b__[a-z]+__\b", "dunder attribute"),
)

# Plans are temporary ledgers: refine before execution, track to completion, then
# delete them and let the durable lessons land in the regular docs (see
# DOCS_WORKFLOW.md:46 -- git history is the provenance, not archive/). Three
# programmes of ~1,000 lines each accumulated because a status line can go stale
# while the body moves on, and a reader who trusts the line skips the body.
#
# PLAN_LIFECYCLE_ENFORCE started False on purpose. Twelve plans tripped these rules
# that day, so enforcing immediately meant a permanently red gate, and a permanently
# red gate is one people learn to ignore. It went True on 2026-10-01 once the drain
# reached zero, so it now blocks on a clean list rather than on a red one.
#
# It blocks on something new and deliberate. A finished plan, or a deferred plan
# that never says what unblocks it, fails the gate; a deferred plan that names a
# real trigger passes. That is the intended pressure -- "Parked" with no stated
# reason to wait is indistinguishable from abandoned, so the doc has to answer.
PLAN_LIFECYCLE_ENFORCE = True

# A tripwire to review, not a hard cap. The retired audit programme measured 1,027
# lines and still carried live method value, so the threshold prompts a look
# rather than forcing a split of a doc that does not need one.
PLAN_LINE_TRIPWIRE = 600

# A line count cannot see the worst offender. jobs-coverage-improvement-plan held
# 21,816 words in 65 lines -- 172,891 characters on a single line, 97% of the
# file. It passed every line-based rule while being the clearest violation in the
# directory, so word count is checked too.
PLAN_WORD_TRIPWIRE = 5000

# "Code work is complete and everything in this plan has landed" opens with a
# subject, not a state, so the anchored terminal pattern alone would miss it. These
# openers are how finished work actually gets described in this repo's plans.
PLAN_TERMINAL_SUBJECT = re.compile(
    r"^(?:\W*)(?:code\s+work\s+is\s+complete\b|everything\b[^.]*\b(?:landed|complete)\b)",
    re.IGNORECASE,
)

# A terminal status means "this plan's work is finished; delete it". Deferral is
# NOT terminal. optional-playwright-browser-download reads "Parked -- deferred until
# the next desktop portable release" and must stay, because that trigger is real
# and checkable. task-abort-control reads "Implemented baseline, refinement-ready"
# and must stay too, because its acceptance criteria are the next step. Flagging
# either would be a false positive in the enforcing phase, and a guardrail that
# condemns correct behaviour is one people disable.
#
# So a status is terminal only when it reads as work-complete AND names no
# pending condition. The two tests are separate on purpose.
#
# ANCHORED, which is the whole mechanism. Real statuses run long and mention
# completed work mid-sentence while the plan itself is active -- art-title reads
# "Active follow-up plan, parser/redirect/company hardening implemented and
# validated on 2026-05-30", and one retired plan carried 172,891 characters on a
# single status line. A pattern that reads only the opening phrase rejects both
# without needing a length window or an explicit "active" override; an earlier
# draft of this check had both and they were dead code.
PLAN_TERMINAL_STATUS = re.compile(
    r"^(?:\W*)(?:folded|closed|superseded|complete|completed|fully\s+executed|"
    r"landed|finished|all\b[^.]*\blanded)\b",
    re.IGNORECASE,
)

# "Parked" and "Implemented" open a deferred plan just as often as a finished one.
# They are only terminal when nothing is pending, so they are judged by the pending
# test below rather than by this pattern.
PLAN_DEFERRED_STATUS = re.compile(
    r"^(?:\W*)(?:parked|deferred|implemented|refinement-ready)\b",
    re.IGNORECASE,
)

# A deferred plan must say what unblocks it. Real cases: "until the next desktop
# portable release", "refinement-ready", "before the next desktop runtime effort".
# Requiring the trigger is what keeps "Parked" from becoming the new "Complete" --
# an unstated reason to wait is indistinguishable from an abandoned plan.
#
# Every term here is a CONDITION or a CRITERION, never a synonym for the state
# itself. An earlier draft listed "deferred", which made "Parked; deferred
# experiment" self-satisfying: the word describing the state also satisfied the
# word justifying it, so a plan with no actual trigger passed.
PLAN_PENDING_TRIGGER = re.compile(
    r"(?:\buntil\b|\bwhen\b|\bbefore\b|\bafter\b|\bonce\b|\brevisit\b|\breturns?\b|"
    r"\bnext\s+\w|\bif\b|\bready\b|\bpending\b|\bremain(?:s|ing)?\b|\bstill\b|"
    r"\bretrigger\b|\bacceptance\s+criteria\b|\bcriteria\b|\bshould\b)",
    re.IGNORECASE,
)

# A trigger with a date in it can EXPIRE, and a past date stops being a reason to
# wait. reliable-job-availability said "Remaining: bounded monitoring window
# through ~2026-08-31 ... then archive this plan" and sat un-flagged for a month,
# because "Remaining" satisfied the trigger test. So a status naming only dates
# that have passed is not a live trigger -- the deferral has lapsed and the plan
# owes a decision.
_PLAN_STATUS_DATE = re.compile(r"(20\d\d)-(\d\d)-(\d\d)")

# "through ~2026-08-31" is an end date; "since 2026-05-30" is a start date and does
# not expire. Only treat a date as a deadline when it is introduced as one.
_PLAN_STATUS_DEADLINE = re.compile(
    r"\b(?:through|until|by|before|ends?|expires?|closes?|deadline)\b[^.;]{0,40}?"
    r"(20\d\d)-(\d\d)-(\d\d)",
    re.IGNORECASE,
)


def _plan_status_deadlines(status: str) -> list[date]:
    """Every deadline the status names, in the form 'through <date>' and friends."""
    found: list[date] = []
    for match in _PLAN_STATUS_DEADLINE.finditer(status):
        try:
            found.append(date(int(match.group(1)), int(match.group(2)), int(match.group(3))))
        except ValueError:  # pragma: no cover - malformed date in a status line
            continue
    return found


def _plan_status_deadlines_passed(status: str, *, today: date) -> bool:
    """True when the status names a deadline that has already gone by."""
    return any(deadline < today for deadline in _plan_status_deadlines(status))


# Longest status prefix quoted in a finding. Display only -- matching is the
# anchored pattern's job, not this slice's.
PLAN_STATUS_QUOTE_CHARS = 60

# refactor-charter-template.md is a template for authoring plans, not a plan. It
# has no status line and no execution history, so lifecycle rules do not apply.
PLAN_LIFECYCLE_EXEMPT = frozenset({"refactor-charter-template.md"})


def _plan_policy_today() -> date:
    """Today, isolated so tests can pin it instead of racing the calendar."""
    return _PLAN_TODAY_OVERRIDE or date.today()


# Set by the tests to make the expiry rule deterministic. A check that only passes
# while the calendar happens to cooperate is a check that will surprise someone.
_PLAN_TODAY_OVERRIDE: date | None = None


def _plan_lifecycle_findings(plans_dir: Path) -> list[str]:
    findings: list[str] = []
    for path in sorted(plans_dir.glob("*.md")):
        if path.name in PLAN_LIFECYCLE_EXEMPT:
            continue
        text = path.read_text(encoding="utf-8")
        lines = len(text.splitlines())
        words = len(text.split())
        if lines > PLAN_LINE_TRIPWIRE:
            findings.append(
                f"{path.name}: {lines} lines (over the {PLAN_LINE_TRIPWIRE}-line "
                f"tripwire) -- review whether the open work still justifies it"
            )
        if words > PLAN_WORD_TRIPWIRE:
            findings.append(
                f"{path.name}: {words} words (over the {PLAN_WORD_TRIPWIRE}-word "
                f"tripwire) -- a line count misses this shape"
            )
        status = next(
            (
                line.partition("**Status:**")[2].strip()
                for line in text.splitlines()[:12]
                if "**Status:**" in line
            ),
            "",
        )
        if not status:
            continue
        # Deferral is legitimate when the status says what unblocks it. Judge that
        # before the terminal test, or "Parked -- deferred until ..." is condemned
        # for the very thing that makes it keepable.
        if PLAN_DEFERRED_STATUS.match(status) or _plan_status_deadlines(status):
            today = _plan_policy_today()
            deadlines = _plan_status_deadlines(status)
            if deadlines and all(deadline < today for deadline in deadlines):
                findings.append(
                    f"{path.name}: deferral has lapsed -- the status names a deadline "
                    f"that has passed ({status[:PLAN_STATUS_QUOTE_CHARS].strip()!r}). "
                    f"Do the deferred action or delete the plan"
                )
            elif deadlines or PLAN_PENDING_TRIGGER.search(status):
                # A named future deadline is itself the trigger. "through 2026-12-01"
                # needs no keyword to be checkable, so requiring a trigger word here
                # would wrongly call it unscoped.
                continue
            else:
                findings.append(
                    f"{path.name}: deferred ({status[:PLAN_STATUS_QUOTE_CHARS].strip()!r}) "
                    f"without naming what unblocks it -- state the trigger, or delete it"
                )
            continue
        if PLAN_TERMINAL_STATUS.match(status) or PLAN_TERMINAL_SUBJECT.match(status):
            findings.append(
                f"{path.name}: status is terminal ({status[:PLAN_STATUS_QUOTE_CHARS].strip()!r}) "
                f"while still in plans/ -- delete it and absorb the lessons"
            )
    return findings


def test_plan_lifecycle_tripwires(repo_root: Path) -> None:
    """Warn on plans that outlived their purpose. Warn-only until the list drains."""
    findings = _plan_lifecycle_findings(repo_root / "docs" / "plans")
    if not findings:
        return
    message = "plan lifecycle tripwires:\n" + "\n".join(f"  - {f}" for f in findings)
    if PLAN_LIFECYCLE_ENFORCE:
        raise AssertionError(message)
    print(f"[warn] {message}")


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    next_heading = text.find("\n## ", start + len(heading))
    return text[start:] if next_heading == -1 else text[start:next_heading]


def test_release_guide_is_canonical_single_source(repo_root: Path) -> None:
    """Ensure `docs/RELEASE.md` remains the canonical release guide."""
    release_path = repo_root / "docs" / "RELEASE.md"
    assert release_path.is_file(), "docs/RELEASE.md must exist as the canonical release guide."
    text = release_path.read_text(encoding="utf-8").strip()
    assert len(text.splitlines()) > 0, "docs/RELEASE.md should not be empty."


def test_release_notes_artifact_matches_current_version(repo_root: Path) -> None:
    """Require the tracked `release-notes.md` artifact to be regenerated on release bumps."""
    release_notes_path = repo_root / "release-notes.md"
    assert release_notes_path.is_file(), (
        "release-notes.md must exist at the repo root; regenerate it with "
        "`python scripts/extract_release_notes.py`."
    )
    release_notes_text = release_notes_path.read_text(encoding="utf-8")
    first_line = release_notes_text.splitlines()[0].strip()
    app_version = get_app_version()
    assert first_line.startswith(f"## [{app_version}]"), (
        f"release-notes.md is stale: its top heading is {first_line!r} but the current app "
        f"version is {app_version!r}. Release commits must regenerate it with "
        "`python scripts/extract_release_notes.py` so the tracked artifact matches the "
        "versioned changelog section."
    )
    assert "## [Unreleased]" not in release_notes_text, (
        "release-notes.md must hold a versioned section, not the Unreleased block; regenerate "
        "it with `python scripts/extract_release_notes.py`."
    )


def test_changelog_keeps_unreleased_above_versioned_rollup(repo_root: Path) -> None:
    """Require `docs/CHANGELOG.md` to keep an `[Unreleased]` section above the versioned rollup."""
    changelog_path = repo_root / "docs" / "CHANGELOG.md"
    assert changelog_path.is_file(), "docs/CHANGELOG.md must exist."
    changelog_text = changelog_path.read_text(encoding="utf-8")
    app_version = get_app_version()

    unreleased_pos = changelog_text.find("## [Unreleased]")
    assert unreleased_pos != -1, (
        "docs/CHANGELOG.md is missing the `## [Unreleased]` section. Keep it directly above "
        f"`## [{app_version}]` so in-progress changes accumulate at the top; release commits "
        "roll it up into a new versioned section and reopen an empty Unreleased block."
    )

    versioned_pos = changelog_text.find(f"## [{app_version}]")
    assert versioned_pos != -1, (
        f"docs/CHANGELOG.md has no `## [{app_version}]` section for the current app version."
    )

    assert unreleased_pos < versioned_pos, (
        "docs/CHANGELOG.md's `## [Unreleased]` section must sit above the versioned rollup "
        f"`## [{app_version}]` (Keep a Changelog convention). Move the Unreleased heading above "
        "the versioned sections so in-progress changes accumulate at the top of the file."
    )


def _changelog_sections(changelog_text: str) -> list[tuple[str, str]]:
    """Split the `## [version]` blocks into ``(version, body)`` pairs, newest first."""
    starts = [m.start() for m in re.finditer(r"^## \[", changelog_text, re.MULTILINE)]
    out: list[tuple[str, str]] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(changelog_text)
        body = changelog_text[start:end]
        out.append((body[len("## [") : body.index("]")], body))
    return out


def _declares_rollup(body: str) -> bool:
    """Whether a section announces itself as a rollup restating earlier releases.

    `0.3.001` legitimately repeats `0.3.0`'s bullets so users upgrading from 0.2.x
    see the whole 0.3 line, and says so in the blockquote under its heading. An
    accidental restatement carries no such declaration, and that is the difference
    this test turns on.
    """
    return "rollup" in body.split("###", 1)[0].lower()


def test_changelog_clamps_released_sections(repo_root: Path) -> None:
    """Keep `docs/CHANGELOG.md` a short release log instead of a version ledger.

    It once held 153 release sections, which made restating entries on every bump
    the only way to stay current and left nobody reading it. `git log` keeps that
    history, so the file is clamped instead.
    """
    changelog_text = (repo_root / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")
    versions = [version for version, _ in _changelog_sections(changelog_text)]
    released = [version for version in versions if version != "Unreleased"]

    assert released, "docs/CHANGELOG.md must keep at least one released version section."
    assert "Unreleased" in versions, "docs/CHANGELOG.md is missing its `## [Unreleased]` section."
    assert len(released) <= CHANGELOG_MAX_RELEASED_SECTIONS, (
        f"docs/CHANGELOG.md holds {len(released)} released sections "
        f"({', '.join(released)}); the clamp is {CHANGELOG_MAX_RELEASED_SECTIONS}. Drop "
        "the oldest -- `git log` keeps that history -- and keep the clamp described in "
        "docs/DOCS_WORKFLOW.md so nobody adds them back."
    )


@dataclass(frozen=True)
class ChangelogRule:
    """One rule of the changelog's shape and the phrase that names it in each list.

    ``workflow_row`` and ``changelog_bullet`` are fragments rather than whole sentences, so
    the prose around them can still be rewritten. Matching by phrase is what lets this audit
    tell "the lists describe these rules" from "the lists happen to have this many bullets".
    """

    key: str
    guardrail: str
    workflow_row: str
    changelog_bullet: str


# The changelog's rules are stated three times -- CHANGELOG.md's bullet list, DOCS_WORKFLOW.md's
# Rule|Guardrail table (the only place that names a guardrail), and RELEASE.md's release-commit
# procedure -- and enforced by the checks in this module. Two of those statements are lists, and
# a list is exactly the kind of thing that goes stale in one place: 0.3.016's section sat in the
# file claiming a release the tag list disagreed with, and the guardrail written to catch that had
# no bullet and no table row. Nothing failed, because nothing compared the lists to the
# registration.
#
# So this table is the join. Each row names a rule, the check that enforces it, and the phrase
# that identifies it in each list;
# `test_changelog_rule_lists_match_registered_guardrails` fails when the two sides disagree in
# either direction.
CHANGELOG_RULES: tuple[ChangelogRule, ...] = (
    ChangelogRule(
        key="max_released_sections",
        guardrail="test_changelog_clamps_released_sections",
        workflow_row="released sections, plus `[Unreleased]`",
        changelog_bullet="plus `[Unreleased]`",
    ),
    ChangelogRule(
        key="one_claim_per_change",
        guardrail="test_no_change_is_claimed_by_two_release_sections",
        workflow_row="A change is described by exactly one release section",
        changelog_bullet="No bullet appears in two sections",
    ),
    ChangelogRule(
        key="no_unshipped_section_above_the_tag",
        guardrail="test_changelog_holds_no_unpublished_section_above_the_last_release_tag",
        workflow_row="No versioned section without a release tag",
        changelog_bullet="never shipped may not queue above the last release tag",
    ),
    ChangelogRule(
        key="unreleased_above_the_rollup",
        guardrail="test_changelog_keeps_unreleased_above_versioned_rollup",
        workflow_row="sits directly above the versioned rollup",
        changelog_bullet="sits above the versioned sections",
    ),
)

# Checks that read docs/CHANGELOG.md without enforcing one of the rules above. Every entry
# states where the work it does *is* described instead, because each of these is registered and
# can fail a release: an exemption with no stated home for the rule is how a changelog guardrail
# leaves the lists unnoticed, which is the failure this audit exists to catch. A stale entry
# fails too -- the check it names must still read the changelog.
_CHANGELOG_READERS_WITHOUT_A_LISTED_RULE = {
    "test_top_release_notes_stay_user_readable": (
        "the shipped section's shape budget and leak advisory, stated in CHANGELOG.md's intro "
        "paragraph and docs/RELEASE.md; the audit compares both limits to the constants"
    ),
    "test_release_docs_cover_the_current_public_release_line": (
        "the released section's required content -- its heading, a Keep-a-Changelog "
        "subsection, a bullet, and the six compatibility phrases -- part of the release "
        "procedure docs/RELEASE.md owns"
    ),
    "test_docs_avoid_stale_archive_and_generated_artifact_links": (
        "docs-tree link hygiene, which reads the changelog only to reject archive links in it"
    ),
    "test_changelog_rule_lists_match_registered_guardrails": (
        "this audit itself: it reads the rule lists rather than the log"
    ),
}

# Every place a doc states one of the changelog's limits. The regex captures the number the doc
# advertises; the audit compares it to the constant its guardrail enforces, so the two cannot
# drift apart, and a site that stops matching fails as well -- deleting the sentence that tells a
# release owner the budget is how the budget stops being known. Numbers are compared with every
# match, not the first, so a doc holding two different numbers fails instead of passing on one.
_CHANGELOG_LIMIT_CLAIMS: tuple[tuple[str, str, int], ...] = (
    ("docs/CHANGELOG.md", r"At most (\d+) versioned sections", CHANGELOG_MAX_RELEASED_SECTIONS),
    ("docs/CHANGELOG.md", r"at most\s+([\d,]+)\s+words", RELEASE_NOTE_WORD_BUDGET),
    (
        "docs/CHANGELOG.md",
        r"bullet over\s+([\d,]+)\s+characters",
        RELEASE_NOTE_BULLET_CHAR_BUDGET,
    ),
    (
        "docs/DOCS_WORKFLOW.md",
        r"most recent (\d+) released versions",
        CHANGELOG_MAX_RELEASED_SECTIONS,
    ),
    (
        "docs/DOCS_WORKFLOW.md",
        r"At most (\d+) released sections",
        CHANGELOG_MAX_RELEASED_SECTIONS,
    ),
    ("docs/RELEASE.md", r"newest (\d+) released versions", CHANGELOG_MAX_RELEASED_SECTIONS),
    ("docs/RELEASE.md", r"at most\s+([\d,]+)\s+words", RELEASE_NOTE_WORD_BUDGET),
    ("docs/RELEASE.md", r"bullet over\s+([\d,]+)\s+characters", RELEASE_NOTE_BULLET_CHAR_BUDGET),
)

_NUMBER_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}

# DOCS_WORKFLOW.md explains two of its rules by position ("The second rule has one deliberate
# exception"), so a probe that inserts, removes, or reorders a row can leave a paragraph pointing
# at a rule that no longer exists. Only words an ordinal reference can use are checked; "the next
# rule" and friends are prose, not positions.
_ORDINAL_WORDS = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "fifth": 5,
    "sixth": 6,
    "seventh": 7,
    "eighth": 8,
    "ninth": 9,
    "tenth": 10,
}


def _rule_count_sentence(text: str, phrase: str, source: str) -> re.Match[str]:
    """The sentence in which a doc says how many rules its list holds."""
    match = re.search(rf"(\w+) rules? {phrase}", text)
    assert match is not None, (
        f"{source} no longer says how many changelog rules the guardrail enforces (expected a "
        f"sentence matching `{phrase}`). The rule lists are audited against the guardrails "
        "registered for them, so the count has to be stated."
    )
    return match


def _rule_count(match: re.Match[str], source: str) -> int:
    token = match.group(1).lower()
    if token.isdigit():
        return int(token)
    assert token in _NUMBER_WORDS, (
        f"{source} states its rule count as {token!r}, which this audit cannot read. Write it "
        'as a number word ("Four rules") or a digit.'
    )
    return _NUMBER_WORDS[token]


def _changelog_rule_bullets(text: str, start: int) -> list[str]:
    """The rule list's `- ` bullets after ``start``, each bullet's continuations joined."""
    bullets: list[str] = []
    current: list[str] = []

    def flush() -> None:
        if current:
            bullets.append(" ".join(current))
            current.clear()

    for line in text[start:].splitlines():
        stripped = line.strip()
        if line.startswith("- "):
            flush()
            current.append(line[2:].strip())
        elif current and line[:1] in {" ", "\t"} and stripped:
            current.append(stripped)
        else:
            flush()
            if bullets and (not stripped or stripped == "---" or stripped.startswith("## ")):
                break
    flush()
    return bullets


def _changelog_rule_rows(text: str, start: int) -> list[tuple[str, str]]:
    """The `| Rule | Guardrail |` rows after ``start``, as ``(rule, guardrail)`` cells."""
    rows: list[tuple[str, str]] = []
    for line in text[start:].splitlines():
        stripped = line.strip()
        if stripped.startswith("|"):
            cells = [cell.strip() for cell in stripped.strip("|").split("|")]
            if len(cells) == 2 and re.search(r"`test_\w+`", cells[1]):
                rows.append((cells[0], cells[1]))
            continue
        if rows and stripped:
            break
    return rows


def _policy_checks(module_path: Path) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    """Every top-level ``test_*`` in a policy module, by name, read from its source.

    Read from ``repo_root`` rather than from this module's namespace for the same reason
    ``container_version_policy`` reads the version from a git revision: the audit judges the
    tree it was handed, and its tests hand it throwaway trees.
    """
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    }


def _reads_the_changelog(function: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Whether a check opens ``docs/CHANGELOG.md``, judged by the path it builds.

    A string mention would be far too broad: several checks assert that *other* docs do not
    link `docs/CHANGELOG.md`, so a text search reads them as changelog guardrails. The path
    join is what says the file is opened.
    """
    for node in ast.walk(function):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            for operand in (node.left, node.right):
                if isinstance(operand, ast.Constant) and operand.value == "CHANGELOG.md":
                    return True
    return False


def test_changelog_rule_lists_match_registered_guardrails(repo_root: Path) -> None:
    """The rules the docs describe and the guardrails registered for them must agree.

    docs/CHANGELOG.md states the changelog's rules as a bullet list and docs/DOCS_WORKFLOW.md as
    a Rule|Guardrail table;    docs/RELEASE.md repeats the limits in the release-commit procedure and names the guardrails
    that procedure relies on. All three are claims about code, and a claim is only worth reading
    if something fails when it stops being true. That last guardrail proved the point: it shipped
    with no bullet and no row, and only a hand audit noticed.

    So this check reads both lists, the release guide's limit sentences and named guardrails, and
    the guardrail groups in ``repo_guardrails.py``, and fails on any disagreement -- a rule with no
    registered guardrail, a guardrail a group never runs, a documented number the enforcement does
    not use, a release-guide name no group runs, a list whose length disagrees with the others, or
    a registered check that reads the changelog without appearing in either list or in the
    exemptions above.
    """
    policy_path = repo_root / "tools" / "repo_health" / "release_docs_policy.py"
    changelog_text = (repo_root / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")
    workflow_text = (repo_root / "docs" / "DOCS_WORKFLOW.md").read_text(encoding="utf-8")
    release_text = (repo_root / "docs" / "RELEASE.md").read_text(encoding="utf-8")
    doc_texts = {
        "docs/CHANGELOG.md": changelog_text,
        "docs/DOCS_WORKFLOW.md": workflow_text,
        "docs/RELEASE.md": release_text,
    }

    declared = len(CHANGELOG_RULES)
    changelog_sentence = _rule_count_sentence(
        changelog_text, "the guardrail enforces", "docs/CHANGELOG.md"
    )
    workflow_sentence = _rule_count_sentence(
        workflow_text, "keep this from quietly regrowing", "docs/DOCS_WORKFLOW.md"
    )
    changelog_claim = _rule_count(changelog_sentence, "docs/CHANGELOG.md")
    workflow_claim = _rule_count(workflow_sentence, "docs/DOCS_WORKFLOW.md")

    assert changelog_claim == workflow_claim == declared, (
        "The changelog's rule lists disagree about how many rules there are: "
        f"docs/CHANGELOG.md says {changelog_claim}, docs/DOCS_WORKFLOW.md says {workflow_claim}, "
        f"and CHANGELOG_RULES declares {declared}. Move the bullets, the table rows, and this "
        "module's table together, or drop the rule the lists no longer describe."
    )

    bullets = _changelog_rule_bullets(changelog_text, changelog_sentence.end())
    rows = _changelog_rule_rows(workflow_text, workflow_sentence.end())
    assert len(bullets) == declared, (
        f"docs/CHANGELOG.md's rule list holds {len(bullets)} bullets but claims {declared}: "
        + ", ".join(repr(bullet[:60]) for bullet in bullets)
    )
    assert len(rows) == declared, (
        f"docs/DOCS_WORKFLOW.md's rule table holds {len(rows)} rows but claims {declared}."
    )

    unlisted_rows = [
        row[0] for row in rows if not any(rule.workflow_row in row[0] for rule in CHANGELOG_RULES)
    ]
    assert not unlisted_rows, (
        "docs/DOCS_WORKFLOW.md documents rules that CHANGELOG_RULES does not declare: "
        + "; ".join(unlisted_rows)
        + ". Add a ChangelogRule naming the guardrail that enforces it, or delete the row."
    )
    unlisted_bullets = [
        bullet[:70]
        for bullet in bullets
        if not any(rule.changelog_bullet in bullet for rule in CHANGELOG_RULES)
    ]
    assert not unlisted_bullets, (
        "docs/CHANGELOG.md states rules that CHANGELOG_RULES does not declare: "
        + "; ".join(unlisted_bullets)
        + ". Add a ChangelogRule naming the guardrail that enforces it, or delete the bullet."
    )
    for rule in CHANGELOG_RULES:
        assert sum(rule.workflow_row in row[0] for row in rows) == 1, (
            f"the phrase `{rule.workflow_row}` identifies the {rule.key} rule in "
            "docs/DOCS_WORKFLOW.md's table, but it appears in "
            f"{sum(rule.workflow_row in row[0] for row in rows)} rows. Make each rule's phrase "
            "unique so a row can be matched to a rule."
        )
        assert sum(rule.changelog_bullet in bullet for bullet in bullets) == 1, (
            f"the phrase `{rule.changelog_bullet}` identifies the {rule.key} rule in "
            "docs/CHANGELOG.md's list, but it appears in "
            f"{sum(rule.changelog_bullet in bullet for bullet in bullets)} bullets. Make each "
            "rule's phrase unique so a bullet can be matched to a rule."
        )

    documented = {name for _, cell in rows for name in re.findall(r"`(test_\w+)`", cell)}
    declared_guardrails = {rule.guardrail for rule in CHANGELOG_RULES}
    assert documented == declared_guardrails, (
        "The rule lists name guardrails CHANGELOG_RULES does not: "
        + ", ".join(sorted(documented - declared_guardrails))
        + "; and CHANGELOG_RULES names guardrails the lists do not: "
        + ", ".join(sorted(declared_guardrails - documented))
        + ". The table in docs/DOCS_WORKFLOW.md and this module's table must name the same "
        "checks."
    )

    checks = _policy_checks(policy_path)
    registered = registered_policy_checks(repo_root).get("release_docs_policy", set())
    unregistered = sorted(name for name in declared_guardrails if name not in registered)
    assert not unregistered, (
        "The rule lists name guardrails no run_*_group registers, so they never run: "
        + ", ".join(unregistered)
        + ". Register them in tools/repo_health/repo_guardrails.py or remove the claim that "
        "they enforce a rule."
    )
    undefined = sorted(name for name in declared_guardrails if name not in checks)
    assert not undefined, (
        "The rule lists name guardrails release_docs_policy.py does not define: "
        + ", ".join(undefined)
        + ". A renamed check leaves the lists claiming enforcement nobody performs."
    )
    decorative = sorted(
        name for name in declared_guardrails if not _reads_the_changelog(checks[name])
    )
    assert not decorative, (
        "CHANGELOG_RULES pairs a rule with a check that never opens docs/CHANGELOG.md: "
        + ", ".join(decorative)
        + ". A guardrail that does not read the document cannot enforce a rule about it."
    )

    # docs/RELEASE.md names the guardrails its release-commit procedure relies on, and three of
    # the names are changelog rules. Those names are claims like any other: a release guide can
    # point at a check no group runs. This audit owns the policy modules, so it judges the names
    # that live in one; a name the repo defines as a pytest test elsewhere is out of its reach,
    # because suite_contract_policy owns test discovery.
    named_guardrails = set(re.findall(r"`(test_\w+)`", release_text))
    health = repo_root / "tools" / "repo_health"
    defined_anywhere = {
        name for module in sorted(health.glob("*_policy.py")) for name in _policy_checks(module)
    }
    registered_anywhere = {
        name for names in registered_policy_checks(repo_root).values() for name in names
    }
    dangling = sorted(
        name
        for name in named_guardrails
        if name in defined_anywhere and name not in registered_anywhere
    )
    assert not dangling, (
        "docs/RELEASE.md names policy guardrails that no run_*_group registers, so the release "
        "procedure describes enforcement that never runs: "
        + ", ".join(dangling)
        + ". Register them in tools/repo_health/repo_guardrails.py or drop the reference."
    )

    readers = {
        name
        for name, function in checks.items()
        if name in registered and _reads_the_changelog(function)
    }
    exemptions = _CHANGELOG_READERS_WITHOUT_A_LISTED_RULE
    undocumented = sorted(readers - declared_guardrails - set(exemptions))
    assert not undocumented, (
        "These registered checks read docs/CHANGELOG.md but appear in neither rule list: "
        + ", ".join(undocumented)
        + ". A changelog guardrail nobody documented is one an editor cannot know about: add a "
        "bullet to docs/CHANGELOG.md, a row to docs/DOCS_WORKFLOW.md, a ChangelogRule here, or "
        "an exemption stating where its rule is documented instead."
    )
    stale = sorted(name for name in exemptions if name not in readers)
    assert not stale, (
        "_CHANGELOG_READERS_WITHOUT_A_LISTED_RULE exempts checks that do not read the "
        "changelog (or are gone or unregistered): "
        + ", ".join(stale)
        + ". Drop the exemption; a stale one hides a real changelog guardrail from this audit."
    )
    unexplained = sorted(name for name, reason in exemptions.items() if not reason.strip())
    assert not unexplained, (
        "An exemption must state where the rule it enforces is documented instead: "
        + ", ".join(unexplained)
    )

    drifted: list[str] = []
    for rel_path, pattern, expected in _CHANGELOG_LIMIT_CLAIMS:
        found = [int(value.replace(",", "")) for value in re.findall(pattern, doc_texts[rel_path])]
        if not found:
            drifted.append(f"{rel_path} no longer states the limit `{pattern}`")
        drifted.extend(
            f"{rel_path} states {value} where {expected} is enforced"
            for value in found
            if value != expected
        )
    assert not drifted, (
        "The changelog's limits are described differently in the docs and the guardrails: "
        + "; ".join(drifted)
        + ". A documented budget the enforcement does not use teaches the wrong number."
    )

    dangling_ordinals = sorted(
        {
            token
            for token in re.findall(r"[Tt]he (\w+) rule\b", workflow_text)
            if token.lower() in _ORDINAL_WORDS and _ORDINAL_WORDS[token.lower()] > declared
        }
    )
    assert not dangling_ordinals, (
        "docs/DOCS_WORKFLOW.md explains a rule by position that the list no longer reaches: "
        + ", ".join(f"`the {token} rule`" for token in dangling_ordinals)
        + f". The table holds {declared} rules, so those paragraphs point at nothing."
    )


# A versioned section is a claim that that version ships. One may await its tag -- that is
# every day between a bump and its release -- but an older one may not.
_VERSIONED_SECTION_RE = re.compile(r"^\d+(?:\.\d+)+$")


def _repo_declared_version(repo_root: Path) -> str:
    """The version the repo under test declares, read from that repo's own files.

    Read from ``repo_root`` rather than the imported ``get_app_version()`` for the same
    reason ``container_version_policy`` reads it from a git revision: this check judges the
    tree it was handed, and its tests build throwaway trees with their own versions.
    """
    app_version_path = repo_root / "src" / "app_version.py"
    if app_version_path.is_file():
        match = re.search(r'APP_VERSION\s*=\s*"([^"]+)"', app_version_path.read_text("utf-8"))
        if match:
            return match.group(1).strip()
    return get_app_version()


def _release_tags(repo_root: Path) -> set[str]:
    """Every local ``v*`` tag, or an empty set when git cannot answer.

    Offline and deterministic on purpose, exactly like the container version gate: this
    runs in pre-commit and pre-push, where a network call would add round-trips and fail
    without a registry. The tag is the release marker, so a version that reached a registry
    without ever being tagged reads as unpublished -- the inert window, since nothing can
    hold the version string. A checkout holding no tags at all (shallow, or fetched with
    ``--no-tags``) yields an empty set, and the check skips rather than reading every
    section as unpublished and blocking commits it cannot judge.
    """
    completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
        ["git", "-C", str(repo_root), "tag", "-l", "v*"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return set()
    return {line.strip() for line in completed.stdout.splitlines() if line.strip()}


def _unjudgeable_checkout_reason(repo_root: Path) -> str | None:
    """Why this checkout cannot answer the changelog's shipping state, or None.

    Two shapes make the tag list untrustworthy, and both are ordinary in automation: a
    shallow clone (`actions/checkout` defaults to fetch-depth 1) holds only the tags it was
    asked for, and a clone fetched with `--no-tags` holds none. Reading either as "every
    section is unpublished" would block the commits that ship a release, so the check
    declines to judge instead of guessing. A developer's full clone is the place this has
    to bite, because that is where the release decision is made.
    """
    shallow = subprocess.run(  # noqa: S603 - fixed argv, no shell
        ["git", "-C", str(repo_root), "rev-parse", "--is-shallow-repository"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if shallow.returncode == 0 and shallow.stdout.strip() == "true":
        return "this checkout is shallow, so it does not hold the release tags"
    if not _release_tags(repo_root):
        return "this checkout holds no v* tags"
    return None


def test_changelog_holds_no_unpublished_section_above_the_last_release_tag(
    repo_root: Path,
) -> None:
    """One versioned section may await its tag; no older one may.

    0.3.016 was bumped, never tagged, and then skipped by 0.3.017. Both sections sat in the
    file as claims that their versions shipped, so the notes generated for the next tag
    described one changeset while the other's two fixes were readable only inside the
    changelog. docs/RELEASE.md's cross-channel checkpoint asks the release owner to decide
    what a release contains when sections queue up above the last published tag, and nothing
    failed when nobody decided.

    Between a bump and its release the version in flight legitimately has no tag, so it is
    allowed to queue. Any *other* versioned section without a tag is a release that will not
    ship, and its changes are owed either to the next release or to deletion.
    """
    changelog_path = repo_root / "docs" / "CHANGELOG.md"
    assert changelog_path.is_file(), "docs/CHANGELOG.md must exist."

    tags = _release_tags(repo_root)
    reason = _unjudgeable_checkout_reason(repo_root)
    if reason:
        print(
            f"  [skipped] {reason}, so the changelog's shipping state cannot be judged "
            "offline; fetch full history and tags to enable this check."
        )
        return

    current_version = _repo_declared_version(repo_root)
    unpublished = [
        version
        for version, _ in _changelog_sections(changelog_path.read_text(encoding="utf-8"))
        if _VERSIONED_SECTION_RE.match(version)
        and version != current_version
        and f"v{version}" not in tags
    ]
    assert not unpublished, (
        "docs/CHANGELOG.md holds versioned section(s) with no release tag: "
        + ", ".join(unpublished)
        + f". {current_version} is the version in flight, and it may queue without its tag; an "
        "older section describes a release that never shipped, so the notes generated for the "
        "next tag omit it -- the rollup decision docs/RELEASE.md's cross-channel checkpoint "
        "asks the release owner to make. Decide it, then either roll those changes into "
        f"`## [{current_version}]` or delete the stale section (`git log` keeps that history). "
        "If that version is about to ship instead, release it before bumping past it."
    )


def test_no_change_is_claimed_by_two_release_sections(repo_root: Path) -> None:
    """One change belongs to one release section, unless a declared rollup says otherwise."""
    changelog_text = (repo_root / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")
    claims: dict[str, list[str]] = {}
    rollups: set[str] = set()

    for version, body in _changelog_sections(changelog_text):
        if _declares_rollup(body):
            rollups.add(version)
        for line in body.splitlines():
            stripped = line.strip()
            if stripped.startswith("- "):
                claims.setdefault(stripped, []).append(version)

    offenders = [
        f"      {bullet[:88]} -> {', '.join(versions)}"
        for bullet, versions in claims.items()
        if len(versions) > 1 and not (set(versions) & rollups)
    ]
    assert not offenders, (
        "The same change is described in more than one release section, so each of "
        "those releases appears to ship it:\n"
        + "\n".join(offenders)
        + "\n    Keep the wording in the earliest section that claims it and delete the "
        "later copy. If the later section is a deliberate rollup for users skipping "
        "releases, declare that in a blockquote under its heading (0.3.001 does)."
    )


def test_top_release_notes_stay_user_readable(repo_root: Path) -> None:
    """The shipped release section must read as release notes, not as a work log.

    Scoped to the TOP versioned section only. History is never scanned: older
    sections record what was actually written at the time, and rewriting them is
    not this gate's business. The 0.2.152 section is the counterexample this
    exists for -- 3,032 accumulated words in `[Unreleased]`, then a 4,131-word
    release section whose bullets ran to 7,054 characters each and read as
    engineering notes ("registryHygieneAudit", "consecutiveFailures",
    "observationAgeDays") rather than as what changed for a user.

    Blocking: total word count and longest single bullet.
    Advisory: an internal-detail leak list, printed but never fatal.
    """
    changelog_text = (repo_root / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")
    app_version = get_app_version()
    top_release = _section(changelog_text, f"## [{app_version}]")

    words = len(top_release.split())
    assert words <= RELEASE_NOTE_WORD_BUDGET, (
        f"The `## [{app_version}]` release section is {words} words, over the "
        f"{RELEASE_NOTE_WORD_BUDGET}-word budget for user-facing release notes. Summarise the "
        "change for someone who uses the app; the engineering detail belongs in the commit "
        "message and the linked evidence, not in the shipped notes."
    )

    bullets = [line for line in top_release.splitlines() if line.startswith("- ")]
    assert bullets, f"The `## [{app_version}]` section has no `- ` bullets."
    longest = max(bullets, key=len)
    assert len(longest) <= RELEASE_NOTE_BULLET_CHAR_BUDGET, (
        f"A bullet in `## [{app_version}]` is {len(longest)} characters, over the "
        f"{RELEASE_NOTE_BULLET_CHAR_BUDGET}-character budget. Longest offender: "
        f"{longest[:90]!r}... Split it into a short statement plus a short consequence."
    )

    # Advisory: report, never block.
    for pattern, label in _ADVISORY_INTERNAL_LEAK_PATTERNS:
        hits = sorted({m.group(0) for m in re.finditer(pattern, top_release)})
        if hits:
            print(
                f"  [advisory] {label} in the {app_version} release notes: " + ", ".join(hits[:5])
            )


def test_release_docs_cover_the_current_public_release_line(repo_root: Path) -> None:
    docs_dir = repo_root / "docs"
    changelog_text = (docs_dir / "CHANGELOG.md").read_text(encoding="utf-8")
    release_text = (docs_dir / "RELEASE.md").read_text(encoding="utf-8")
    app_version = get_app_version()
    top_release = _section(changelog_text, f"## [{app_version}]")

    assert "src/app_version.py" in release_text
    assert "v<app_version>" in release_text
    assert "single release-note source of truth" in release_text
    assert "npm run test:frontend:packaged:jobs-pipeline" in release_text
    assert "npm run test:frontend:packaged:admin-active-run" in release_text
    assert "npm run test:frontend:packaged:task-abort-schedule-rehearsal" in release_text
    assert "npm run test:frontend:packaged:sync-rehearsal" in release_text
    assert "npm run test:py:extended" in release_text
    assert "python scripts/extract_release_notes.py" in release_text
    assert f"## [{app_version}]" in changelog_text
    assert top_release.startswith(f"## [{app_version}] - ")
    assert any(
        heading in top_release for heading in ("### Added", "### Changed", "### Fixed", "### Notes")
    )
    assert "\n- " in top_release
    assert "same-origin Linux container" in top_release
    assert "Umbrel raw-LAN installs" in top_release
    assert "GHCR multi-arch image publishing" in top_release
    assert "private community app-store metadata" in top_release
    assert "wildcard browser CORS allow headers" in top_release
    assert "desktop localhost bridge compatibility" in top_release
    assert "## [Unreleased]" in changelog_text
    assert "Current development" not in changelog_text
    assert "## [1.3.0]" not in changelog_text
    assert "[1.3.0] — 2026-03-22" not in changelog_text


def test_local_setup_points_to_canonical_commands_and_docs(repo_root: Path) -> None:
    local_setup_path = repo_root / "docs" / "LOCAL_SETUP.md"
    text = local_setup_path.read_text(encoding="utf-8")

    assert "npm run dev:bridge" in text
    assert "npm run test:py" in text
    assert "admin-bridge-api.md" in text
    assert "RELEASE.md" in text
    assert "1.2.3" not in text


def test_ai_bootstrap_sequence_is_single_path(repo_root: Path) -> None:
    index_text = (repo_root / "docs" / "INDEX.md").read_text(encoding="utf-8")
    guide_text = (repo_root / "docs" / "AI_ASSISTANT_GUIDE.md").read_text(encoding="utf-8")

    sequence = _section(index_text, "## Recommended AI read sequence")
    read_order = _section(guide_text, "## Read order")

    assert "AI_ASSISTANT_GUIDE.md" in sequence
    assert "architecture-ai-map.md" in sequence
    assert "testing.md" in sequence
    assert "README.md" not in sequence
    assert "../AGENTS.md" not in sequence
    assert "Historical archive detail is intentionally trimmed" in index_text

    assert "README.md" not in read_order
    assert "DOCS_WORKFLOW.md" not in read_order
    assert "architecture-ai-map.md" in read_order
    assert "AGENTS.md" in read_order
    assert "Do not load archive docs by default." in guide_text
    assert "docs-first, not docs-only" in guide_text
    assert "authoritative only for the surface they declare" in guide_text
    assert "Serena memory and repo docs ever diverge" in guide_text
    assert "docs/wiki/" not in guide_text
    assert "wiki > code" not in guide_text


def test_docs_workflow_is_indexed_and_linked_for_contributors(repo_root: Path) -> None:
    docs_dir = repo_root / "docs"
    workflow_text = (docs_dir / "DOCS_WORKFLOW.md").read_text(encoding="utf-8")
    index_text = (docs_dir / "INDEX.md").read_text(encoding="utf-8")
    contributing_text = (repo_root / "CONTRIBUTING.md").read_text(encoding="utf-8")
    readme_text = (repo_root / "README.md").read_text(encoding="utf-8")

    assert "docs-first, not docs-only" in workflow_text
    assert "checked-in repo targets" in workflow_text
    assert "generated or usually-absent artifact paths such as `_out/`" in workflow_text
    assert "docs/CHANGELOG.md" not in workflow_text
    assert "[`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md)" in index_text
    assert "Documentation maintenance" in index_text
    assert "docs/DOCS_WORKFLOW.md" in contributing_text
    assert "docs/DOCS_WORKFLOW.md" in readme_text


def test_runtime_and_tool_configs_keep_separate_ownership(repo_root: Path) -> None:
    runtime_config = json.loads((repo_root / "baluffo.config.json").read_text(encoding="utf-8"))
    tool_config = json.loads((repo_root / "opencode.json").read_text(encoding="utf-8"))
    data_contract_text = (repo_root / "docs" / "DATA_CONTRACT.md").read_text(encoding="utf-8")

    runtime_sections = {"bridge", "storage", "security", "sync", "desktop"}
    tool_sections = {"$schema", "plugin", "mcp"}
    assert runtime_sections.issubset(runtime_config)
    assert not tool_sections.intersection(runtime_config)
    assert tool_sections.issubset(tool_config)
    assert not runtime_sections.intersection(tool_config)

    storage = runtime_config["storage"]
    assert storage["data_dir"] == "data"
    assert storage["source_discovery_config_path"] == "data/source-discovery-config.json"
    assert storage["source_discovery_log_path"] == "data/source-discovery.log"
    assert storage["social_sources_config_path"] == "data/social-sources-config.json"
    assert "`opencode.json` remains separate" in data_contract_text
    assert "do not move MCP/editor keys into" in data_contract_text


def test_serena_tooling_is_first_class_for_codex_and_opencode(repo_root: Path) -> None:
    docs_dir = repo_root / "docs"
    serena_text = (repo_root / "tools" / "mcp" / "SERENA.md").read_text(encoding="utf-8")
    mcp_index_text = (repo_root / "tools" / "mcp" / "INDEX.md").read_text(encoding="utf-8")
    playwright_text = (repo_root / "tools" / "mcp" / "PLAYWRIGHT.md").read_text(encoding="utf-8")
    index_text = (docs_dir / "INDEX.md").read_text(encoding="utf-8")
    guide_text = (docs_dir / "AI_ASSISTANT_GUIDE.md").read_text(encoding="utf-8")
    contributing_text = (repo_root / "CONTRIBUTING.md").read_text(encoding="utf-8")
    readme_text = (repo_root / "README.md").read_text(encoding="utf-8")
    tools_text = (repo_root / "tools" / "README.md").read_text(encoding="utf-8")
    opencode = json.loads((repo_root / "opencode.json").read_text(encoding="utf-8"))

    assert "SERENA.md" in mcp_index_text
    assert "PLAYWRIGHT.md" in mcp_index_text
    assert "Conventions for Future MCP Docs" in mcp_index_text
    assert "Codex browser inspection uses the built-in in-app Browser" in mcp_index_text
    assert "not Chrome DevTools MCP or `@playwright/mcp`" in mcp_index_text
    assert "For the MCP tooling landing page, use [INDEX.md](INDEX.md)." in playwright_text
    assert "deprecated local Playwright MCP fallback" in playwright_text
    assert (
        "Do not configure Chrome DevTools MCP, `@playwright/mcp`, or this local server"
        in playwright_text
    )
    assert "[PLAYWRIGHT.md](PLAYWRIGHT.md)" in serena_text
    assert "use the built-in in-app Browser and Developer mode" in serena_text
    assert "Codex CLI" in serena_text
    assert "OpenCode" in serena_text
    assert "repo docs stay canonical" in serena_text
    assert ".serena/" in serena_text
    assert "codex --profile baluffo" in serena_text
    assert 'uv tool install --force -p 3.13 "serena-agent"' in serena_text
    assert "latest `serena-agent` release" in serena_text
    assert '"--from", "serena-agent@latest"' in serena_text
    assert "serena project create --language python --language typescript" in serena_text
    assert "There is no separate JavaScript Serena language key; use `typescript`" in serena_text
    assert "Node.js and npm" in serena_text
    assert "OpenCode-specific context" in serena_text
    assert "Do not commit absolute" in serena_text
    assert "Do not commit absolute" in (repo_root / "tools" / "mcp" / "BASIC_MEMORY.md").read_text(
        encoding="utf-8"
    )
    assert "tools/mcp/SERENA.md" in contributing_text
    assert "tools/mcp/SERENA.md" in readme_text
    assert "tools/mcp/README.md" not in contributing_text
    assert "tools/mcp/README.md" not in readme_text
    assert "tools/mcp/README.md" not in tools_text
    assert "[`../tools/mcp/SERENA.md`](../tools/mcp/SERENA.md)" in index_text
    assert "[`../tools/mcp/INDEX.md`](../tools/mcp/INDEX.md)" in index_text
    assert "../tools/mcp/README.md" not in index_text
    assert "Serena memory and repo docs ever diverge" in guide_text
    assert (
        "Do not configure Chrome DevTools MCP, `@playwright/mcp`, or standalone Playwright"
        in guide_text
    )
    assert "required Serena setup" in tools_text
    assert "deprecated non-Codex Playwright MCP fallback" in tools_text
    assert "OpenCode" in tools_text
    assert opencode["mcp"]["servers"]["serena"]["command"] == [
        "uvx",
        "-p",
        "3.13",
        "--from",
        "serena-agent@latest",
        "serena",
        "start-mcp-server",
        "--context",
        "ide",
        "--project-from-cwd",
    ]
    assert opencode["mcp"]["servers"]["basic-memory"]["command"] == [
        "uvx",
        "basic-memory@latest",
        "mcp",
        "--project",
        "baluffo-memory",
    ]


def test_docs_avoid_stale_archive_and_generated_artifact_links(repo_root: Path) -> None:
    index_text = (repo_root / "docs" / "INDEX.md").read_text(encoding="utf-8")
    changelog_text = (repo_root / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")

    assert "[`_out/`](../_out/)" not in index_text
    assert "`_out/`" in index_text
    # The changelog must not point readers at archive material or generated artifacts.
    # This used to also require a specific historical mention of
    # `scraping-pipeline-run-notes.md`; that file is gone from the repo and the mention
    # lived in `0.1.0`, so it pinned a deleted file's wording rather than a rule.
    assert "docs/archive/" not in changelog_text
    assert "(archive/)" not in changelog_text


def test_ai_docs_classify_compatibility_surfaces(repo_root: Path) -> None:
    index_text = (repo_root / "docs" / "INDEX.md").read_text(encoding="utf-8")
    guide_text = (repo_root / "docs" / "AI_ASSISTANT_GUIDE.md").read_text(encoding="utf-8")
    map_text = (repo_root / "docs" / "architecture-ai-map.md").read_text(encoding="utf-8")
    inventory_text = (repo_root / "docs" / "adapter-plugin-inventory.md").read_text(
        encoding="utf-8"
    )

    assert "## Compatibility Surfaces" in index_text
    assert "This index intentionally does not duplicate that table." in index_text
    assert "stable thin entrypoint for bridge startup and compatibility wrappers" not in index_text
    assert "src/admin_bridge.py (stable thin entrypoint / wiring-only composition root)" in map_text
    assert "stable thin CLI entrypoint" in map_text
    assert "stable thin CLI facade" in map_text
    assert "`src/jobs/common/__init__.py` is a package marker only" in guide_text
    assert "_runtime.facade()` is retired" in guide_text
    assert "package marker only" in map_text
    assert "transitional local-data boundary" in map_text
    assert (
        "export the parser through `src/jobs/parsers.py` and `src/jobs_fetcher.py`"
        not in inventory_text
    )
    assert (
        "only touch `src/jobs_fetcher.py` if a legacy CLI compatibility re-export must stay available"
        in inventory_text
    )


def test_ai_docs_use_exact_frontend_syntax_example(repo_root: Path) -> None:
    ai_text = (repo_root / "docs" / "AI_ASSISTANT_GUIDE.md").read_text(encoding="utf-8")
    map_text = (repo_root / "docs" / "architecture-ai-map.md").read_text(encoding="utf-8")

    for text in (ai_text, map_text):
        assert "node --check frontend/*/app.js" not in text
        assert "node --check frontend/jobs/app.js" in text


def test_readme_is_product_overview_not_ai_entrypoint(repo_root: Path) -> None:
    text = (repo_root / "README.md").read_text(encoding="utf-8")

    assert "primary entry point for AI coders" not in text
    assert "scan-first guide for AI-assisted coding" not in text
    assert "AI coding workflow and edit routing" in text


def test_package_json_build_aliases_use_leaf_builders(repo_root: Path) -> None:
    package_path = repo_root / "package.json"
    package = json.loads(package_path.read_text(encoding="utf-8"))
    scripts = package["scripts"]

    assert scripts["build:ship-bundle"] == (
        "npm run check:python-version && python scripts/build_ship_bundle.py"
    )
    assert scripts["build:portable-exe"] == (
        "npm run check:python-version && python scripts/build_portable_exe.py"
    )
    assert scripts["build:portable-exe:prepare"] == (
        "npm run check:python-version && python scripts/build_portable_exe.py --skip-zip"
    )
    assert scripts["test:frontend:packaged"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py"
    )
    assert scripts["test:frontend:packaged:sync-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --sync-rehearsal --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:browser-job-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --browser-job-rehearsal --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:first-run"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --open-path desktop-probe.html --node-smoke-script tests/frontend/packaged-desktop-smoke.first-run-jobs.mjs --runtime-timeout 60 --playwright-timeout 240"
    )
    assert scripts["test:frontend:packaged:active-task-close-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --active-task-close-rehearsal --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:task-abort-schedule-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --open-path admin.html --node-smoke-script tests/frontend/packaged-desktop-smoke.task-abort-schedule.mjs --runtime-timeout 60 --playwright-timeout 240"
    )
    assert scripts["test:frontend:packaged:admin-active-run"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --open-path admin.html --node-smoke-script tests/frontend/packaged-desktop-smoke.admin-active-run.mjs --runtime-timeout 60 --playwright-timeout 180"
    )
    assert scripts["release:preflight"] == (
        "npm run lint:precommit && npm run test:py:extended && npm run test:frontend:unit && npm run security:js && npm run check:published-version && npm run build:portable-exe:prepare && npm run test:frontend:packaged && npm run test:frontend:packaged:admin-startup && npm run test:frontend:packaged:admin-active-run && npm run test:frontend:packaged:fetch-evidence && npm run test:frontend:packaged:sync-rehearsal && npm run test:frontend:packaged:update-rehearsal && npm run test:frontend:packaged:orphan-reclaim-rehearsal && npm run test:frontend:packaged:browser-job-rehearsal && npm run test:frontend:packaged:desktop-lifecycle-rehearsal && npm run test:frontend:packaged:active-task-close-rehearsal && npm run test:frontend:packaged:task-abort-schedule-rehearsal && npm run test:frontend:packaged:first-run && npm run test:frontend:packaged:jobs-pipeline && npm run probe:desktop:startup:jobs:cold"
    )
    assert (
        "_out/latest/build/portable/Baluffo.exe"
        not in scripts["test:frontend:packaged:jobs-pipeline"]
    )
    assert "verify:portable" not in scripts


def test_release_and_setup_docs_use_canonical_packaged_smoke_commands(repo_root: Path) -> None:
    release_text = (repo_root / "docs" / "RELEASE.md").read_text(encoding="utf-8")
    local_setup_text = (repo_root / "docs" / "LOCAL_SETUP.md").read_text(encoding="utf-8")

    assert "python scripts/packaged_desktop_smoke.py --rebuild" not in release_text
    assert "npm run probe:desktop:startup:cold" in release_text
    assert "npm run probe:desktop:startup:warm" in release_text
    assert "python scripts/packaged_desktop_smoke.py --rebuild" not in local_setup_text


def test_testing_doc_owns_verification_matrix(repo_root: Path) -> None:
    testing_text = (repo_root / "docs" / "testing.md").read_text(encoding="utf-8")

    for command in (
        "npm run test:py",
        "npm run test:py:extended",
        "npm run release:preflight",
        "npm run lint:repo-guardrails",
        "npm run security:python",
        "npm run security:js",
        "npm run build:ship-bundle",
        "npm run build:portable-exe",
        "python scripts/build_ship_bundle.py --bundle-version <version>",
        "python scripts/build_portable_exe.py --bundle-version <version>",
        "npm run test:frontend:packaged",
        "npm run test:frontend:packaged:sync-rehearsal",
        "npm run test:frontend:packaged:admin-active-run",
        "npm run test:frontend:packaged:orphan-reclaim-rehearsal",
        "npm run test:frontend:packaged:browser-job-rehearsal",
        "npm run test:frontend:packaged:active-task-close-rehearsal",
        "npm run test:frontend:packaged:task-abort-schedule-rehearsal",
        "npm run test:frontend:packaged:first-run",
        "npm run probe:desktop:startup:cold",
        "npm run probe:desktop:startup:jobs:cold",
    ):
        assert command in testing_text

    assert "This document owns the verification matrix for Baluffo." in testing_text
    assert "startup-probe-architecture.md" in testing_text
    assert "Real shard files must own real tests." in testing_text
    assert "delete or merge any older test that already protects the same invariant" in testing_text
    assert "For the AI bootstrap and task-routing summary" in testing_text
    assert "admin_bridge_entrypoint_root" in testing_text
    assert "admin_bridge_ops_root" not in testing_text
    assert "tests/source_discovery/" in testing_text


def test_release_guide_uses_canonical_release_preflight(repo_root: Path) -> None:
    release_text = (repo_root / "docs" / "RELEASE.md").read_text(encoding="utf-8")

    assert "npm run release:preflight" in release_text
    assert "exact commit you plan to push or tag" in release_text
    assert "npm run lint:precommit" in release_text
    assert "npm run test:frontend:packaged:sync-rehearsal" in release_text
    assert "npm run test:frontend:packaged:admin-active-run" in release_text
    assert "npm run test:frontend:packaged:orphan-reclaim-rehearsal" in release_text
    assert "npm run test:frontend:packaged:browser-job-rehearsal" in release_text
    assert "npm run test:frontend:packaged:active-task-close-rehearsal" in release_text
    assert "npm run test:frontend:packaged:task-abort-schedule-rehearsal" in release_text
    assert "npm run test:frontend:packaged:first-run" in release_text
    assert "npm run test:frontend:packaged:jobs-pipeline" in release_text
    assert "npm run probe:desktop:startup:jobs:cold" in release_text
    assert "startup-probe-architecture.md" in release_text


def test_active_docs_avoid_stale_runtime_and_test_guidance(repo_root: Path) -> None:
    local_setup_text = (repo_root / "docs" / "LOCAL_SETUP.md").read_text(encoding="utf-8")
    admin_api_text = (repo_root / "docs" / "admin-bridge-api.md").read_text(encoding="utf-8")
    game_sheet_text = (repo_root / "docs" / "game-studios-sheet.md").read_text(encoding="utf-8")
    release_text = (repo_root / "docs" / "RELEASE.md").read_text(encoding="utf-8")
    testing_text = (repo_root / "docs" / "testing.md").read_text(encoding="utf-8")
    index_text = (repo_root / "docs" / "INDEX.md").read_text(encoding="utf-8")
    assert "configured local admin PIN" not in local_setup_text
    assert "1234" not in local_setup_text
    assert "Admin overview (requires PIN)" not in admin_api_text
    assert "Wipe account (requires PIN)" not in admin_api_text

    for text in (local_setup_text, admin_api_text):
        assert "requires PIN" not in text

    for text in (local_setup_text, game_sheet_text, testing_text, index_text):
        assert "tests/test_source_discovery.py" not in text

    assert "scripts/ship/update_manager.py" not in release_text
    assert "src/ship/update_manager.py" in release_text
    assert "## Minimum commands" in local_setup_text
    assert "npm run test:py" in local_setup_text
    assert "Full suite / release lane" in testing_text


def test_index_routes_current_process_docs_only(repo_root: Path) -> None:
    index_text = (repo_root / "docs" / "INDEX.md").read_text(encoding="utf-8")
    archive_files = sorted(
        path.relative_to(repo_root).as_posix()
        for path in (repo_root / "docs" / "archive").rglob("*")
        if path.is_file()
    )

    assert "historical-debt-roadmap.md" not in index_text
    assert "quality-improvement-roadmap.md" not in index_text
    assert "quality-follow-up.md" not in index_text
    assert "startup-probe-architecture.md" in index_text
    assert "## Archive" in index_text
    assert "archive/README.md" in index_text
    assert "## Refactor Charters" not in index_text
    assert "Refactor record" not in index_text
    assert archive_files == [
        "docs/archive/0.2.0-deferred-desktop-ux-polish-plan.md",
        "docs/archive/0.2.0-release-readiness-plan.md",
        "docs/archive/PLAN-abort-telemetry.md",
        "docs/archive/README.md",
        "docs/archive/admin-health-dashboard-console-closeout.md",
        "docs/archive/ai-modification-safety-improvements-plan.md",
        "docs/archive/ats-custom-domain-detection-plan.md",
        "docs/archive/bridge-route-inventory-guardrail-plan.md",
        "docs/archive/browser-fallback-pool-plan.md",
        "docs/archive/cold-run-jobs-freshness-strategy-plan.md",
        "docs/archive/cross-cutting-tech-debt-reduction-plan.md",
        "docs/archive/daily-trust-ux-strategy-plan.md",
        "docs/archive/dedup-current-run-blocker-review-closeout.md",
        "docs/archive/dedup-google-sheets-role-bucket-audit-closeout.md",
        "docs/archive/dedup-lifecycle-readiness-closeout.md",
        "docs/archive/dedup-provider-static-disagreement-reconciliation-closeout.md",
        "docs/archive/end-to-end-benchmarking-plan.md",
        "docs/archive/external-memory-mcp-policy-plan.md",
        "docs/archive/initial_findings-2026-08-20.md",
        "docs/archive/job-sanitization-plan-2026-05-18.md",
        "docs/archive/jobs-fetcher-aggressive-simplification-closeout.md",
        "docs/archive/jobs-pipeline-memory-reduction-plan.md",
        "docs/archive/linux-compatibility-plan.md",
        "docs/archive/provider-discovery-coverage-gap-plan.md",
        "docs/archive/read-only-lifecycle-ux-closeout.md",
        "docs/archive/registry-summary-desktop-performance-cleanup-report.md",
        "docs/archive/runtime-storage-and-sync-architecture-plan.md",
        "docs/archive/source-discovery-adapter-follow-ups-closeout.md",
        "docs/archive/source-sync-production-readiness-closeout.md",
        "docs/archive/static-outlier-source-conflict-decisions.md",
        "docs/archive/static-plugin-simple-runner-migration-plan.md",
        "docs/archive/static-scope-conflict-dry-run-decisions.md",
        "docs/archive/task-lifecycle-ledger-plan.md",
        "docs/archive/task-progress-operational-console-closeout.md",
        "docs/archive/task_lifecycle_ledger_closeout_plan.md",
        "docs/archive/windows-user-data-migration-plan.md",
    ]


def test_contributing_points_startup_perf_changes_to_canonical_architecture_doc(
    repo_root: Path,
) -> None:
    contributing_text = (repo_root / "CONTRIBUTING.md").read_text(encoding="utf-8")

    assert "docs/startup-probe-architecture.md" in contributing_text
