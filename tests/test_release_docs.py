import subprocess
import sys
from pathlib import Path

import pytest

from src.app_version import get_app_version
from tools.repo_health.release_docs_policy import (
    test_changelog_holds_no_unpublished_section_above_the_last_release_tag,
    test_changelog_keeps_unreleased_above_versioned_rollup,
    test_release_notes_artifact_matches_current_version,
)


def test_release_notes_extractor_uses_top_changelog_section(
    repo_root: Path, tmp_path: Path
) -> None:
    script_path = repo_root / "scripts" / "extract_release_notes.py"
    changelog_path = repo_root / "docs" / "CHANGELOG.md"
    output_path = tmp_path / "release-notes.md"
    app_version = get_app_version()

    completed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            str(script_path),
            "--version",
            app_version,
            "--changelog",
            str(changelog_path),
            "--output",
            str(output_path),
        ],
        cwd=repo_root,
        capture_output=True,
        check=True,
        text=True,
    )

    extracted = output_path.read_text(encoding="utf-8")
    assert str(output_path) in completed.stdout
    assert extracted.startswith(f"## [{app_version}] - ")
    assert "## [Unreleased]" not in extracted


def test_release_notes_guardrail_accepts_current_version(repo_root: Path) -> None:
    """The tracked release-notes.md must be regenerated for the current app version."""
    release_notes_path = repo_root / "release-notes.md"
    assert release_notes_path.is_file(), "test precondition: release-notes.md is tracked"
    test_release_notes_artifact_matches_current_version(repo_root)  # noqa: S101 - raises on stale


def test_changelog_unreleased_guardrail_accepts_current_tree(repo_root: Path) -> None:
    """The tracked changelog keeps an `[Unreleased]` section above the versioned rollup."""
    changelog_path = repo_root / "docs" / "CHANGELOG.md"
    assert changelog_path.is_file(), "test precondition: docs/CHANGELOG.md is tracked"
    test_changelog_keeps_unreleased_above_versioned_rollup(repo_root)  # noqa: S101 - raises on drift


def test_changelog_unreleased_guardrail_rejects_missing_unreleased(repo_root: Path) -> None:
    """A changelog with no `[Unreleased]` heading must fail the docs guardrail."""
    changelog_path = repo_root / "docs" / "CHANGELOG.md"
    original = changelog_path.read_bytes()
    app_version = get_app_version()
    try:
        payload = (
            f"# Changelog\n\n---\n\n## [{app_version}] - 2026-08-18\n\n### Added\n\n- nothing\n"
        )
        changelog_path.write_bytes(payload.encode("utf-8"))
        with pytest.raises(AssertionError, match="missing the `## \\[Unreleased\\]` section"):
            test_changelog_keeps_unreleased_above_versioned_rollup(repo_root)
    finally:
        changelog_path.write_bytes(original)


def test_changelog_unreleased_guardrail_rejects_unreleased_below_rollup(repo_root: Path) -> None:
    """An `[Unreleased]` heading below the versioned rollup must fail the docs guardrail."""
    changelog_path = repo_root / "docs" / "CHANGELOG.md"
    original = changelog_path.read_bytes()
    app_version = get_app_version()
    try:
        payload = f"# Changelog\n\n---\n\n## [{app_version}] - 2026-08-18\n\n### Added\n\n- nothing\n\n---\n\n## [Unreleased]\n\n"
        changelog_path.write_bytes(payload.encode("utf-8"))
        with pytest.raises(AssertionError, match="must sit above the versioned rollup"):
            test_changelog_keeps_unreleased_above_versioned_rollup(repo_root)
    finally:
        changelog_path.write_bytes(original)


def test_release_notes_guardrail_rejects_stale_version(repo_root: Path, tmp_path: Path) -> None:
    """A version bump without regenerating release-notes.md must fail the docs guardrail."""
    release_notes_path = repo_root / "release-notes.md"
    original = release_notes_path.read_bytes()
    try:
        release_notes_path.write_bytes(b"## [0.0.0] - 1970-01-01\n\n> stale placeholder\n")
        with pytest.raises(AssertionError, match="release-notes.md is stale"):
            test_release_notes_artifact_matches_current_version(repo_root)
    finally:
        release_notes_path.write_bytes(original)


def test_tracked_docs_restore_is_byte_identical(repo_root: Path) -> None:
    """Rewriting and restoring tracked docs must preserve on-disk bytes exactly."""
    for rel_path in ("docs/CHANGELOG.md", "release-notes.md"):
        path = repo_root / rel_path
        before = path.read_bytes()
        try:
            path.write_bytes(b"## [0.0.0] - 1970-01-01\n\n> stale placeholder\n")
        finally:
            path.write_bytes(before)


# --- LF-only output --------------------------------------------------------
#
# The repo is LF-only (.gitattributes: `* text=auto eol=lf`). Path.write_text()
# with the default newline=None applies os.linesep translation, so on Windows
# these writers emitted CRLF and `ruff format --check` then failed on
# src/app_version.py -- a confusing failure that looks unrelated to the bump.
# These tests pin the newline="\n" argument so the bug cannot return silently.
#
# loc_budget.py is here for the same reason and was missed by the original
# sweep: it rewrote a tracked baseline as CRLF, so every `--update` on Windows
# produced a whole-file diff for a one-number change. Three writers have now
# needed this fix, which is why the list is pinned by test rather than by
# remembering.

_LF_SENSITIVE_WRITE_SITES = (
    ("scripts/bump_version.py", "_write"),
    ("scripts/extract_release_notes.py", "build_release_notes"),
    ("tools/repo_health/loc_budget.py", "write_baseline"),
)


@pytest.mark.parametrize(("rel_path", "func_name"), _LF_SENSITIVE_WRITE_SITES)
def test_release_scripts_write_lf_only(repo_root: Path, rel_path: str, func_name: str) -> None:
    """A release-tooling writer must pass newline="\\n" to write_text()."""
    import ast

    source = (repo_root / rel_path).read_text(encoding="utf-8")
    tree = ast.parse(source)

    target = next(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == func_name
        ),
        None,
    )
    assert target is not None, f"{rel_path} has no {func_name}()"

    # Keep the call and its method name together: mypy does not carry the
    # `isinstance(node.func, ast.Attribute)` narrowing into a later `call.func`
    # access, so `call.func.attr` would be an attr-defined error on ast.expr.
    write_calls: list[tuple[ast.Call, str]] = [
        (node, node.func.attr)
        for node in ast.walk(target)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"write_text", "write_bytes"}
    ]
    assert write_calls, f"{rel_path}:{func_name} no longer writes anything; update this test"

    for call, method_name in write_calls:
        keywords = {kw.arg for kw in call.keywords}
        assert "newline" in keywords, (
            f"{rel_path}:{func_name} calls {method_name}() at line {call.lineno} "
            'without newline="\\n", so it emits CRLF on Windows and breaks '
            "`ruff format --check` on the LF-only tree."
        )


def test_bump_version_writes_lf_on_this_platform(repo_root: Path, tmp_path: Path) -> None:
    """End-to-end: the bump helper must not emit CRLF bytes on any platform."""
    sys.path.insert(0, str(repo_root / "scripts"))
    try:
        import importlib

        bump_version = importlib.import_module("bump_version")
    finally:
        sys.path.pop(0)

    target = tmp_path / "sample.py"
    bump_version._write(target, 'APP_VERSION = "0.0.0"\n\n\n')  # noqa: SLF001 - pinning the writer

    raw = target.read_bytes()
    assert b"\r\n" not in raw, f"bump_version._write emitted CRLF: {raw!r}"
    assert raw.endswith(b"\n")


def test_loc_budget_writes_lf_baseline_on_this_platform(tmp_path: Path) -> None:
    """The loc baseline writer must not rewrite a tracked file as CRLF.

    Behavioural rather than AST-only, because the damage is in the bytes on
    disk: an LF-only tree plus a CRLF baseline means the next `git add` shows a
    whole-file diff, and the worktree copy stops matching the committed one.
    """
    import importlib.util

    repo_root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "loc_budget_under_test", repo_root / "tools" / "repo_health" / "loc_budget.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # write_baseline targets a nested relative path and does not mkdir; the real
    # repo always has the file, so the fixture has to mirror the layout.
    (tmp_path / module.BASELINE_RELATIVE_PATH).parent.mkdir(parents=True)
    module.write_baseline(tmp_path, {"src": 1, "tests": 2})

    raw = (tmp_path / module.BASELINE_RELATIVE_PATH).read_bytes()
    assert b"\r\n" not in raw, f"loc_budget wrote CRLF into the baseline: {raw!r}"
    assert raw.endswith(b"\n")


# --- queued-version drift --------------------------------------------------
#
# 0.3.016 was bumped, never tagged, then skipped by 0.3.017. Both sections sat in the
# changelog claiming their versions had shipped, so the notes generated for the next tag
# covered one changeset while the other's two fixes were readable only inside the file.
# The rule is pinned against repository shapes, not changelog text alone, so these build
# throwaway repos -- including the tag-less and shallow shapes automation actually uses.


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        ["git", *args], cwd=repo, capture_output=True, text=True, check=False
    )


def _drifting_repo(tmp_path: Path, *, version: str, sections: list[str]) -> Path:
    repo = tmp_path / "repo"
    (repo / "docs").mkdir(parents=True)
    (repo / "src").mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test Runner")
    (repo / "src" / "app_version.py").write_text(f'APP_VERSION = "{version}"\n', encoding="utf-8")
    body = "\n".join(
        f"## [{item}] - 2026-01-0{index}\n### Changed\n\n- something shipped in {item}\n"
        for index, item in enumerate(sections, start=1)
    )
    (repo / "docs" / "CHANGELOG.md").write_text(
        f"# Changelog\n\n## [Unreleased]\n\n{body}", encoding="utf-8"
    )
    _git(repo, "add", "-A")
    completed = _git(repo, "commit", "-q", "-m", "seed changelog")
    assert completed.returncode == 0, completed.stderr
    return repo


def _tag(repo: Path, version: str) -> None:
    completed = _git(repo, "tag", f"v{version}")
    assert completed.returncode == 0, completed.stderr


def test_drift_check_fails_the_version_that_was_bumped_and_skipped(tmp_path: Path) -> None:
    """The 0.3.016 -> 0.3.017 shape: a queued section whose version never tagged."""
    repo = _drifting_repo(tmp_path, version="0.3.017", sections=["0.3.017", "0.3.016", "0.3.015"])
    _tag(repo, "0.3.015")

    with pytest.raises(AssertionError) as excinfo:
        test_changelog_holds_no_unpublished_section_above_the_last_release_tag(repo)

    message = str(excinfo.value)
    flagged = message.split("with no release tag: ", 1)[1].split(". ", 1)[0]
    assert flagged == "0.3.016", (
        f"only the unshipped 0.3.016 should be flagged, not the version in flight: {message!r}"
    )
    assert "cross-channel checkpoint" in message, "the remedy must point at the release guide"


def test_drift_check_allows_the_version_in_flight_to_await_its_tag(tmp_path: Path) -> None:
    """Between a bump and its release the current version legitimately has no tag."""
    repo = _drifting_repo(tmp_path, version="0.3.017", sections=["0.3.017", "0.3.016"])
    _tag(repo, "0.3.016")
    _tag(repo, "0.3.015")

    test_changelog_holds_no_unpublished_section_above_the_last_release_tag(repo)


def test_drift_check_still_fails_once_the_current_version_is_tagged(tmp_path: Path) -> None:
    """A stale section stays flagged even when the version in flight already shipped."""
    repo = _drifting_repo(tmp_path, version="0.3.017", sections=["0.3.017", "0.3.016"])
    _tag(repo, "0.3.017")

    with pytest.raises(AssertionError, match=r"0\.3\.016"):
        test_changelog_holds_no_unpublished_section_above_the_last_release_tag(repo)


def test_drift_check_declines_to_judge_a_checkout_with_no_tags(tmp_path: Path) -> None:
    """A clone fetched with `--no-tags` must not read every section as unpublished."""
    repo = _drifting_repo(tmp_path, version="0.3.017", sections=["0.3.017", "0.3.016", "0.3.015"])
    _tag(repo, "0.3.015")

    clone = tmp_path / "no-tags-clone"
    completed = _git(tmp_path, "clone", "-q", "--no-tags", str(repo), str(clone))
    assert completed.returncode == 0, completed.stderr
    assert _git(clone, "tag", "-l", "v*").stdout.strip() == "", "precondition: no tags"

    test_changelog_holds_no_unpublished_section_above_the_last_release_tag(clone)


def test_drift_check_declines_to_judge_a_shallow_checkout(tmp_path: Path) -> None:
    """`actions/checkout` defaults to a depth that holds only the tags it asked for."""
    repo = _drifting_repo(tmp_path, version="0.3.017", sections=["0.3.017", "0.3.016", "0.3.015"])
    _tag(repo, "0.3.015")

    clone = tmp_path / "shallow-clone"
    completed = _git(tmp_path, "clone", "-q", "--depth", "1", f"file://{repo}", str(clone))
    shallow = _git(clone, "rev-parse", "--is-shallow-repository").stdout.strip()
    if completed.returncode != 0 or shallow != "true":
        pytest.skip(f"git here cannot produce a shallow clone (rc={completed.returncode})")

    test_changelog_holds_no_unpublished_section_above_the_last_release_tag(clone)


def test_drift_guardrail_accepts_the_current_tree(repo_root: Path) -> None:
    """Every versioned section in this repo either shipped or is the version in flight."""
    test_changelog_holds_no_unpublished_section_above_the_last_release_tag(repo_root)
