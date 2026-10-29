"""Working-tree EOL contract: tracked files must honour `.gitattributes`.

`.gitattributes` pins `* text=auto eol=lf`, so every tracked text file is meant to
sit on disk with LF endings. Git compares working-tree content through the clean
filter, which normalizes CRLF back to LF before diffing: a file rewritten with
CRLF by a platform-native writer is therefore *clean* to `git status` and
`git diff` while its bytes disagree with the contract. That is how the generated
`frontend-runtime-config.js` sat outside the contract on Windows after
`Path.write_text` wrote CRLF, and four other tracked files were found in the same
state when this gate was written.

The expected form is read from git rather than re-derived from the attribute
text: `git ls-files --eol` reports the worktree class git itself computed
(`w/lf`, `w/crlf`, `w/mixed`, `w/none`, `w/-text`) next to each file's resolved
attributes. Binary detection, nested `.gitattributes`, and `-text` overrides stay
git's decisions instead of a hand-rolled approximation of them.

Two checks, because a silently disabled contract is worse than a violated one:

* `check_worktree_eol_contract` -- no tracked text file's worktree class
  contradicts its resolved `eol` attribute.
* `check_root_eol_contract` -- a synthetic repo path still resolves to `eol=lf`,
  so deleting or narrowing the `*` rule cannot reduce the first check to a
  vacuous pass. With `core.autocrlf=true`, as on this machine, an absent rule
  would also make CRLF the expected checkout form.

A fixture that genuinely needs CRLF bytes opts out the git way -- `-text` or an
explicit `eol=crlf` rule -- and is then outside this contract by construction,
not by an allowlist here.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Every tracked text path must resolve to this ending. See the module docstring:
# without the root rule the comparison would have no reference and the gate would
# check nothing, which is why `check_root_eol_contract` asserts it separately.
REQUIRED_ROOT_EOL = "lf"
CONTRACT_PROBE_PATH = "eol-contract-probe"

# A drift event usually hits one generated file, so a long list means a writer or
# checkout went wrong globally. Enough paths to identify the producer, not a wall.
MAX_REPORTED_FILES = 20

_WORKTREE_ENDINGS = {
    "crlf": "CRLF line endings",
    "mixed": "mixed LF and CRLF line endings",
    "lf": "LF line endings",
}
# `none` is an empty file; `-text` is git's binary classification (or an explicit
# `-text` attribute). Neither is line-ending converted, so neither is in contract.
_UNCONVERTED_WORKTREE_CLASSES = frozenset({"", "none", "-text"})


def _git_bytes(repo_root: Path, *args: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).decode("utf-8", "replace").strip()
        raise RuntimeError(
            f"git {' '.join(args)} failed in {repo_root} (exit {completed.returncode}): "
            f"{detail or 'no output'}"
        )
    return completed.stdout


def _tracked_eol_records(repo_root: Path) -> list[tuple[str, str, str]]:
    """``(path, worktree class, resolved attributes)`` for every tracked file.

    ``git ls-files --eol -z`` emits ``i/<class> w/<class> attr/<attrs>\\t<path>\\0``.
    Splitting the header at most twice keeps attribute values that contain spaces
    together instead of treating each one as a separate field.
    """
    records: list[tuple[str, str, str]] = []
    for record in _git_bytes(repo_root, "ls-files", "--eol", "-z").split(b"\0"):
        if not record:
            continue
        header, _, raw_path = record.partition(b"\t")
        fields = header.split(None, 2)
        worktree = (
            fields[1][2:].decode("ascii", "replace")
            if len(fields) > 1 and fields[1].startswith(b"w/")
            else ""
        )
        attributes = (
            fields[2][5:].decode("ascii", "replace")
            if len(fields) > 2 and fields[2].startswith(b"attr/")
            else ""
        )
        records.append((raw_path.decode("utf-8", "surrogateescape"), worktree, attributes))
    return records


def _expected_eol(attributes: str) -> str:
    """The ``eol`` value among ``key=value`` attribute tokens, or an empty string."""
    for token in attributes.split():
        key, _, value = token.partition("=")
        if key == "eol":
            return value
    return ""


def check_root_eol_contract(repo_root: Path | None = None) -> list[str]:
    """Require the root rule itself, so the per-file check cannot go vacuous."""
    root = ROOT if repo_root is None else repo_root
    # `git check-attr` resolves patterns for a path that need not exist, so this
    # reads the rule as git will apply it to any file instead of matching the
    # `.gitattributes` text.
    raw = _git_bytes(root, "check-attr", "-z", "eol", "--", CONTRACT_PROBE_PATH)
    parts = raw.split(b"\0")
    resolved = parts[2].decode("utf-8", "replace").strip() if len(parts) > 2 else ""
    if resolved == REQUIRED_ROOT_EOL:
        return []
    return [
        f".gitattributes resolves eol={resolved or 'unspecified'} for a file at the repo "
        f"root, but this repository's contract is eol={REQUIRED_ROOT_EOL} for every tracked "
        "text file. Restore `* text=auto eol=lf`; without it the per-file EOL check has no "
        "reference, and Windows checkouts follow core.autocrlf instead of the contract."
    ]


def check_worktree_eol_contract(repo_root: Path | None = None) -> list[str]:
    """List tracked text files whose worktree endings contradict their ``eol`` attribute."""
    root = ROOT if repo_root is None else repo_root
    offenders: list[str] = []
    for path, worktree, attributes in _tracked_eol_records(root):
        expected = _expected_eol(attributes)
        if expected not in {"lf", "crlf"} or worktree in _UNCONVERTED_WORKTREE_CLASSES:
            continue
        if worktree == expected:
            continue
        endings = _WORKTREE_ENDINGS.get(worktree, f"{worktree!r} line endings")
        offenders.append(
            f"{path}: working tree has {endings} but .gitattributes resolves "
            f"eol={expected}. Rewrite the file with {expected.upper()} endings and fix the "
            "writer that produced it; git normalizes this comparison, so the drift leaves "
            "`git status` and `git diff` clean."
        )
    if len(offenders) <= MAX_REPORTED_FILES:
        return offenders
    hidden = len(offenders) - MAX_REPORTED_FILES
    return [
        *offenders[:MAX_REPORTED_FILES],
        f"... and {hidden} more tracked file(s) outside the EOL contract.",
    ]
