"""The corpus recorder's provenance label must survive the diff it hashes.

``epoch.git_identity`` hashes the uncommitted diff so a corpus recorded from a dirty tree can say
which dirty tree it came from. That diff is bytes, and a tree can hold text the host console code
page cannot decode -- this pipeline fetches CJK titles routinely -- so the read has to be explicit.
Decoded in cp1252, such a byte raised in the subprocess reader thread and surfaced as
``stdout=None``; both of the function's guards then turned that into a *silently dropped* diff, so
two different dirty trees were labelled the same way. The pin therefore compares the label against
the one built from both git reads, which catches a dropped diff as well as a mis-decoded one.
"""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import pytest

from tools.measurements.corpus import epoch as epoch_mod

# ``東`` is E6 9D B1 in UTF-8, and 0x9d is one of the bytes cp1252 leaves undefined.
ROW_TEXT = "title: Senior ゲーム Engineer — 東急\n"


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _git_stdout(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    ).stdout


def _expected_dirty_hash(repo: Path) -> str:
    """The label as the tool must build it: both reads, decoded explicitly, hashed as one string."""

    dirty = _git_stdout(repo, "status", "--porcelain") + _git_stdout(repo, "diff", "HEAD")
    return hashlib.sha256(dirty.encode("utf-8")).hexdigest()


def _repo_with_one_commit(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "corpus-recorder@example.com")
    _git(repo, "config", "user.name", "Corpus Recorder")
    (repo / "row.txt").write_text("title: ascii\n", encoding="utf-8")
    _git(repo, "add", "row.txt")
    _git(repo, "commit", "-q", "-m", "base")
    return repo


def test_the_fixture_carries_a_byte_cp1252_leaves_undefined() -> None:
    """Without the byte these tests would pass against the defect, so it is asserted, not assumed."""

    with pytest.raises(UnicodeDecodeError):
        ROW_TEXT.encode("utf-8").decode("cp1252")


def test_a_clean_tree_has_a_head_sha_and_no_dirty_hash(tmp_path: Path) -> None:
    repo = _repo_with_one_commit(tmp_path)
    sha, dirty_hash = epoch_mod.git_identity(repo)
    assert len(sha) == 40
    assert dirty_hash == ""


def test_a_diff_the_console_code_page_cannot_decode_is_hashed_not_dropped(
    tmp_path: Path,
) -> None:
    repo = _repo_with_one_commit(tmp_path)
    (repo / "row.txt").write_text(ROW_TEXT, encoding="utf-8")

    sha, dirty_hash = epoch_mod.git_identity(repo)

    assert len(sha) == 40
    # The label has to cover the diff, and cover its bytes: a dropped diff (the reader thread's
    # failure) or one decoded in the console code page both hash to something else.
    assert dirty_hash == _expected_dirty_hash(repo)
    # The same tree must label itself the same way twice, or two recordings of one tree read as two.
    assert epoch_mod.git_identity(repo)[1] == dirty_hash


def test_two_different_dirty_trees_are_labelled_differently(tmp_path: Path) -> None:
    repo = _repo_with_one_commit(tmp_path)
    (repo / "row.txt").write_text(ROW_TEXT, encoding="utf-8")
    _, first = epoch_mod.git_identity(repo)
    (repo / "row.txt").write_text(ROW_TEXT.replace("東急", "東名"), encoding="utf-8")
    _, second = epoch_mod.git_identity(repo)
    assert first != second
