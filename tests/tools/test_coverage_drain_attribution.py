"""Attribution must be per-board, not per-host.

`tools/coverage_drain.py` reports how many openings each curated board collected. It did
that by indexing boards on host alone, so every board sharing a host collapsed into one
bucket: 955 of the collected jobs were `jobs.jobvite.com`, which is several distinct boards,
and the total was over-counted by construction — it reported 6,945 openings collected against
4,716 openings registered, which is not possible and should have been the alarm.

A board owns the path prefix under which it serves postings, so that is the key. The longest
matching prefix wins, because one board can sit under another (`/careers/locations/milan`
under `/careers`).

With per-board attribution the Workday pagination fix is visible for what it is: NVIDIA's
board keeps **1,780** postings against the 2,000 its API reports.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tools"))


def _load():
    spec = importlib.util.spec_from_file_location(
        "coverage_drain_under_test", _ROOT / "tools" / "coverage_drain.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


drain = _load()

# Two boards, one host: the shape that made host-keyed attribution over-count.
INDEX = [
    ("jobs.jobvite.com", "/double-negative-visual-effects", ("jobs.jobvite.com", "dnve")),
    ("jobs.jobvite.com", "/asus", ("jobs.jobvite.com", "asus")),
    ("jobs.jobvite.com", "", ("jobs.jobvite.com", "root")),
]


def _url(path: str) -> str:
    return f"https://jobs.jobvite.com{path}"


def test_postings_route_to_the_board_whose_prefix_contains_them() -> None:
    assert drain._match_board(_url("/double-negative-visual-effects/123"), INDEX) == (
        "jobs.jobvite.com",
        "dnve",
    )
    assert drain._match_board(_url("/asus/456"), INDEX) == ("jobs.jobvite.com", "asus")


def test_boards_sharing_a_host_do_not_collapse() -> None:
    """The defect: both postings landed in one bucket, so the total was inflated."""
    first = drain._match_board(_url("/asus/1"), INDEX)
    second = drain._match_board(_url("/double-negative-visual-effects/1"), INDEX)
    assert first != second


def test_a_posting_outside_every_prefix_is_unattributed() -> None:
    """With no root board on the host, a posting under no prefix belongs to nobody."""
    without_root = [row for row in INDEX if row[1]]
    assert drain._match_board(_url("/some/other/board/9"), without_root) is None


def test_a_root_board_absorbs_a_posting_no_prefix_claims() -> None:
    """With a root board present it owns what the more specific boards do not."""
    assert drain._match_board(_url("/some/other/board/9"), INDEX) == ("jobs.jobvite.com", "root")


def test_a_root_board_owns_the_whole_host() -> None:
    only_root = [row for row in INDEX if row[1] == ""]
    assert drain._match_board(_url("/anything/at/all"), only_root) == ("jobs.jobvite.com", "root")


def test_an_ambiguous_root_claim_is_not_guessed() -> None:
    """Two empty-prefix claimants on one host must resolve to neither.

    The shape that produced the false zeros: every URL-less curated provider row lands in
    the index with an empty prefix, and a shared provider host then has dozens of "root"
    claimants. Returning the first credited one greenhouse board with 728 postings while
    the other 50 read zero -- the tenant rule is the only rule that can tell them apart.
    """
    crowd = [
        ("job-boards.greenhouse.io", "", ("job-boards.greenhouse.io", "2k")),
        ("job-boards.greenhouse.io", "", ("job-boards.greenhouse.io", "hasbro")),
    ]
    assert drain._match_board("https://job-boards.greenhouse.io/2k/jobs/1", crowd) is None


def test_the_longest_matching_prefix_wins() -> None:
    """One board can sit under another; the more specific board owns the posting."""
    nested = [
        ("careers.wbd.com", "/careers", ("careers.wbd.com", "all")),
        ("careers.wbd.com", "/careers/locations/milan", ("careers.wbd.com", "milan")),
    ]
    assert drain._match_board("https://careers.wbd.com/careers/locations/milan/j/9", nested) == (
        "careers.wbd.com",
        "milan",
    )
    assert drain._match_board("https://careers.wbd.com/careers/j/9", nested) == (
        "careers.wbd.com",
        "all",
    )


def test_a_prefix_does_not_match_a_sibling_that_merely_starts_with_the_same_text() -> None:
    """`/careers` must not own `/careersearch`."""
    boards = [("x.example", "/careers", ("x.example", "c"))]
    assert drain._match_board("https://x.example/careersearch/1", boards) is None


def test_posting_url_is_read_from_joblink() -> None:
    assert drain._job_posting_url({"jobLink": "https://a.example/job/1"}) == (
        "https://a.example/job/1"
    )


def test_a_bundle_entry_resolves_a_posting_the_url_cannot() -> None:
    job = {
        "jobLink": "https://cdn.example/asset/1",
        "sourceBundle": [{"jobLink": "https://jobs.jobvite.com/asus/1"}],
    }
    assert drain._match_bundle(job, INDEX) == ("jobs.jobvite.com", "asus")


def test_a_bundle_with_nothing_usable_returns_none() -> None:
    assert drain._match_bundle({"jobLink": "https://cdn.example/a/1"}, INDEX) is None
    assert drain._match_bundle({"sourceBundle": "not-a-list"}, INDEX) is None
