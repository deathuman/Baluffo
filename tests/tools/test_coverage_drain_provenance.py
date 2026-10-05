"""Attribution must follow provenance, not where a posting URL lives.

The delivery report answers "how many openings did each curated board collect". It first
answered that by matching every output row's `jobLink` against each board's host and path.
That produced 70 "n-ix jobs" for a board whose own fetch kept **zero** — they were
Google-Sheet rows whose posting links happened to point at `careers.n-ix.com`. A sheet row
posted by a studio is not the board collecting anything, and the number was wrong by the
entire size of the sheet.

A job already records the source that fetched it:

- a **static** board's postings carry that board's own source id, so the mapping is exact
  and needs no URL matching;
- a **provider** adapter serves every board through one rollup row (`workday_sources`), so
  the bundle's posting URL is matched against the board's path prefix — but only among jobs
  that rollup fetched.

Matching a board against the whole output regardless of source is the failure this pins.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tools"))


def _load():
    spec = importlib.util.spec_from_file_location(
        "coverage_drain_provenance_under_test", _ROOT / "tools" / "coverage_drain.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


drain = _load()

REPORT = [
    {
        "adapter": "static",
        "listing_url": "https://careers.n-ix.com",
        "registered": True,
    },
    {
        "adapter": "static",
        "listing_url": "https://hrmos.co/pages/capcom/jobs",
        "registered": True,
    },
    {
        "adapter": "workday",
        "listing_url": "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite",
        "registered": True,
    },
]

NIX_KEY = ("careers.n-ix.com", "careers.n-ix.com")
CAPCOM_KEY = ("hrmos.co", "capcom")
NVIDIA_KEY = ("nvidia.wd5.myworkdayjobs.com", "nvidiaexternalcareersite")


def test_static_source_name_matches_the_registry_key(tmp_path: Path) -> None:
    assert drain.static_source_name("https://careers.n-ix.com") == (
        "static_source::static:listing_url:https://careers.n-ix.com"
    )


def test_a_sheet_row_pointing_at_a_board_is_not_that_board(tmp_path: Path) -> None:
    """The defect: 70 'n-ix jobs' that n-ix never fetched."""
    sheet_job = {
        "source": "google_sheets",
        "jobLink": "https://careers.n-ix.com/jobs/123",
        "title": "Engineer",
    }
    counts, unmatched = drain.attribute_collected(_payload([sheet_job], tmp_path), REPORT)
    assert NIX_KEY not in counts, "a sheet row must not be credited to the board"
    assert unmatched == 1


def test_a_static_board_is_credited_from_its_own_source(tmp_path: Path) -> None:
    job = {
        "source": drain.static_source_name("https://hrmos.co/pages/capcom/jobs"),
        "jobLink": "https://hrmos.co/pages/capcom/jobs/01_01_010",
        "title": "Planner",
    }
    counts, unmatched = drain.attribute_collected(_payload([job], tmp_path), REPORT)
    assert counts.get(CAPCOM_KEY) == 1
    assert unmatched == 0


def test_a_provider_job_is_matched_inside_its_own_rollup(tmp_path: Path) -> None:
    job = {
        "source": "workday_sources",
        "jobLink": "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite/job/x/1",
        "sourceBundle": [
            {"jobLink": "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite/job/x/1"}
        ],
    }
    counts, _ = drain.attribute_collected(_payload([job], tmp_path), REPORT)
    assert counts.get(NVIDIA_KEY) == 1


def test_a_provider_job_for_an_unregistered_board_is_unattributed(tmp_path: Path) -> None:
    job = {
        "source": "workday_sources",
        "jobLink": "https://unknown.wd5.myworkdayjobs.com/OtherSite/job/y/1",
        "sourceBundle": [{"jobLink": "https://unknown.wd5.myworkdayjobs.com/OtherSite/job/y/1"}],
    }
    counts, unmatched = drain.attribute_collected(_payload([job], tmp_path), REPORT)
    assert counts == {}
    assert unmatched == 1


def test_a_workday_job_never_credits_a_static_board_on_the_same_host(tmp_path: Path) -> None:
    """Cross-adapter leakage: the shape that made the first numbers unreadable."""
    job = {
        "source": "workday_sources",
        "jobLink": "https://careers.n-ix.com/jobs/999",
        "sourceBundle": [{"jobLink": "https://careers.n-ix.com/jobs/999"}],
    }
    counts, _ = drain.attribute_collected(_payload([job], tmp_path), REPORT)
    assert NIX_KEY not in counts


def test_unregistered_boards_are_never_credited(tmp_path: Path) -> None:
    report = [
        *REPORT,
        {"adapter": "static", "listing_url": "https://x.example/jobs", "registered": False},
    ]
    job = {
        "source": drain.static_source_name("https://x.example/jobs"),
        "jobLink": "https://x.example/jobs/1",
    }
    counts, unmatched = drain.attribute_collected(_payload([job], tmp_path), report)
    assert counts == {}
    assert unmatched == 1


def _payload(rows: list[dict[str, Any]], tmp_path: Path) -> Path:
    """A data dir holding `rows` as the fetch's real gzipped output.

    Written for real rather than stubbed, because the reader's gzip handling is itself part
    of what these tests cover: reading a bare `.json` name is what silently reported zero
    collection while 41,277 rows sat in the gzipped file.
    """
    import gzip
    import json

    (tmp_path / "jobs-unified.json.gz").write_bytes(gzip.compress(json.dumps(rows).encode("utf-8")))
    return tmp_path


def _payload_impl(rows: list[dict[str, Any]]) -> Any:
    raise AssertionError("use _payload(rows, tmp_path) -- write a real gzipped output")
