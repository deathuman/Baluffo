"""The 504 coverage-audit boards are curated data, and the loader must not break discovery.

Two things are pinned here.

**The loader is fail-safe.** A missing or malformed catalogue has to yield an empty
list, not an exception. ``STATIC_DISCOVERY_CANDIDATES`` is evaluated at import time,
so a loader that raises takes the whole discovery stage down with it, and it takes
the studio seed catalogue's failure mode with it: the sibling
``load_studio_seeds`` falls back to its inline defaults rather than propagating.

**The rows are auditable.** Each one exists because a specific missed opening pointed
at that board, and carries the opening count it was added for. Without that,
provenance a future sweep cannot tell a board added on evidence from one added by
guess, and cannot tell whether an addition was worth what it claimed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.source_discovery.config import (
    COVERAGE_BOARDS_PATH,
    STATIC_DISCOVERY_CANDIDATES,
    load_curated_coverage_boards,
)

_ROOT = Path(__file__).resolve().parents[2]

# Board locator fields, by adapter. greenhouse keys on ``slug`` and
# SmartRecruiters on ``company_id``, so a key that only looks at URLs collapses
# every SmartRecruiters row onto one identity.
_LOCATORS = ("slug", "account", "company_id", "board_url", "api_url", "listing_url")


def test_the_catalogue_file_exists_and_parses() -> None:
    assert COVERAGE_BOARDS_PATH.exists(), f"{COVERAGE_BOARDS_PATH} is missing"
    payload = json.loads(COVERAGE_BOARDS_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    assert payload, "the catalogue shipped empty"


def test_every_row_carries_an_adapter_a_studio_and_a_board_locator() -> None:
    """A row with no locator becomes a board nothing ever fetches."""
    for row in load_curated_coverage_boards():
        assert row.get("adapter"), row
        assert row.get("studio"), row
        assert any(row.get(f) for f in _LOCATORS), f"no board locator in {row}"


def test_every_row_records_the_openings_it_was_added_for() -> None:
    """Provenance is what makes a future sweep able to audit the addition."""
    for row in load_curated_coverage_boards():
        assert isinstance(row.get("coverageAuditOpenings"), int), row
        assert row["coverageAuditOpenings"] >= 1, row


def test_rows_are_deduplicated_by_board_url() -> None:
    """Two rows for one board means it is fetched twice and double-counted."""
    seen: set[tuple[str, str]] = set()
    for row in load_curated_coverage_boards():
        locator = next((str(row[f]) for f in _LOCATORS if row.get(f)), "")
        key = (str(row["adapter"]), locator)
        assert locator, f"no locator in {row}"
        assert key not in seen, f"duplicate board {key}"
        seen.add(key)


def test_a_missing_catalogue_yields_no_rows_rather_than_raising(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """STATIC_DISCOVERY_CANDIDATES is built at import time from this loader."""
    monkeypatch.setattr(
        "src.source_discovery.config.COVERAGE_BOARDS_PATH", tmp_path / "does-not-exist.json"
    )
    assert load_curated_coverage_boards() == []


def test_a_malformed_catalogue_yields_no_rows_rather_than_raising(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bad = tmp_path / "curated_coverage_boards.json"
    bad.write_text("{not json", encoding="utf-8")
    monkeypatch.setattr("src.source_discovery.config.COVERAGE_BOARDS_PATH", bad)
    assert load_curated_coverage_boards() == []


def test_a_non_list_catalogue_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bad = tmp_path / "curated_coverage_boards.json"
    bad.write_text('{"rows": []}', encoding="utf-8")
    monkeypatch.setattr("src.source_discovery.config.COVERAGE_BOARDS_PATH", bad)
    assert load_curated_coverage_boards() == []


def test_rows_missing_an_adapter_or_studio_are_dropped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A half-formed row would stage a candidate with nothing to fetch."""
    path = tmp_path / "curated_coverage_boards.json"
    path.write_text(
        json.dumps(
            [
                {"studio": "Good", "adapter": "greenhouse", "slug": "good"},
                {"adapter": "greenhouse", "slug": "no-studio"},
                {"studio": "No adapter", "slug": "no-adapter"},
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.source_discovery.config.COVERAGE_BOARDS_PATH", path)
    assert [r["studio"] for r in load_curated_coverage_boards()] == ["Good"]


def test_nl_priority_defaults_to_false() -> None:
    """Dutch-priority flags are hand-set, so an absent one must not inherit True."""
    for row in load_curated_coverage_boards():
        assert row["nlPriority"] is False


def test_the_hand_curated_rows_are_still_present() -> None:
    """The data file extends the list, it does not replace it."""
    studios = {str(row.get("studio")) for row in STATIC_DISCOVERY_CANDIDATES}
    assert {"Voodoo", "CD PROJEKT RED"} <= studios


def test_no_board_is_registered_twice() -> None:
    """Four boards were in both tables after the first landing.

    Voodoo, 2K Czech, Hangar 13 and Yggdrasil were already in the hand-curated
    literal from earlier work, and the audit re-proposed them because the audit
    dedupes against the *live registry*, which does not carry unreleased rows. Two
    rows for one board means it is fetched twice and double-counted, so the
    duplicates were removed from the data file and their opening counts moved onto
    the surviving hand-curated rows.
    """
    seen: set[tuple[str, str]] = set()
    for row in STATIC_DISCOVERY_CANDIDATES:
        locator = next((str(row[f]) for f in _LOCATORS if row.get(f)), "")
        key = (str(row["adapter"]), locator)
        assert locator, f"no locator in {row}"
        assert key not in seen, f"board registered twice: {key}"
        seen.add(key)


def test_provenance_survives_where_the_duplicate_was_removed() -> None:
    """The opening count must not vanish with the row it was attached to."""
    by_studio = {str(r.get("studio")): r for r in STATIC_DISCOVERY_CANDIDATES}
    for studio, openings in (("Voodoo", 90), ("2K Czech", 7), ("Hangar 13", 9), ("Yggdrasil", 3)):
        row = by_studio.get(studio)
        assert row is not None, studio
        assert row.get("coverageAuditOpenings") == openings, studio


def test_the_adapter_mix_matches_what_the_audit_verified() -> None:
    """A regression guard on the shape of the addition, not a pin on exact counts.

    The audit verified 270 boards that return rows and registered 234 more that
    render in a browser; static boards dominate because scraped careers pages are
    the long tail. If this inverts, the data file has been corrupted.
    """
    counts: dict[str, int] = {}
    for row in load_curated_coverage_boards():
        counts[row["adapter"]] = counts.get(row["adapter"], 0) + 1
    assert counts["static"] == max(counts.values()), counts
    for adapter in ("greenhouse", "ashby", "lever", "workday", "smartrecruiters"):
        assert counts.get(adapter, 0) > 0, f"{adapter} missing from the catalogue: {counts}"


def test_no_row_targets_a_vendor_without_an_adapter() -> None:
    """hrmos, feishu and csod have no adapter; registering them would collect nothing.

    They are a counted backlog instead -- 1,689 openings unreachable until an
    adapter exists -- and must not appear here as if they were handled.
    """
    unreadable = {"hrmos", "feishu", "csod", "hirentalent"}
    for row in load_curated_coverage_boards():
        assert row["adapter"] not in unreadable, row
