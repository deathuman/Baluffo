"""A provider board's own evidence lives in its adapter rollup row's `details`.

Reading only the rollup's totals made every provider board inherit them, which is why 25
boards registered in 0.3.011-0.3.013 read `rollup_only` in the v8 run: 17 greenhouse boards
were asked and returned nothing, and 8 ashby boards errored with "no jobs extracted from ashby
board html". The rollup keeping 477 greenhouse rows says the adapter ran; it never said those
boards were among them.

`run_registry_entries_source` appends one entry per registry entry to the rollup row's
`details`, each with its own status, counts, duration, cache decision and error -- the same
shape a static row has. `fetch_evidence` reads that entry now, matched on the board's registry
id when the adapter records one and otherwise on its tenant, which for every provider platform
*is* the board's slug. Name and studio are never used: the registry carries one board under two
studio labels, and a label-keyed join hands one board's evidence to another.

`rollup_only` is not deleted by this. It stays for a board with no entry of its own (198 of
2,369 in v8), where the rollup really is all the evidence there is.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tools"))


def _load():
    spec = importlib.util.spec_from_file_location(
        "coverage_drain_board_details_under_test", _ROOT / "tools" / "coverage_drain.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cd = _load()


def _source(**fields):
    row = {
        "adapter": "greenhouse",
        "durationMs": 0,
        "fetchedCount": 0,
        "keptCount": 0,
        "details": [],
        "error": "",
        "name": "greenhouse_boards",
    }
    row.update(fields)
    return row


def _report(*rows):
    active = []
    for row in rows:
        adapter = str(row.get("adapter") or "").lower()
        field = cd.ADAPTER_ID_FORMAT.get(adapter, "listing_url")
        value = str(row.get(field) or row.get("listing_url") or "")
        active.append({"id": f"{adapter}:{field}:{value}", "adapter": adapter})
    return cd.registration_report(curated=list(rows), active=active, pending=[])


def _board(url, adapter, **fields):
    row = {"adapter": adapter, "coverageAuditOpenings": 1, "listing_url": url, "name": url}
    row.update(fields)
    return row


def _write(tmp_path, sources):
    (tmp_path / cd.FETCH_REPORT).write_text(json.dumps({"sources": sources}), encoding="utf-8")


def test_a_board_asked_and_empty_is_reported_as_asked_and_empty(tmp_path):
    """The v8 shape for 17 greenhouse boards: asked, 112-713 ms, nothing fetched, no error."""
    board = _board("https://job-boards.greenhouse.io/taketwo", "greenhouse", slug="taketwo")
    report = _report(board)
    _write(
        tmp_path,
        [
            _source(
                durationMs=9000,
                fetchedCount=479,
                keptCount=479,
                details=[
                    {
                        "slug": "atariinc",
                        "status": "ok",
                        "durationMs": 179,
                        "fetchedCount": 2,
                        "keptCount": 2,
                    },
                    {
                        "slug": "taketwo",
                        "status": "ok",
                        "durationMs": 191,
                        "fetchedCount": 0,
                        "keptCount": 0,
                        "cacheDecision": "run_now",
                    },
                ],
            )
        ],
    )
    row = cd.fetch_evidence(tmp_path, report)[cd.report_identity(report[0])]
    assert row["state"] == "fetched_empty"
    assert row["duration_ms"] == 191
    assert row["rollup"] == "greenhouse_boards"


def test_a_board_erroring_on_its_own_entry_is_unknown_not_empty(tmp_path):
    """The other v8 shape: 8 ashby boards, "no jobs extracted", browser fallback recommended."""
    board = _board(
        "https://jobs.ashbyhq.com/supercell",
        "ashby",
        board_url="https://jobs.ashbyhq.com/supercell",
    )
    report = _report(board)
    _write(
        tmp_path,
        [
            _source(
                adapter="ashby",
                name="ashby_sources",
                durationMs=4200,
                fetchedCount=141,
                keptCount=140,
                details=[
                    {
                        "slug": "supercell",
                        "status": "error",
                        "durationMs": 0,
                        "fetchedCount": 0,
                        "keptCount": 0,
                        "error": "no jobs extracted from ashby board html",
                    }
                ],
            )
        ],
    )
    row = cd.fetch_evidence(tmp_path, report)[cd.report_identity(report[0])]
    assert row["state"] == "unknown", "no jobs extracted with zero kept is undecidable"
    assert row["error_kind"] == "no_openings"


def test_a_board_with_no_entry_of_its_own_still_reports_rollup_only(tmp_path):
    """The fallback the fix must not erase: no entry means the rollup is all there is."""
    board = _board(
        "https://job-boards.greenhouse.io/studioghibli", "greenhouse", slug="studioghibli"
    )
    report = _report(board)
    _write(
        tmp_path,
        [
            _source(
                durationMs=9000,
                fetchedCount=479,
                keptCount=479,
                details=[
                    {"slug": "someone-else", "durationMs": 10, "fetchedCount": 1, "keptCount": 1}
                ],
            )
        ],
    )
    row = cd.fetch_evidence(tmp_path, report)[cd.report_identity(report[0])]
    assert row["state"] == "rollup_only"
    assert row["rollup"] == "greenhouse_boards"


def test_two_entries_sharing_a_slug_are_not_resolved(tmp_path):
    """A slug claimed twice is a platform ambiguity, not a licence to pick one."""
    board = _board("https://job-boards.greenhouse.io/dup", "greenhouse", slug="dup")
    report = _report(board)
    _write(
        tmp_path,
        [
            _source(
                durationMs=9000,
                fetchedCount=479,
                keptCount=479,
                details=[
                    {"slug": "dup", "durationMs": 10, "fetchedCount": 0, "keptCount": 0},
                    {"slug": "dup", "durationMs": 20, "fetchedCount": 5, "keptCount": 5},
                ],
            )
        ],
    )
    assert (
        cd.fetch_evidence(tmp_path, report)[cd.report_identity(report[0])]["state"] == "rollup_only"
    )


def test_boards_sharing_a_studio_label_keep_their_own_evidence(tmp_path):
    """The registry carries one board under two labels; labels must not swap evidence."""
    first = _board(
        "https://job-boards.greenhouse.io/alpha", "greenhouse", slug="alpha", studio="Shared Label"
    )
    second = _board(
        "https://job-boards.greenhouse.io/beta", "greenhouse", slug="beta", studio="Shared Label"
    )
    report = _report(first, second)
    _write(
        tmp_path,
        [
            _source(
                durationMs=9000,
                fetchedCount=479,
                keptCount=479,
                details=[
                    {
                        "slug": "alpha",
                        "studio": "Shared Label",
                        "durationMs": 90,
                        "fetchedCount": 0,
                        "keptCount": 0,
                    },
                    {
                        "slug": "beta",
                        "studio": "Shared Label",
                        "durationMs": 88,
                        "fetchedCount": 3,
                        "keptCount": 3,
                    },
                ],
            )
        ],
    )
    evidence = cd.fetch_evidence(tmp_path, report)
    assert evidence[cd.report_identity(report[0])]["state"] == "fetched_empty"
    assert evidence[cd.report_identity(report[1])]["state"] == "collected"


def test_a_registry_id_on_the_entry_wins_over_the_slug(tmp_path):
    """An adapter that records `sourceId` is matched exactly, even if the slug differs."""
    board = _board(
        "https://jobs.ashbyhq.com/supercell",
        "ashby",
        board_url="https://jobs.ashbyhq.com/supercell",
    )
    _report(board)
    # Exercised on the helper rather than through `fetch_evidence`, because
    # `registration_report` projects a fixed shape that drops `id` -- so within the drain's own
    # flow the tenant is always the effective key. Callers that pass rows carrying their
    # registry id (the v8 attribution script does) get exact matching through this branch.
    details = [
        {
            "slug": "not-this-board",
            "sourceId": "ashby:board_url:https://jobs.ashbyhq.com/supercell",
            "durationMs": 12,
            "fetchedCount": 2,
            "keptCount": 2,
        },
        {"slug": "supercell", "durationMs": 0, "fetchedCount": 0, "keptCount": 0},
    ]
    matched = cd._rollup_detail_for_board(
        details,
        board_id="ashby:board_url:https://jobs.ashbyhq.com/supercell",
        tenant="supercell",
    )
    assert matched is not None and matched["slug"] == "not-this-board", "the id-matched entry wins"
