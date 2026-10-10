"""Parity is the acceptance test for replay, and it has to be run through ``run_pipeline``.

A harness that only round-trips a single URL proves nothing about the lane: the pipeline
selects sources, executes them, canonicalises rows and writes a report, and a replay that
diverges anywhere in that chain must fail rather than produce a plausible-looking report.
So the load-bearing test here records a real run against a **loopback server**, stops the
server, replays under the armed egress guard, and requires the same row set -- with
``guard.blocked == 0`` as the proof that nothing slipped back onto the network.

The rest of the file pins the driver's pure pieces (selection, identity digest, parity
comparison), because those decide *what a replay is compared against* and a silent change
in them would move the bar without touching a test.
"""

from __future__ import annotations

import gzip
import json
import sys
from http.client import InvalidURL
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.pipeline import run_pipeline  # noqa: E402
from src.shared import json_io  # noqa: E402
from tests.tools.test_corpus_capture import _surface_loader, local_http_server  # noqa: E402
from tools.measurements.corpus import harness, run_fetch  # noqa: E402
from tools.measurements.corpus.capture import (  # noqa: E402
    CaptureRecorder,
    CorpusReplayer,
    install_egress_guard,
    raise_for_entry,
)
from tools.measurements.corpus.store import CorpusStore, RequestLedger  # noqa: E402


def _rows(output_dir: Path) -> list[dict[str, Any]]:
    # Through the product's gzip-aware reader: the run writes jobs-unified.json.gz.
    rows = json_io.read_json(output_dir / "jobs-unified.json", [])
    assert isinstance(rows, list)
    return rows


def test_run_pipeline_record_then_replay_is_row_and_identity_parity(tmp_path: Path) -> None:
    corpus = CorpusStore(tmp_path / "corpus")
    record_ledger = RequestLedger()
    record_dir = tmp_path / "record" / "data"
    replay_dir = tmp_path / "replay" / "data"

    with local_http_server() as base:
        loader = _surface_loader(base)
        with CaptureRecorder(corpus, scope="parity", ledger=record_ledger) as recorder:
            with run_fetch.browser_fallback_disabled():
                run_pipeline(
                    output_dir=record_dir,
                    run_id="record",
                    source_loaders=[("loopback", loader)],
                    fetch_text=recorder.fetch_text,
                    force_refresh_all=True,
                    browser_fallback_max_workers=0,
                    max_workers=1,
                    show_progress=False,
                )
        recorded_summary = run_fetch.run_summary(record_dir)

    assert recorded_summary["rowCount"] == 1
    assert record_ledger.attribution_is_complete()

    replay_ledger = RequestLedger()
    guard = install_egress_guard()
    with guard:
        with CorpusReplayer(corpus, scope="parity", ledger=replay_ledger) as replayer:
            with run_fetch.browser_fallback_disabled():
                run_pipeline(
                    output_dir=replay_dir,
                    run_id="replay",
                    source_loaders=[("loopback", loader)],
                    fetch_text=replayer.fetch_text,
                    force_refresh_all=True,
                    browser_fallback_max_workers=0,
                    max_workers=1,
                    show_progress=False,
                )
    replayed_summary = run_fetch.run_summary(replay_dir)

    assert guard.blocked == 0, f"replay reached a socket: {guard.targets}"
    assert replay_ledger.misses == 0
    assert run_fetch.parity_matches(recorded_summary, replayed_summary) == {
        "rowCount": True,
        "identityDigest": True,
        "rowProjectionDigest": True,
        "sourceStatusCounts": True,
    }
    # Row-for-row equality is *not* the bar: ``id``/``fetchedAt``/``firstSeenAt`` differ
    # between any two real runs. The field projection is, and it must be non-empty, so this
    # cannot pass vacuously on a replay that wrote no rows.
    recorded_projection = run_fetch.row_projection(_rows(record_dir))
    assert recorded_projection == run_fetch.row_projection(_rows(replay_dir))
    assert [row["title"] for row in recorded_projection] == ["Senior Engineer"]
    assert run_fetch.job_identities(_rows(replay_dir)) == [
        "https://boards.example.com/jobs/senior-engineer"
    ]


def test_select_loaders_is_deterministic_and_strided() -> None:
    statics = [
        (f"static_source::static:listing_url:https://boards{i}.example/jobs", object())
        for i in range(40)
    ]
    families = [("google_sheets", object()), ("greenhouse_boards", object())]
    all_loaders = [*families, *statics]

    chosen = run_fetch.select_loaders(all_loaders, families=("google_sheets",), static_sources=5)

    assert [name for name, _ in chosen[:1]] == ["google_sheets"]
    picked = [name for name, _ in chosen[1:]]
    assert len(picked) == 5
    # A stride, not the first five: alphabetical registry order would otherwise sample one
    # hosting era.
    assert picked != [name for name, _ in statics[:5]]
    assert chosen == run_fetch.select_loaders(
        all_loaders, families=("google_sheets",), static_sources=5
    )

    only = run_fetch.select_loaders(all_loaders, only_sources=("greenhouse_boards",))
    assert [name for name, _ in only] == ["greenhouse_boards"]
    assert run_fetch.select_loaders(all_loaders, families=("nope",), static_sources=0) == []


def test_all_sources_selects_the_whole_registry_and_supersedes_the_sample() -> None:
    """A reference epoch covers every registered source, not a counted sample of them.

    The families and the stride size are still *passed* (they have CLI defaults), so this also
    pins that ``all_sources`` supersedes them rather than quietly intersecting with them.
    """

    statics = [
        (f"static_source::static:listing_url:https://boards{i}.example/jobs", object())
        for i in range(40)
    ]
    families = [("google_sheets", object()), ("greenhouse_boards", object())]
    all_loaders = [*families, *statics]

    chosen = run_fetch.select_loaders(
        all_loaders,
        families=("google_sheets",),
        static_sources=5,
        all_sources=True,
    )

    assert chosen == all_loaders
    assert [name for name, _ in chosen] == [name for name, _ in all_loaders]
    # The two selectors cannot be combined on the CLI: a silently ignored flag would let a
    # "full" epoch be recorded from a sample.
    with pytest.raises(SystemExit):
        run_fetch.build_parser().parse_args(
            ["--mode", "record", "--only-sources", "google_sheets", "--all-sources"]
        )


def test_a_recorded_invalid_url_replays_as_its_own_class() -> None:
    """A malformed-URL failure replays as ``InvalidURL``, not as a class a caller cannot match."""
    entry = {
        "outcome": "failure",
        "url": 'https://boards.example.com/careers/" + root + "/job',
        "error": {"class": "InvalidURL", "message": "control characters"},
    }
    with pytest.raises(InvalidURL):
        raise_for_entry(entry)


def test_row_projection_ignores_run_stamps_and_keeps_job_fields() -> None:
    first = [
        {
            "id": 1,
            "title": "Senior Engineer",
            "jobLink": "https://a.example/1",
            "fetchedAt": "2026-10-08T18:00:00+00:00",
            "firstSeenAt": "2026-10-08T18:00:01+00:00",
            "lastSeenAt": "2026-10-08T18:00:01+00:00",
        }
    ]
    second = [dict(first[0], id=7, fetchedAt="2026-10-09T04:00:00+00:00", firstSeenAt="x")]

    assert run_fetch.row_projection(first) == run_fetch.row_projection(second)
    assert run_fetch.row_projection(first) == [
        {
            "title": "Senior Engineer",
            "company": "",
            "city": "",
            "country": "",
            "workType": "",
            "contractType": "",
            "jobLink": "https://a.example/1",
            "sector": "",
            "profession": "",
            "source": "",
            "sourceJobId": "",
            "status": "",
            "postedAt": "",
        }
    ]
    # A changed job field must move the projection, or it proves nothing.
    assert run_fetch.row_projection(second) != run_fetch.row_projection(
        [dict(second[0], title="Different")]
    )
    assert run_fetch.row_projection("not a list") == []


def test_job_identities_and_digest_pin_the_parity_unit() -> None:
    rows = [
        {"jobLink": "https://b.example/2", "title": "T", "company": "C"},
        {"title": "Only Title", "company": "Only Company"},
        {"jobLink": "https://a.example/1"},
    ]

    identities = run_fetch.job_identities(rows)

    # Sorted, and by code point: the digest is the parity unit, so a replay emitting the same
    # jobs in another order must not read as a mismatch.
    assert identities == [
        "Only Company|Only Title",
        "https://a.example/1",
        "https://b.example/2",
    ]
    assert run_fetch.identity_digest(["b", "a"]) == run_fetch.identity_digest(["a", "b"])
    assert run_fetch.identity_digest(identities) != run_fetch.identity_digest(identities[:-1])
    assert run_fetch.job_identities("not a list") == []


def test_run_summary_reads_rows_and_source_statuses(tmp_path: Path) -> None:
    output_dir = tmp_path / "data"
    output_dir.mkdir()
    (output_dir / "jobs-unified.json").write_text(
        json.dumps([{"jobLink": "https://a.example/1"}, {"title": "T", "company": "C"}]),
        encoding="utf-8",
    )
    (output_dir / "jobs-fetch-report.json").write_text(
        json.dumps({"sources": [{"name": "a", "status": "ok"}, {"name": "b", "status": "error"}]}),
        encoding="utf-8",
    )

    summary = run_fetch.run_summary(output_dir)

    assert summary["rowsFile"] == "jobs-unified.json"
    assert summary["reportFile"] == "jobs-fetch-report.json"
    assert summary["rowCount"] == 2
    assert summary["sourceStatusCounts"] == {"error": 1, "ok": 1}
    written = json.loads((output_dir / "jobs-unified.json").read_text(encoding="utf-8"))
    assert summary["rowProjectionDigest"] == run_fetch.identity_digest(
        [json.dumps(item, sort_keys=True) for item in run_fetch.row_projection(written)]
    )
    assert summary["identityDigest"] == run_fetch.identity_digest(["https://a.example/1", "C|T"])
    # An empty output directory is reported as such rather than raising.
    empty = run_fetch.run_summary(tmp_path / "missing")
    assert empty["rowCount"] == 0 and empty["rowsFile"] == ""


def test_the_stored_summary_digests_the_projection_instead_of_copying_it(tmp_path: Path) -> None:
    """A stored sample must carry the projection *digest*, never the projected rows.

    Every consumer compares four digests. An earlier version attached the whole projection too,
    which put 11.8 MB of restated rows into each determinism sample -- a 53 MB receipt for a
    three-sample check, and the largest artifact this harness wrote.
    """

    output_dir = tmp_path / "data"
    output_dir.mkdir()
    (output_dir / "jobs-unified.json").write_text(
        json.dumps([{"jobLink": "https://a.example/1", "title": "T", "company": "C"}]),
        encoding="utf-8",
    )

    summary = harness.run_summary(tmp_path)

    assert "projection" not in summary
    assert summary["rowCount"] == 1
    assert summary["rowProjectionDigest"] == run_fetch.identity_digest(
        [json.dumps(item, sort_keys=True) for item in run_fetch.row_projection(_rows(output_dir))]
    )


def test_run_summary_reads_the_gzip_backed_rows_artifact(tmp_path: Path) -> None:
    """A real run writes ``jobs-unified.json.gz`` (``src/shared/json_io.py``).

    Reading only the plain name made the driver report zero rows for a run that had kept one --
    a confident zero, which is the failure this test now pins.
    """

    output_dir = tmp_path / "data"
    output_dir.mkdir()
    with gzip.open(output_dir / "jobs-unified.json.gz", mode="wt", encoding="utf-8") as handle:
        json.dump([{"jobLink": "https://a.example/1"}], handle)

    summary = run_fetch.run_summary(output_dir)

    assert summary["rowsFile"] == "jobs-unified.json.gz"
    assert summary["rowCount"] == 1


def test_guard_blocked_sources_are_named_from_the_report() -> None:
    """Row parity can hold while a source fails on the guard, so the driver names those sources."""

    sources: list[Any] = [
        {"name": "zz_ok", "error": ""},
        {"name": "b_uncovered", "error": "x: egress blocked during replay: 127.0.0.1:5"},
        {"name": "a_uncovered", "error": "egress blocked during replay: 127.0.0.1:6"},
        {"name": "c_missing", "error": "corpus miss for https://x/y"},
        {"name": "", "error": "egress blocked during replay: 127.0.0.1:7"},
        "not a mapping",
    ]

    assert run_fetch.guard_blocked_source_names({"sources": sources}) == [
        "a_uncovered",
        "b_uncovered",
    ]
    assert run_fetch.guard_blocked_source_names({}) == []


def test_parity_comparison_needs_every_unit_to_match() -> None:
    recorded = {
        "rowCount": 3,
        "identityDigest": "abc",
        "rowProjectionDigest": "p1",
        "sourceStatusCounts": {"ok": 1},
    }
    same = dict(recorded)

    assert run_fetch.parity_matches(recorded, same) == {
        "rowCount": True,
        "identityDigest": True,
        "rowProjectionDigest": True,
        "sourceStatusCounts": True,
    }
    # Same count, different jobs: the case a row-count-only check would wave through.
    assert run_fetch.parity_matches(recorded, dict(same, identityDigest="def")) == {
        "rowCount": True,
        "identityDigest": False,
        "rowProjectionDigest": True,
        "sourceStatusCounts": True,
    }
    assert run_fetch.parity_matches(recorded, dict(same, rowCount=2))["rowCount"] is False
    # Same jobs, drifted fields: only the projection unit sees this.
    assert (
        run_fetch.parity_matches(recorded, dict(same, rowProjectionDigest="p2"))[
            "rowProjectionDigest"
        ]
        is False
    )
    # Same rows but a different set of sources failing: a fidelity failure of its own, and the
    # reason this fourth unit exists (it caught a guard-caused source failure on a real replay).
    assert (
        run_fetch.parity_matches(recorded, dict(same, sourceStatusCounts={"error": 1}))[
            "sourceStatusCounts"
        ]
        is False
    )
    # A unit absent on either side is a mismatch, not a free pass.
    assert (
        run_fetch.parity_matches(
            {"rowCount": 3, "identityDigest": "abc"}, {"rowCount": 3, "identityDigest": "abc"}
        )["rowProjectionDigest"]
        is False
    )
    assert run_fetch.parity_matches({}, {}) == {
        "rowCount": True,
        "identityDigest": True,
        "rowProjectionDigest": False,
        "sourceStatusCounts": False,
    }


def test_driver_parses_modes_and_surfaces() -> None:
    parser = run_fetch.build_parser()
    record = parser.parse_args(["--mode", "record"])

    assert record.scope == "subset"
    assert record.sources == run_fetch.DEFAULT_STATIC_SOURCES
    assert record.families == "google_sheets"
    assert record.max_workers == 1
    assert run_fetch._selected_surfaces("") == ("fetch_text", "urlopen", "httpx")
    assert run_fetch._selected_surfaces("httpx,") == ("fetch_text", "urlopen")
    with pytest.raises(SystemExit):
        parser.parse_args(["--mode", "sideways"])


def test_run_tokens_follow_the_repo_convention() -> None:
    token = run_fetch.new_run_token("record")

    assert token.startswith("record-")
    stamp = token.split("-", 1)[1]
    assert len(stamp) == len("20261008-180000")
    assert stamp[8] == "-"
    assert run_fetch.DEFAULT_CORPUS_PARENT.name == "perf-runs"
