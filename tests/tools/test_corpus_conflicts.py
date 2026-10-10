"""A record that answered one request key twice says so, per host, and still certifies.

The conflict audit exists because a corpus can hold only one response per request key, and the
measured case -- LinkedIn's per-render ``refId``/``trackingId`` tokens -- is not something the run
chose: four sources fetched one browse page and were served four different renders. Three seams
carry the answer, and each is pinned here rather than described: the store writes a replacement
down the moment it overwrites an entry (the only witness left, because after the write the
previous entry is gone); the audit joins those lines to the orphaned bodies on disk and reports the
count per host, with the orphan count still measured when no log exists; and the record carries the
declaration in ``EPOCH.json`` beside the verdict instead of refusing the epoch for it.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.helpers.local_http_server import local_http_server  # noqa: E402
from tests.tools.test_corpus_birth_verification import _record_args  # noqa: E402
from tools.measurements.corpus import conflicts, run_fetch  # noqa: E402
from tools.measurements.corpus.store import CorpusStore  # noqa: E402

_JOB_URL = "https://boards.example.com/jobs/senior-engineer"


def _store(tmp_path: Path) -> CorpusStore:
    return CorpusStore(tmp_path / "corpus")


def _ok(store: CorpusStore, url: str, text: str) -> dict[str, Any]:
    return store.record(url=url, outcome="ok", text=text, status_code=200, surface="fetch_text")


def _failure(store: CorpusStore, url: str) -> dict[str, Any]:
    return store.record(
        url=url, outcome="failure", error=RuntimeError("network error"), surface="fetch_text"
    )


def test_a_replaced_response_is_written_down_with_both_bodies(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _ok(store, "https://boards.example.com/jobs", "render one")
    second = _ok(store, "https://boards.example.com/jobs", "render two")

    (line,) = store.replacement_records()
    assert line["kind"] == "body-changed"
    assert line["url"] == "https://boards.example.com/jobs"
    assert line["previousBodySha256"] == first["bodySha256"]
    assert line["bodySha256"] == second["bodySha256"]

    # A re-record of the same bytes replaced nothing a replay could tell apart, so it is not a
    # replacement -- the log is an account of differences, not of requests.
    _ok(store, "https://boards.example.com/jobs", "render two")
    assert len(store.replacement_records()) == 1


def test_a_failure_replacing_a_response_is_reported_as_a_lost_response(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _ok(store, "https://boards.example.com/jobs", "render one")
    _failure(store, "https://boards.example.com/jobs")

    report = conflicts.audit(corpus_dir=store.root)
    assert report["status"] == "flagged"
    assert report["replacedKeys"] == 1
    assert report["replacementsByKind"] == {"response-lost": 1}
    assert report["orphanedBodies"] == 1
    assert report["attributedOrphans"] == 1 and report["unattributedOrphans"] == 0
    assert report["replacementLog"] == "present"

    (finding,) = report["hosts"]
    assert finding["host"] == "boards.example.com"
    assert finding["replacedKeys"] == 1
    assert finding["orphanedBodies"] == 1
    assert finding["samples"] == ["https://boards.example.com/jobs"]
    assert finding["kinds"] == {"response-lost": 1}


def test_a_clean_corpus_declares_nothing(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _ok(store, "https://a.example.com/jobs", "one")
    _ok(store, "https://b.example.com/jobs", "two")

    report = conflicts.audit(corpus_dir=store.root)
    assert report["status"] == "clean"
    assert report["replacedKeys"] == 0 and report["orphanedBodies"] == 0
    assert report["hosts"] == [] and report["hostCount"] == 0
    assert conflicts.referee_note(report) is None
    assert conflicts.warning_lines(report) == []

    # And a clean audit leaves the certification exactly as truncation wrote it.
    verdict = {"eligible": True, "accepted": False, "blocking": [], "knobs": [], "notes": []}
    assert conflicts.carry(verdict, report) == verdict


def test_the_note_declares_the_class_without_refusing_the_record(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _ok(store, "https://www.linkedin.com/company/x/jobs/", "render one")
    _ok(store, "https://www.linkedin.com/company/x/jobs/", "render two")
    report = conflicts.audit(corpus_dir=store.root)

    note = conflicts.referee_note(report)
    assert note is not None
    assert note["reason"] == conflicts.NOTE_REASON
    assert note["hosts"] == ["www.linkedin.com"]
    assert note["hostCount"] == 1
    assert note["replacedKeys"] == 1 and note["orphanedBodies"] == 1
    assert "keep-first" in note["detail"] and "no knob" not in note["detail"]

    carried = conflicts.carry(
        {"eligible": True, "accepted": False, "blocking": [], "knobs": [], "notes": []}, report
    )
    assert carried["eligible"] is True and carried["blocking"] == []
    assert carried["notes"] == [note]


def test_each_host_is_named_with_its_own_counts(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _ok(store, "https://a.example.com/one", "a1")
    _ok(store, "https://a.example.com/one", "a2")
    _ok(store, "https://a.example.com/two", "b1")
    _ok(store, "https://a.example.com/two", "b2")
    _ok(store, "https://b.example.com/one", "c1")
    _ok(store, "https://b.example.com/one", "c2")

    report = conflicts.audit(corpus_dir=store.root)
    assert report["replacedKeys"] == 3 and report["orphanedBodies"] == 3
    assert report["hostCount"] == 2
    assert [
        (item["host"], item["replacedKeys"], item["orphanedBodies"]) for item in report["hosts"]
    ] == [("a.example.com", 2, 2), ("b.example.com", 1, 1)]

    lines = conflicts.warning_lines(report)
    assert lines[0].startswith("[corpus] WARNING conflict: a.example.com")
    assert "2 request key(s) served more than one stored response" in lines[0]
    assert any("rather than refusing the certification" in line for line in lines)


def test_an_epoch_without_the_log_reports_orphans_but_not_hosts(tmp_path: Path) -> None:
    """An epoch recorded before this audit existed: the disk join still counts, hosts are honest."""

    store = _store(tmp_path)
    _ok(store, "https://boards.example.com/jobs", "render one")
    _ok(store, "https://boards.example.com/jobs", "render two")
    store.replacements_path.unlink()

    report = conflicts.audit(corpus_dir=store.root)
    assert report["replacedKeys"] == 0
    assert report["orphanedBodies"] == 1
    assert report["unattributedOrphans"] == 1
    assert report["hosts"] == [] and report["hostCount"] == 0
    assert report["replacementLog"] == "absent"

    note = conflicts.referee_note(report)
    assert note is not None
    assert note["orphanedBodies"] == 1
    assert note["hostCount"] == 0 and note["replacementLog"] == "absent"
    assert "predates the replacement log" in note["detail"]
    assert any("no replacement log" in line for line in conflicts.warning_lines(report))


def test_the_record_carries_a_replaced_response_in_its_certification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """One key, two renders, through the real record path: declared, and the record stands."""

    counter = {"n": 0}

    def changing() -> tuple[int, dict[str, str], bytes]:
        body = f"<html><body>render {counter['n']}</body></html>"
        counter["n"] += 1
        return (200, {"Content-Type": "text/html; charset=utf-8"}, body.encode())

    def loader_at(base: str) -> Any:
        def loader(**kwargs: Any) -> list[dict[str, Any]]:
            fetch_text = kwargs["fetch_text"]
            seen = [fetch_text(f"{base}/changing", 5) for _ in range(2)]
            assert seen[0] != seen[1]
            return [
                {
                    "title": "Senior Engineer",
                    "company": "Loopback Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": _JOB_URL,
                    "sector": "Game",
                    "sourceJobId": "loopback-1",
                    "postedAt": "2026-03-01",
                }
            ]

        return loader

    corpus_dir = tmp_path / "corpus"
    with local_http_server({"/changing": changing}) as base:
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader_at(base))])
        exit_code = run_fetch.main(
            _record_args(corpus_dir, tmp_path / "record", "--no-referee-verification")
        )

    captured = capsys.readouterr()
    manifest = json.loads((corpus_dir / "EPOCH.json").read_text(encoding="utf-8"))
    assert exit_code == run_fetch.EXIT_OK
    # Two keys, one body: the same page is recorded once for the seam call and once for the
    # transport call it makes (different header sets), so both stored responses were replaced,
    # while the first render -- shared by both keys -- is the single orphaned body.
    assert manifest["conflicts"]["replacedKeys"] == 2
    assert manifest["conflicts"]["orphanedBodies"] == 1
    assert manifest["conflicts"] == conflicts.summary(conflicts.load_report(corpus_dir))

    # The declaration travels with the verdict -- and the verdict is still a certification.
    assert manifest["deltaReferee"]["eligible"] is True
    (note,) = [
        item
        for item in manifest["deltaReferee"]["notes"]
        if item["reason"] == conflicts.NOTE_REASON
    ]
    assert note["hosts"] == ["127.0.0.1"]
    assert note["replacedKeys"] == 2 and note["orphanedBodies"] == 1

    report = conflicts.load_report(corpus_dir)
    (finding,) = report["hosts"]
    assert finding["host"] == "127.0.0.1"
    assert finding["replacedKeys"] == 2 and finding["orphanedBodies"] == 1
    # Two replacement events (one per key), one orphan: the counts are events and objects.
    assert finding["orphaningReplacements"] == 2
    assert finding["samples"] == [f"{base}/changing"]
    assert f"[corpus] conflicts {corpus_dir / conflicts.REPORT_NAME}" in captured.out
    assert "WARNING conflict: 127.0.0.1" in captured.err
