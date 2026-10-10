"""The corpus is only evidence if it round-trips exactly and never lies by omission.

Four rules carry the weight:

* a miss **raises** -- an empty body would make "the corpus could not answer"
  indistinguishable from "the board served nothing", which is the failure this slice
  exists to prevent;
* failures are stored as records, because 257 of 1,450 sources error and a
  successes-only corpus would replay a fictional green run;
* identical bodies are stored once, so a corpus of 5,600 requests does not pay for the
  same page twice;
* epoch authority is newest-by-``recordedAt``, and an unreadable manifest is skipped
  rather than sorted to the top, because a corrupt manifest winning an A/B is how the
  source-registry work already went wrong once.
"""

from __future__ import annotations

import gzip
import json
import shutil
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.common.http import HttpStatusError  # noqa: E402
from tools.measurements.corpus import epoch as epoch_mod  # noqa: E402
from tools.measurements.corpus.store import (  # noqa: E402
    CorpusMiss,
    CorpusStore,
    RequestLedger,
    canonical_url,
    error_record,
    request_key,
)


def test_request_key_keys_only_the_headers_that_change_a_response() -> None:
    """User-Agent is excluded on purpose; the response-changing headers are not."""

    base = request_key(url="https://Boards.Example.COM/Jobs", request_headers={})

    assert (
        request_key(
            url="https://Boards.Example.COM/Jobs",
            request_headers={"User-Agent": "Mozilla/5.0 (browser profile)"},
        )
        == base
    )
    assert (
        request_key(
            url="https://Boards.Example.COM/Jobs",
            request_headers={"Content-Length": "12", "X-Trace": "abc"},
        )
        == base
    )

    for header, value in (
        ("Accept", "application/json"),
        ("Cookie", "session=1"),
        ("Authorization", "Bearer t"),
        ("If-None-Match", 'W/"abc"'),
        ("If-Modified-Since", "Wed, 01 Oct 2026 00:00:00 GMT"),
    ):
        assert (
            request_key(url="https://boards.example.com/Jobs", request_headers={header: value})
            != base
        ), header

    # Header names are case-insensitive and methods are part of the identity.
    assert request_key(
        url="https://boards.example.com/Jobs", request_headers={"if-none-match": 'W/"abc"'}
    ) == request_key(
        url="https://boards.example.com/Jobs", request_headers={"If-None-Match": 'W/"abc"'}
    )
    assert request_key(url="https://boards.example.com/Jobs", method="HEAD") != request_key(
        url="https://boards.example.com/Jobs", method="GET"
    )


def test_canonical_url_normalises_only_what_cannot_change_the_response() -> None:
    assert canonical_url("HTTPS://Boards.Example.com:443/Jobs") == "https://boards.example.com/Jobs"
    assert canonical_url("http://boards.example.com:80/Jobs") == "http://boards.example.com/Jobs"
    assert canonical_url("https://boards.example.com/Jobs#section") == (
        "https://boards.example.com/Jobs"
    )
    # A non-default port, the path and the query order are all preserved: a board can
    # serve a different page for them, so folding them together would be a lie.
    assert canonical_url("https://boards.example.com:8443/Jobs?b=2&a=1") == (
        "https://boards.example.com:8443/Jobs?b=2&a=1"
    )
    assert canonical_url("https://boards.example.com/Jobs") != canonical_url(
        "https://boards.example.com/jobs"
    )
    # A bracketed IPv6 host keeps its colons intact and still loses a default port.
    assert canonical_url("https://[2001:db8::1]:443/Jobs") == "https://[2001:db8::1]/Jobs"
    assert canonical_url("https://[2001:db8::1]:8443/Jobs") == "https://[2001:db8::1]:8443/Jobs"
    assert canonical_url("") == ""


def test_round_trip_preserves_unicode_and_reports_local_sizes(tmp_path: Path) -> None:
    store = CorpusStore(tmp_path / "corpus")
    text = '{"label": "Éditeur — café", "n": 1}'

    store.record(url="https://boards.example.com/jobs", text=text, charset="utf-8")
    entry = store.lookup(url="https://boards.example.com/jobs")

    assert entry["outcome"] == "ok"
    assert store.body_text(entry) == text
    assert entry["decodedChars"] == len(text)
    # The seam yields text, so this is the UTF-8 length of that text, not wire bytes.
    assert entry["bodyBytes"] == len(text.encode("utf-8"))
    assert entry["bodyBytes"] > entry["decodedChars"]


def test_identical_bodies_are_stored_once_and_the_size_report_shows_the_saving(
    tmp_path: Path,
) -> None:
    store = CorpusStore(tmp_path / "corpus")
    page = "<html><body>" + ("job row " * 200) + "</body></html>"

    store.record(url="https://a.example.com/jobs", text=page)
    store.record(url="https://b.example.com/jobs", text=page)
    store.record(url="https://c.example.com/jobs", text=page + " different")

    counts = store.counts()
    assert counts["entries"] == 3
    assert counts["uniqueBodies"] == 2
    assert counts["dedupeSavedBytes"] == len(page.encode("utf-8"))
    # One of three entries' worth of bytes was saved by dedupe, so the ratio is a third.
    assert counts["dedupeRatio"] > 0.3
    assert counts["compressedBodyBytes"] < counts["uniqueBodyBytes"]

    size = store.size_report(scope="unit")
    assert size["scope"] == "unit"
    assert size["dedupeSavedBytes"] == counts["dedupeSavedBytes"]


def test_concurrent_recording_keeps_every_body_readable_and_every_count_exact(
    tmp_path: Path,
) -> None:
    """A recorded run fetches sources on a worker pool, so the store has to survive threads.

    Two failures are being pinned, both of which corrupt evidence rather than print a wrong
    number: two workers gzipping the same content-addressed body at once produce a file
    ``body_text`` cannot read, and a lost ``+=`` increment makes the coverage report's
    ``offered == recorded`` claim false. Four URLs share each body, so the dedupe path -- the
    one place two threads actually collide on a file -- is exercised, not just touched.
    """

    store = CorpusStore(tmp_path / "corpus")
    ledger = RequestLedger()
    bodies = {"alpha": "a" * 20000, "beta": "b" * 20000, "gamma": "c" * 20000}
    shared = sorted(bodies)
    jobs = [(worker, index) for worker in range(8) for index in range(15)]

    def record(job: tuple[int, int]) -> None:
        worker, index = job
        url = f"https://boards.example.com/{worker}/{index}"
        ledger.note_offer(url, surface="fetch_text")
        store.record(
            url=url,
            text=bodies[shared[index % len(shared)]],
            surface="fetch_text",
            ledger=ledger,
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(record, jobs))

    report = ledger.as_report()
    assert report["requestsOffered"] == len(jobs)
    assert report["requestsRecorded"] == len(jobs)
    assert report["unattributedCount"] == 0

    counts = store.counts()
    assert counts["entries"] == len(jobs)
    assert counts["uniqueBodies"] == len(bodies)
    # Every entry's body verifies against its own hash; a torn gzip write would raise here
    # instead of surfacing as an unreadable page in the middle of a replay A/B.
    restored: set[str] = set()
    for path in store.entries_dir.rglob("*.json"):
        entry = json.loads(path.read_text(encoding="utf-8"))
        restored.add(store.body_text(entry))
    assert restored == set(bodies.values())


def test_bodies_are_gzipped_on_disk_and_deterministic(tmp_path: Path) -> None:
    store = CorpusStore(tmp_path / "corpus")
    text = "x" * 4096

    entry = store.record(url="https://boards.example.com/jobs", text=text)
    path = store.body_path(entry["bodySha256"])

    assert path.read_bytes()[:2] == b"\x1f\x8b"
    assert gzip.decompress(path.read_bytes()).decode("utf-8") == text
    first = path.read_bytes()

    # Re-recording the same bytes leaves the file untouched rather than rewriting it with
    # a fresh gzip header, so two corpora recorded from the same input compare by hash.
    store.record(url="https://other.example.com/jobs", text=text)
    assert path.read_bytes() == first


def test_failure_entries_are_first_class_records(tmp_path: Path) -> None:
    store = CorpusStore(tmp_path / "corpus")

    store.record(
        url="https://boards.example.com/gone",
        outcome="failure",
        error=HttpStatusError(404, "https://boards.example.com/gone"),
        status_code=404,
    )
    store.record(
        url="https://boards.example.com/unreachable",
        outcome="failure",
        error=RuntimeError("Network error for https://boards.example.com: timed out"),
    )
    store.record(url="https://boards.example.com/empty", text="")

    gone = store.lookup(url="https://boards.example.com/gone")
    unreachable = store.lookup(url="https://boards.example.com/unreachable")
    empty = store.lookup(url="https://boards.example.com/empty")

    assert gone["outcome"] == "failure"
    assert gone["error"] == {
        "class": "HttpStatusError",
        "message": "HTTP 404 for https://boards.example.com/gone",
        "code": 404,
    }
    assert gone["statusCode"] == 404
    assert gone["bodySha256"] == ""
    assert unreachable["error"]["class"] == "RuntimeError"
    # A replayed failure must never look like a successful empty fetch: same seam, two
    # distinguishable records.
    assert unreachable["outcome"] == "failure"
    assert empty["outcome"] == "ok"
    assert store.body_text(empty) == ""
    with pytest.raises(ValueError):
        store.body_text(gone)
    assert store.counts()["outcomes"] == {"failure": 2, "ok": 1}


def test_a_contradictory_entry_is_refused(tmp_path: Path) -> None:
    store = CorpusStore(tmp_path / "corpus")

    with pytest.raises(ValueError):
        store.record(url="https://a.example.com/jobs", outcome="ok", error=RuntimeError("boom"))
    with pytest.raises(ValueError):
        store.record(url="https://a.example.com/jobs", outcome="failure")


def test_error_record_reads_a_status_code_from_either_exception_form() -> None:
    from_status_attribute = error_record(HttpStatusError(503, "https://a.example.com/x"))
    # The async transport reports status inside the message instead of as an attribute.
    from_message = error_record(RuntimeError("HTTP 429 for https://a.example.com/x"))
    from_plain_message = error_record(RuntimeError("connection reset"))

    assert from_status_attribute is not None and from_status_attribute["code"] == 503
    assert from_message is not None and from_message["code"] == 429
    assert from_plain_message is not None and from_plain_message["code"] == 0
    assert error_record(None) is None


def test_a_miss_is_loud_named_and_never_an_empty_body(tmp_path: Path) -> None:
    store = CorpusStore(tmp_path / "corpus")
    ledger = RequestLedger()

    with pytest.raises(CorpusMiss) as caught:
        store.lookup(url="https://boards.example.com/jobs", surface="fetch_text", ledger=ledger)

    assert caught.value.url == "https://boards.example.com/jobs"
    assert caught.value.key == request_key(url="https://boards.example.com/jobs")
    assert "fetch_text" in str(caught.value)
    # A miss is not a record: nothing was written, and the ledger says so by name.
    assert store.counts()["entries"] == 0
    assert ledger.misses == 1
    report = ledger.as_report()
    assert report["requestsMissed"] == 1
    assert report["misses"][0]["url"] == "https://boards.example.com/jobs"
    assert report["misses"][0]["reason"] == "no_entry"


def test_coverage_names_what_was_offered_but_never_attributed(tmp_path: Path) -> None:
    store = CorpusStore(tmp_path / "corpus")
    ledger = RequestLedger()

    ledger.note_offer("https://a.example.com/jobs", surface="fetch_text")
    store.record(url="https://a.example.com/jobs", text="ok", ledger=ledger)
    assert ledger.attribution_is_complete()

    # An offer with no record and no named miss is exactly the harness defect the
    # coverage report exists to expose.
    ledger.note_offer("https://b.example.com/jobs", surface="httpx")
    assert not ledger.attribution_is_complete()
    report = ledger.as_report()
    assert report["requestsOffered"] == 2
    assert report["requestsRecorded"] == 1
    assert report["unattributedCount"] == 1
    assert report["unattributedUrls"] == ["https://b.example.com/jobs"]
    assert report["offersBySurface"] == {"fetch_text": 1, "httpx": 1}

    store.lookup(url="https://a.example.com/jobs", ledger=ledger)
    assert ledger.hits == 1
    assert not ledger.attribution_is_complete()

    store.record(url="https://b.example.com/jobs", text="ok", ledger=ledger)
    assert ledger.attribution_is_complete()

    coverage_path, size_path = store.write_reports(ledger=ledger, scope="unit", mode="replay")
    payload = json.loads(coverage_path.read_text(encoding="utf-8"))
    assert payload["mode"] == "replay"
    assert payload["counts"]["entries"] == 2
    assert payload["requestsHit"] == 1
    assert payload["unattributedCount"] == 0
    assert json.loads(size_path.read_text(encoding="utf-8"))["entries"] == 2


def test_epoch_records_provenance_and_the_newest_one_wins(tmp_path: Path) -> None:
    parent = tmp_path / "perf-runs"
    start = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)

    older = epoch_mod.epoch_dir(parent, "subset", now=start)
    epoch_mod.create_epoch(
        older,
        scope="subset",
        counts={"entries": 10, "uniqueBodies": 4},
        coverage_summary={"requestsOffered": 12, "unattributedCount": 0},
        git_sha="0" * 40,
        dirty_diff_hash="dirty",
        now=start,
    )
    newer = epoch_mod.epoch_dir(parent, "subset", now=start + timedelta(minutes=30))
    epoch_mod.create_epoch(
        newer, scope="subset", counts={"entries": 20}, now=start + timedelta(minutes=30)
    )

    manifest = epoch_mod.read_epoch(newer)
    assert manifest["epochId"] == newer.name
    assert manifest["recordedWith"] == "force_refresh_all"
    assert manifest["supersededBy"] == ""
    assert manifest["counts"] == {"entries": 20}

    assert epoch_mod.resolve_current(parent) == newer
    changed = epoch_mod.supersede_older_epochs(parent, current=newer.name)
    assert changed == [older.name]
    assert epoch_mod.read_epoch(older)["supersededBy"] == newer.name
    # Supersession is a note, not a deletion: a corpus cannot be regenerated.
    assert epoch_mod.read_epoch(older)["counts"]["entries"] == 10
    assert "supersededBy=" in epoch_mod.describe_epoch(older)
    # Idempotent: re-running does not rewrite the same note.
    assert epoch_mod.supersede_older_epochs(parent, current=newer.name) == []


def test_epoch_resolution_skips_unreadable_manifests_rather_than_promoting_them(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "perf-runs"
    start = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
    good = epoch_mod.epoch_dir(parent, "subset", now=start)
    epoch_mod.create_epoch(good, now=start)

    corrupt = parent / "corpus-subset-20261008-130000"
    corrupt.mkdir(parents=True)
    (corrupt / epoch_mod.EPOCH_FILE).write_text("{not json", encoding="utf-8")
    manifest_less = parent / "corpus-subset-20261008-140000"
    manifest_less.mkdir(parents=True)

    assert epoch_mod.epoch_dirs(parent) == [good]
    assert epoch_mod.resolve_current(parent) == good
    with pytest.raises(epoch_mod.EpochError):
        epoch_mod.read_epoch(corrupt)
    assert epoch_mod.resolve_current(tmp_path / "missing") is None
    assert epoch_mod.epoch_dirs(tmp_path / "missing") == []


def test_git_identity_is_empty_outside_a_repository() -> None:
    # Deliberately not `tmp_path`: this repo's pytest config keeps its basetemp inside the
    # checkout, so a tmp_path is still inside the repository and git answers from it.
    outside = Path(tempfile.mkdtemp(prefix="baluffo-epoch-outside-"))
    try:
        assert epoch_mod.git_identity(outside) == ("", "")
    finally:
        shutil.rmtree(outside, ignore_errors=True)
