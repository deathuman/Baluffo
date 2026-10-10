"""The truncation audit exists to catch, at record time, work a bounded phase dropped.

A corpus cannot show what a run decided *not* to request, so a replay that walks further than the
record did dies inside an unrelated-looking phase (measured: 47 of 465 candidates requested, 200
links missed — `docs/snapshots/fetch-pipeline-full-source-epoch-2026-10-09.md`). The audit joins
the two halves the record owns — the candidates its kept rows declare and the requests it made —
so these tests pin the join, not a threshold: a healthy record must be silent, a truncated one
must name the host and the counts, and non-candidates must never appear.

The second full-source epoch (2026-10-10) measured the opposite error: 252 rows from a source the
resolver is never handed (``gracklehq``, ``REDIRECT_RESOLUTION_SKIP_SOURCES``) read as unrequested
candidates while the phase it was blamed on had requested every one of its own -- so the join
also mirrors the product's skip gate.

The fixture URLs are derived from the product's own redirect allowlist rather than invented. That
is deliberate: the candidate rule is "an allowlisted host with a ``/rd/`` path", so a hand-written
URL shape would pass here while auditing nothing in production.
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

from src.jobs.common.config import SUPPORTED_REDIRECT_HOSTS  # noqa: E402
from tests.tools.test_corpus_capture import _surface_loader, local_http_server  # noqa: E402
from tools.measurements.corpus import run_fetch, truncation  # noqa: E402
from tools.measurements.corpus.store import CorpusStore  # noqa: E402

_ALLOWLISTED = sorted(SUPPORTED_REDIRECT_HOSTS)
PRIMARY_HOST = _ALLOWLISTED[0]
SECONDARY_HOST = _ALLOWLISTED[-1]
PLAIN_HOST = "plain-careers.example.com"

PRIMARY_LINK = f"https://{PRIMARY_HOST}/rd/{{index}}"
SECONDARY_LINK = f"https://{SECONDARY_HOST}/rd/{{index}}"
# A link the resolver would never be handed: right path, host not on the allowlist.
PLAIN_LINK = f"https://{PLAIN_HOST}/careers/senior-engineer"
# And the same allowlisted host with a path the rule rejects.
WRONG_PATH_LINK = f"https://{PRIMARY_HOST}/jobs"


def _rows(links: list[str]) -> list[dict[str, Any]]:
    return [{"jobLink": link, "title": f"Job {index}"} for index, link in enumerate(links)]


def _sourced_rows(links: list[str], source: str) -> list[dict[str, Any]]:
    return [
        {"jobLink": link, "source": source, "title": f"Job {index}"}
        for index, link in enumerate(links)
    ]


def test_the_allowlist_has_two_distinct_hosts_so_the_ordering_case_is_real() -> None:
    """The fixture below needs two hosts the resolver would treat as separate hosts."""

    assert len(_ALLOWLISTED) >= 2
    assert PRIMARY_HOST != SECONDARY_HOST


def test_a_healthy_record_audits_clean() -> None:
    """Every candidate requested: the report says clean, and the warning voice stays silent."""

    links = [PRIMARY_LINK.format(index=index) for index in range(4)]
    report = truncation.audit(rows=_rows(links), requested_urls=links, pool=16)

    assert report["status"] == "clean"
    assert report["candidates"] == 4
    assert report["unrequestedCandidates"] == 0
    assert report["hosts"] == []
    assert truncation.warning_lines(report) == []


def test_a_truncated_phase_is_named_with_counts_and_samples() -> None:
    """The finding is the host, how many candidates were known, and how many were requested."""

    links = [PRIMARY_LINK.format(index=index) for index in range(10)]
    requested = links[:3]
    report = truncation.audit(rows=_rows(links), requested_urls=requested, pool=16)

    assert report["status"] == "flagged"
    assert report["unrequestedCandidates"] == 7
    assert report["hostsWithUnrequestedCandidates"] == 1
    finding = report["hosts"][0]
    assert finding["host"] == PRIMARY_HOST
    assert (finding["candidates"], finding["requested"], finding["unrequested"]) == (10, 3, 7)
    # Samples are the evidence a reader checks by hand, and they are unrequested ones.
    assert finding["unrequestedSamples"] == links[3:8]
    assert set(truncation.candidate_sample_urls(report)) <= set(links) - set(requested)

    lines = truncation.warning_lines(report)
    assert PRIMARY_HOST in lines[0]
    assert "3 of 10" in lines[0]
    assert "7 unrequested" in lines[0]


@pytest.mark.parametrize("link", [PLAIN_LINK, WRONG_PATH_LINK])
def test_links_the_resolver_would_not_resolve_are_not_candidates(link: str) -> None:
    """A gap is only a gap for links the resolver was actually handed.

    Auditing every link a row carries would flag hosts that were never candidates, which is how a
    warning becomes noise nobody reads.
    """

    report = truncation.audit(rows=_rows([link]), requested_urls=[], pool=16)

    assert report["candidates"] == 0
    assert report["status"] == "clean"


def test_a_source_the_resolver_is_never_handed_declares_no_candidates() -> None:
    """Measured on the second full-source epoch: 252 ``gracklehq`` rows read as a gap that was not one.

    The product skips redirect resolution for ``gracklehq`` (``REDIRECT_RESOLUTION_SKIP_SOURCES``),
    so those links are never handed to the resolver and a replay makes no request to miss. The same
    links from a source whose path *does* resolve must still flag, or the exclusion would swallow
    the finding the check exists for.
    """

    links = [PRIMARY_LINK.format(index=index) for index in range(3)]
    skipped = truncation.audit(rows=_sourced_rows(links, "gracklehq"), requested_urls=[], pool=64)

    assert skipped["candidates"] == 0
    assert skipped["status"] == "clean"
    # The epoch this measures was refused certification on these rows; with the rule corrected it
    # is a referee again.
    assert truncation.referee_verdict(skipped)["eligible"] is True

    resolved = truncation.audit(
        rows=_sourced_rows(links, "google_sheets"), requested_urls=[], pool=64
    )
    assert resolved["candidates"] == 3
    assert resolved["unrequestedCandidates"] == 3
    assert resolved["hosts"][0]["host"] == PRIMARY_HOST


def test_the_skip_set_is_the_products_own(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pinned like the predicate: a copy of the set here could drift from the gate that does the skipping."""

    from src.jobs import canonicalize_locations

    assert "gracklehq" in truncation._skip_sources()

    monkeypatch.setattr(
        canonicalize_locations, "REDIRECT_RESOLUTION_SKIP_SOURCES", {"another-skipped-source"}
    )
    report = truncation.audit(
        rows=_sourced_rows([PRIMARY_LINK.format(index=0)], "another-skipped-source"),
        requested_urls=[],
        pool=64,
    )

    assert report["candidates"] == 0
    assert report["status"] == "clean"


def test_hosts_are_reported_worst_first_and_the_pool_is_context_only() -> None:
    """Ordering by size is what makes a long report readable, and the pool is not a gate.

    The abandonment point moves with how failures interleave with successes, so the report states
    the bound as context and lets the counts decide.
    """

    small = [SECONDARY_LINK.format(index=index) for index in range(2)]
    large = [PRIMARY_LINK.format(index=index) for index in range(9)]
    report = truncation.audit(rows=_rows([*small, *large]), requested_urls=[], pool=64)

    assert [item["host"] for item in report["hosts"]] == [PRIMARY_HOST, SECONDARY_HOST]
    assert report["pool"] == 64
    assert report["abandonmentThresholdAtMost"] == 128


def test_a_differently_spelled_request_counts_as_requested() -> None:
    """The join is on URL identity, not on spelling.

    A row link and the URL a request was sent to can differ by a fragment or case; comparing raw
    strings would report a fetched candidate as unrequested, which is the one error this check
    cannot afford.
    """

    link = PRIMARY_LINK.format(index=1)
    # Same identity, three different spellings: scheme case, an explicit default port, a fragment.
    for spelling in (
        link.replace("https://", "HTTPS://"),
        link.replace("//", "//").replace(PRIMARY_HOST, f"{PRIMARY_HOST}:443"),
        f"{link}#apply",
    ):
        report = truncation.audit(rows=_rows([link]), requested_urls=[spelling], pool=16)
        assert report["status"] == "clean", spelling


def test_the_audit_reads_the_corpus_through_the_store(tmp_path: Path) -> None:
    """`iter_requested_urls` is the join's other half: it must find what the corpus holds.

    Driven through the store, not through a dict built by the test, so a change in how entries are
    laid out (sharding, keying) shows up here rather than as a silent false flag on a real record.
    """

    store = CorpusStore(tmp_path / "corpus")
    sent = PRIMARY_LINK.format(index=1)
    store.record(
        url=sent,
        method="HEAD",
        request_headers={},
        status_code=200,
        response_headers={},
        text="",
        outcome="ok",
    )

    requested = set(store.iter_requested_urls())
    assert sent in requested

    report = truncation.audit(
        rows=_rows([sent, PRIMARY_LINK.format(index=2)]), requested_urls=requested, pool=16
    )
    assert report["candidates"] == 2
    assert report["unrequestedCandidates"] == 1
    assert report["hosts"][0]["unrequestedSamples"] == [PRIMARY_LINK.format(index=2)]


def test_the_record_driver_writes_the_audit_and_the_manifest_carries_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The audit is only useful if a record runs it *and acts on it*: drive ``main`` and read what it left.

    The fixture loader serves a loopback link, which the real redirect allowlist never treats as a
    candidate, so the predicate is substituted here to make one exist -- this test is about the
    wiring (report written, manifest summary, warning emitted, certification refused), not about the
    candidate rule, which ``test_the_predicate_is_the_products_own`` pins instead.

    The second run is the other half of that decision: the same flagged record, with
    ``--accept-truncated-referee``. A refusal nobody can override is a wall, and an override that
    leaves no trace in the artifact is not a certification anyone can read.
    """

    corpus_dir = tmp_path / "corpus"
    accepted_dir = tmp_path / "corpus-accepted"
    monkeypatch.setattr(
        truncation,
        "_predicate",
        staticmethod(lambda: lambda url: str(url).endswith("/jobs/senior-engineer")),
    )

    def record(corpus: Path, run_dir: Path, *extra: str) -> int:
        return run_fetch.main(
            [
                "--mode",
                "record",
                "--scope",
                "truncation-wiring",
                "--only-sources",
                "loopback",
                "--max-workers",
                "1",
                "--corpus-dir",
                str(corpus),
                "--run-dir",
                str(run_dir),
                *extra,
            ]
        )

    with local_http_server() as base:
        loader = _surface_loader(base)
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader)])
        refused = record(corpus_dir, tmp_path / "record")
        refused_stderr = capsys.readouterr().err
        accepted = record(accepted_dir, tmp_path / "record-accepted", "--accept-truncated-referee")
        accepted_stderr = capsys.readouterr().err

    assert refused == run_fetch.EXIT_REFEREE_REFUSED
    report = truncation.load_report(corpus_dir)
    assert report["scope"] == "truncation-wiring"
    assert report["unrequestedCandidates"] == report["candidates"] - report["requestedOfCandidates"]
    assert (report["status"] == "flagged") == bool(report["hosts"])
    # The substituted predicate names the row's own link, which this run never requested: the
    # fixture loader declares the board it stands in for, so the flag points at that host.
    assert report["status"] == "flagged"
    assert report["hosts"][0]["host"] == "boards.example.com"
    assert report["hosts"][0]["unrequestedSamples"] == [
        "https://boards.example.com/jobs/senior-engineer"
    ]

    manifest = json.loads((corpus_dir / "EPOCH.json").read_text(encoding="utf-8"))
    assert manifest["truncation"] == truncation.summary(report)
    # The epoch is written even though the certification was refused: it is a coverage record, and
    # the manifest says exactly which knob would make it a referee.
    assert manifest["deltaReferee"] == truncation.referee_verdict(report)
    assert manifest["deltaReferee"]["eligible"] is False
    assert manifest["deltaReferee"]["accepted"] is False
    assert manifest["deltaReferee"]["knobs"] == [truncation.KNOB_REDIRECT_POOL]
    assert truncation.KNOB_REDIRECT_POOL in refused_stderr
    assert "refusing the certification" in refused_stderr

    # The override records the same bound and says it was deliberate.
    accepted_manifest = json.loads((accepted_dir / "EPOCH.json").read_text(encoding="utf-8"))
    assert accepted == run_fetch.EXIT_OK
    assert accepted_manifest["deltaReferee"]["eligible"] is False
    assert accepted_manifest["deltaReferee"]["accepted"] is True
    assert "accepted anyway by --accept-truncated-referee" in accepted_stderr


def test_the_report_round_trips_through_disk(tmp_path: Path) -> None:
    link = PRIMARY_LINK.format(index=0)
    report = truncation.audit(rows=_rows([link]), requested_urls=[], pool=16)
    path = truncation.write_report(report, tmp_path / truncation.REPORT_NAME)

    assert truncation.load_report(tmp_path) == report
    assert truncation.summary(report)["report"] == truncation.REPORT_NAME
    assert truncation.summary(report)["unrequestedCandidates"] == 1
    assert path.name == truncation.REPORT_NAME


def test_the_predicate_is_the_products_own() -> None:
    """Pinned so a future edit cannot quietly re-implement the candidate rule and shift the bar."""

    predicate = truncation._predicate()
    assert predicate.__module__.startswith(truncation._PREDICATE_MODULE)
    assert predicate.__name__ == truncation._PREDICATE_NAME
    assert predicate(PRIMARY_LINK.format(index=1))
    assert not predicate(PLAIN_LINK)
    assert not predicate(WRONG_PATH_LINK)


@pytest.mark.parametrize(
    ("url", "host"),
    [
        ("https://Example.COM/rd/1", "example.com"),
        ("http://example.com:8443/rd/1?x=1", "example.com"),
        ("https://example.com/rd/1#frag", "example.com"),
        ("https://user:pw@example.com/rd/1", "example.com"),
        ("pathless", ""),
        ("", ""),
    ],
)
def test_host_of_normalises_the_way_a_host_key_needs_to_be(url: str, host: str) -> None:
    assert truncation.host_of(url) == host
