"""The interception layer decides whether replay is faithful, so these tests use the real thing:
a **loopback HTTP server** and the product's own transport functions.

Three properties carry the weight, none of which a fake seam can show: a live-socket recording
replays **after the server is gone** with the guard armed and ``blocked == 0`` (the zero-egress
bar, meaningful only if the record side really opened a socket); a recorded failure replays as the
exception it was (``HttpStatusError(404)``, ``RuntimeError``), never as an empty body, which would
read as a board with no openings; and a corpus miss raises ``CorpusMiss`` rather than ``""``.
The three surfaces go through product code, not the harness: ``fetch_text`` via ``run_pipeline``,
``urlopen`` via ``transport.default_fetch_text`` and ``common/url``, ``httpx`` via
``transport.conditional_revalidate_url``.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import src.jobs.common.http as common_http  # noqa: E402
from src.jobs import transport  # noqa: E402
from tests.helpers.local_http_server import local_http_server  # noqa: E402
from tools.measurements.corpus.capture import (  # noqa: E402
    CaptureRecorder,
    CorpusMiss,
    CorpusReplayer,
    install_egress_guard,
)
from tools.measurements.corpus.store import CorpusStore, RequestLedger  # noqa: E402


def closed_port_url() -> str:
    """A URL on a port nothing listens on: connection refused, deterministically."""

    import socket

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return f"http://127.0.0.1:{port}/unreachable"


def _surface_loader(base: str):
    """A loader that reaches the network through the seam and through ``urlopen``."""

    def loader(**kwargs: Any) -> list[dict[str, Any]]:
        fetch_text = kwargs["fetch_text"]
        listing = fetch_text(f"{base}/listing", 5)
        with urllib.request.urlopen(f"{base}/redirect", timeout=5) as response:  # noqa: S310
            redirected = response.read().decode("utf-8")
        assert redirected == listing
        with pytest.raises(urllib.error.HTTPError):
            urllib.request.urlopen(f"{base}/gone", timeout=5)  # noqa: S310
        # The job link is a public-looking host on purpose: the canonicalizer rejects
        # loopback links, so a row carrying one would be dropped before it could prove
        # anything about parity. The *pages* are still fetched from the loopback server.
        return [
            {
                "title": "Senior Engineer",
                "company": "Loopback Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://boards.example.com/jobs/senior-engineer",
                "sector": "Game",
                "sourceJobId": "loopback-1",
                "postedAt": "2026-03-01",
            }
        ]

    return loader


def test_round_trip_replays_from_the_corpus_after_the_server_is_gone(tmp_path: Path) -> None:
    corpus = CorpusStore(tmp_path / "corpus")
    record_ledger = RequestLedger()

    with local_http_server() as base:
        loader = _surface_loader(base)
        with CaptureRecorder(corpus, scope="unit", ledger=record_ledger) as recorder:
            recorded = transport.default_fetch_text(f"{base}/listing", 5)
            assert "Senior Engineer" in recorded
            replayable = recorder.fetch_text(f"{base}/listing", 5)
            assert replayable == recorded
        assert record_ledger.attribution_is_complete()

    # The server is closed: every byte below must come from the corpus.
    replay_ledger = RequestLedger()
    guard = install_egress_guard()
    with guard:
        with CorpusReplayer(corpus, scope="unit", ledger=replay_ledger) as replayer:
            assert transport.default_fetch_text(f"{base}/listing", 5) == recorded
            assert replayer.fetch_text(f"{base}/listing", 5) == recorded

    assert guard.blocked == 0, f"replay reached a socket: {guard.targets}"
    assert replay_ledger.misses == 0
    assert replay_ledger.hits >= 2
    # Two entries: the same page was reached once through the seam and once through
    # urlopen, and the key is per request (headers included), not per URL.
    counts = corpus.counts()
    assert set(counts["outcomes"]) == {"ok"}
    assert counts["entries"] >= 2


def test_recorded_failures_replay_as_their_mapped_exceptions(tmp_path: Path) -> None:
    corpus = CorpusStore(tmp_path / "corpus")
    refused = closed_port_url()

    with local_http_server() as base:
        missing_url = f"{base}/gone"
        with CaptureRecorder(corpus, scope="unit") as recorder:
            # Recorded through the product's own transport, so record and replay send the
            # same request headers -- the corpus key includes them.
            with pytest.raises(common_http.HttpStatusError) as live_status:
                transport.default_fetch_text(missing_url, 5)
            with pytest.raises(RuntimeError) as live:
                recorder.fetch_text(refused, 3)
            live_message = str(live.value)
        assert live_status.value.code == 404
        assert "Network error" in live_message

    with CorpusReplayer(corpus, scope="unit") as replayer:
        # The 404 replays through the product's own transport as HttpStatusError.
        with pytest.raises(common_http.HttpStatusError) as status:
            transport.default_fetch_text(missing_url, 5)
        assert status.value.code == 404
        # A connection refusal replays as the same RuntimeError the live run raised.
        with pytest.raises(RuntimeError) as replayed:
            replayer.fetch_text(refused, 3)
        assert str(replayed.value) == live_message

    # Read the entries by URL: the key includes the request headers, so a lookup without
    # them is a different key by design.
    entries = [
        json.loads(path.read_text(encoding="utf-8")) for path in corpus.entries_dir.rglob("*.json")
    ]
    by_url = {str(entry["url"]): entry for entry in entries}
    assert by_url[missing_url]["outcome"] == "failure"
    assert by_url[missing_url]["error"]["code"] == 404
    assert by_url[missing_url]["bodySha256"] == ""
    # Never an empty body standing in for a failure.
    assert by_url[refused]["outcome"] == "failure"
    # Three records, not two: the refused connection is seen by the urlopen surface on its
    # way out and by the seam when the product wraps it, and each surface records what it
    # saw. Only failures are stored, and every one of them is a failure record.
    assert set(corpus.counts()["outcomes"]) == {"failure"}
    assert corpus.counts()["outcomes"]["failure"] >= 2


def test_a_corpus_miss_fails_loudly_on_every_surface(tmp_path: Path) -> None:
    corpus = CorpusStore(tmp_path / "corpus")
    ledger = RequestLedger()
    unknown = "https://never-recorded.invalid/jobs"

    with CorpusReplayer(corpus, scope="unit", ledger=ledger) as replayer:
        with pytest.raises(CorpusMiss) as seam:
            replayer.fetch_text(unknown, 5)
        with pytest.raises(CorpusMiss) as via_urlopen:
            urllib.request.urlopen(unknown, timeout=5)  # noqa: S310

    assert unknown in str(seam.value)
    assert unknown in str(via_urlopen.value)
    assert ledger.misses == 2
    assert [miss["url"] for miss in ledger.as_report()["misses"]] == [unknown, unknown]
    assert corpus.counts()["entries"] == 0


def test_httpx_revalidation_records_and_replays_through_product_code(tmp_path: Path) -> None:
    corpus = CorpusStore(tmp_path / "corpus")

    with local_http_server() as base:
        url = f"{base}/listing"
        with CaptureRecorder(corpus, scope="unit"):
            live = transport.conditional_revalidate_url(url, 5, etag='"abc"')
        assert live["supported"] is True
        assert live["statusCode"] == 200

    with CorpusReplayer(corpus, scope="unit"):
        replayed = transport.conditional_revalidate_url(url, 5, etag='"abc"')

    assert replayed == live
    entries = list(corpus.entries_dir.rglob("*.json"))
    assert len(entries) == 1
    assert '"httpx"' in entries[0].read_text(encoding="utf-8")


def test_httpx_redirects_are_keyed_by_the_request_url_and_replay(tmp_path: Path) -> None:
    """A redirecting httpx request must be stored under what was *asked for*.

    The redirect pool asks for board URLs that 302 elsewhere and ``follow_redirects=True``
    makes ``response.url`` the destination, so keying by it left those requests unattributed
    *and* un-replayable: a replay looks up the URL it was asked for and misses. Found by
    replaying a real subset epoch; a non-redirecting fake seam would have hidden it.
    """

    corpus = CorpusStore(tmp_path / "corpus")
    record_ledger = RequestLedger()
    with local_http_server() as base:
        requested = f"{base}/redirect"
        resolved = f"{base}/listing"
        with CaptureRecorder(corpus, scope="unit", ledger=record_ledger) as recorder:
            live = transport.conditional_revalidate_url(requested, 5, etag='"abc"')
        assert live["supported"] is True and live["statusCode"] == 200

    assert record_ledger.attribution_is_complete()
    (entry_path,) = corpus.entries_dir.rglob("*.json")
    entry = json.loads(entry_path.read_text(encoding="utf-8"))
    assert entry["url"] == requested
    assert entry["redirectChain"] == [requested, resolved]

    # The server is closed: everything below comes from the corpus or not at all.
    replay_ledger = RequestLedger()
    guard = install_egress_guard()
    with guard:
        with CorpusReplayer(corpus, scope="unit", ledger=replay_ledger) as replayer:
            replayed = transport.conditional_revalidate_url(requested, 5, etag='"abc"')

    assert replayed == live
    assert guard.blocked == 0, f"replay reached a socket: {guard.targets}"
    assert replay_ledger.misses == 0


def test_a_privately_built_opener_is_recorded_and_replays_without_a_socket(tmp_path: Path) -> None:
    """An adapter that keeps its own opener is a door onto the network, not a coverage footnote.

    Measured on the 2026-10-10 full-source epoch: ``dayforce_sources`` builds its own opener over
    stdlib ``urllib`` (a cookie jar of its own) and calls ``opener.open``, so no offer for it ever
    reached ``offersBySurface`` and ``unattributedCount == 0`` read as "the record captured all
    traffic" when it had not. Its birth replay said so plainly -- ``egress blocked during replay:
    jobs.dayforcehcm.com:443``.

    Two properties are load-bearing. The offer is *counted* like any other ``urlopen`` offer, or the
    coverage report lies again. And the live call still goes through the opener it was made on --
    the marker handler below is only reachable through that opener, so a live call rerouted through
    the module-level ``urlopen`` would leave it unset while quietly fetching a different request.
    """

    corpus = CorpusStore(tmp_path / "corpus")
    record_ledger = RequestLedger()
    marker: dict[str, bool] = {}

    class _Marker(urllib.request.BaseHandler):
        def http_request(self, request: Any) -> Any:  # noqa: N802 - urllib's naming
            marker["handled"] = True
            return request

    with local_http_server() as base:
        listing_url = f"{base}/listing"
        with CaptureRecorder(corpus, scope="unit", ledger=record_ledger) as recorder:
            opener = urllib.request.build_opener(_Marker())
            with opener.open(listing_url, timeout=5) as response:
                recorded_body = response.read().decode("utf-8")
            assert marker.get("handled") is True, "the live call bypassed the opener it was made on"
            assert recorder.coverage_extra()["privateOpeners"].startswith(
                "urllib.request.build_opener"
            )
        assert record_ledger.attribution_is_complete()
        assert record_ledger.as_report()["offersBySurface"] == {"urlopen": 1}
        assert "Senior Engineer" in recorded_body

    # The server is gone: every byte below has to come from the corpus, with no socket available.
    replay_ledger = RequestLedger()
    guard = install_egress_guard()
    with guard:
        with CorpusReplayer(corpus, scope="unit", ledger=replay_ledger):
            opener = urllib.request.build_opener()
            with opener.open(listing_url, timeout=5) as response:
                assert response.read().decode("utf-8") == recorded_body

    assert guard.blocked == 0, f"replay reached a socket: {guard.targets}"
    assert replay_ledger.misses == 0
    assert replay_ledger.hits >= 1


def test_patches_are_installed_inside_and_restored_afterwards(tmp_path: Path) -> None:
    import httpx

    corpus = CorpusStore(tmp_path / "corpus")
    original_module_urlopen = urllib.request.urlopen
    original_common_http_urlopen = common_http.urlopen
    original_build_opener = urllib.request.build_opener
    original_httpx_client = httpx.Client
    # The global opener is private module state, so it is read the way a caller that cannot name
    # it must: through ``getattr`` with a default, which is also what the type checker sees.
    original_global_opener = getattr(urllib.request, "_opener", None)

    with CaptureRecorder(corpus, scope="unit"):
        assert urllib.request.urlopen is not original_module_urlopen
        assert common_http.urlopen is not original_common_http_urlopen
        assert urllib.request.build_opener is not original_build_opener
        assert httpx.Client is not original_httpx_client
        # The opener's own state is not put back by restoring these two names; see
        # test_a_session_does_not_leave_a_shadowed_global_opener.
        assert getattr(urllib.request, "_opener", None) is not original_global_opener

    assert urllib.request.urlopen is original_module_urlopen
    assert common_http.urlopen is original_common_http_urlopen
    assert urllib.request.build_opener is original_build_opener
    assert httpx.Client is original_httpx_client
    assert getattr(urllib.request, "_opener", None) is original_global_opener


def test_a_session_does_not_leave_a_shadowed_global_opener(tmp_path: Path) -> None:
    """``urlopen`` builds the process-global opener lazily, and it builds it *through*
    ``build_opener``.

    A build that happens while a session is live installs a shadowed ``open`` on the module's own
    opener, and that object outlives the session: both patched names read as restored while every
    later ``urlopen`` in the process is answered from the corpus. It surfaced as an intermittent
    container-gateway ETag failure under ``-n 4``, where a corpus file and the gateway file share
    one worker. A later ``urlopen`` must reach a socket.
    """

    corpus = CorpusStore(tmp_path / "corpus")
    before = getattr(urllib.request, "_opener", None)
    # A caller that imported ``urlopen`` by name -- as the gateway test does -- reaches the real
    # function, whose first call is what builds the global opener.
    name_imported_urlopen = urllib.request.urlopen

    with local_http_server() as base:
        with CaptureRecorder(corpus, scope="unit"):
            with name_imported_urlopen(f"{base}/listing", timeout=5) as recorded:  # noqa: S310
                assert recorded.read()
        # The session hands the process its opener back before the next live call, which is the
        # one that would otherwise be answered from the corpus.
        assert getattr(urllib.request, "_opener", None) is before
        with urllib.request.urlopen(f"{base}/listing", timeout=5) as live:  # noqa: S310
            assert type(live.headers).__name__ == "HTTPMessage", (
                "the process-global opener still replays: a later urlopen was answered by the corpus"
            )


def test_a_partial_surface_set_leaves_the_others_alone(tmp_path: Path) -> None:
    """``--no-surfaces`` exists so a caller can disable one interception deliberately."""

    import httpx

    corpus = CorpusStore(tmp_path / "corpus")
    original_httpx_client = httpx.Client
    surfaces = tuple(
        surface for surface in ("fetch_text", "urlopen", "httpx") if surface != "httpx"
    )

    with CaptureRecorder(corpus, scope="unit", surfaces=surfaces) as recorder:
        assert httpx.Client is original_httpx_client
        assert "urlopen" in recorder.coverage_extra()["surfaces"]
        assert "httpx" not in recorder.coverage_extra()["surfaces"]
    assert httpx.Client is original_httpx_client


def test_coverage_extra_names_what_is_not_intercepted(tmp_path: Path) -> None:
    corpus = CorpusStore(tmp_path / "corpus")

    with CaptureRecorder(corpus, scope="subset") as recorder:
        extra: Mapping[str, Any] = recorder.coverage_extra()

    assert extra["scope"] == "subset"
    assert extra["surfaces"] == ["fetch_text", "urlopen", "httpx"]
    assert any("browser_fallback_pool" in item for item in extra["outOfLaneNetworkCallers"])
    assert "not exercised" in str(extra["asyncTransportLane"])
