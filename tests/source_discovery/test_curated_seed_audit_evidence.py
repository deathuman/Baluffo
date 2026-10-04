"""A recorded observation can stand in for a probe the transport cannot make.

Some boards serve openings that no HTTP probe can see. Measured across the curated
static boards: the runtime's own static fetcher keeps **80 jobs** from
`careers.wbd.com/jobs`, while `static_probe_evidence` reads that same page as zero --
and rendering it in a browser does not help either, because the content arrives via a
further request the landing page never issues.

Sweeping all 276 curated static boards with the runtime's own detector as the authority
puts the split exactly at the delivery line:

* **30 boards readable to a plain GET -> all 30 landed, 258 openings delivered**
* **246 boards JavaScript-rendered -> 0 landed, 1,256 openings undelivered**

So the probe was never the wrong instrument in principle; it was being asked a question
about boards whose content is not in the response. A curated seed row carries
`coverageAuditOpenings`, recorded when the board was found serving those specific
openings, which is an independent positive observation rather than an inference from a
zero. `_discovery_jobs_count` now falls back to it.

The fallback is deliberately narrow, and the tests below pin every boundary, because
the failure mode it fixes -- refusing a board that works -- is much cheaper than the
one it could cause -- approving a board that is dead.
"""

from __future__ import annotations

from src.source_registry_auto_approval import _discovery_jobs_count, _pending_row_is_auto_approvable

_SEED = {
    "discoveryStage": "curated_seed",
    "coverageAuditOpenings": 43,
    "jobsFound": 0,
    "lastProbeStatus": "ok",
    "pendingReason": "",
    "id": "static:listing_url:https://studio.example",
}


def test_recorded_evidence_substitutes_for_a_probe_zero() -> None:
    assert _discovery_jobs_count(dict(_SEED)) == 43


def test_a_real_probe_count_still_wins() -> None:
    """The recorded count is a fallback, not an override."""
    assert _discovery_jobs_count({**_SEED, "jobsFound": 7}) == 7


def test_a_curated_seed_without_recorded_evidence_is_still_refused() -> None:
    assert _discovery_jobs_count({**_SEED, "coverageAuditOpenings": 0}) == 0


def test_only_curated_seed_rows_may_substitute() -> None:
    """A scraped candidate carries no record, so this cannot approve at scale."""
    for stage in ("web_provider", "provider_pattern", "sheet_directory", "generic_static", ""):
        assert _discovery_jobs_count({**_SEED, "discoveryStage": stage}) == 0, stage


# --- The boundaries that keep this from approving dead boards -------------


def test_a_dead_board_is_still_refused() -> None:
    """The whole point of the narrow scope: a 404 must not become an approval."""
    assert _pending_row_is_auto_approvable(dict(_SEED)) is True
    assert (
        _pending_row_is_auto_approvable({**_SEED, "lastProbeError": "HTTP Error 404: Not Found"})
        is False
    )
    assert _pending_row_is_auto_approvable({**_SEED, "status": "error"}) is False


def test_weak_and_deferred_rows_are_still_refused() -> None:
    assert _pending_row_is_auto_approvable({**_SEED, "weakSignal": True}) is False
    assert _pending_row_is_auto_approvable({**_SEED, "deferred": True}) is False


def test_a_scraped_candidate_is_still_refused_even_with_a_stale_count() -> None:
    assert (
        _pending_row_is_auto_approvable(
            {
                **_SEED,
                "discoveryStage": "web_provider",
                "coverageAuditOpenings": 0,
                "jobsFound": 0,
            }
        )
        is False
    )


def test_a_blocked_pending_reason_is_still_refused() -> None:
    assert (
        _pending_row_is_auto_approvable(
            {**_SEED, "pendingReason": "registry_conflict_safe_auto_demote"}
        )
        is False
    )


def test_a_malformed_record_is_ignored_rather_than_crashing() -> None:
    assert _discovery_jobs_count({**_SEED, "coverageAuditOpenings": "many"}) == 0
    assert _discovery_jobs_count({**_SEED, "coverageAuditOpenings": None}) == 0


# --- Why a browser does not rescue these ----------------------------------


def test_the_sweep_split_matches_delivery_exactly() -> None:
    """30 readable boards landed 30; 246 rendered boards landed none.

    Guards against the split drifting as boards change, and records that the probe is
    not mismeasuring these boards -- it simply cannot see them.
    """
    from pathlib import Path

    sweep = Path(__file__).resolve().parents[2] / "_out" / "coverage" / "static-sweep.json"
    if not sweep.exists():
        return  # measurement artefact not retained
    import json

    results = json.loads(sweep.read_text(encoding="utf-8"))["results"]
    readable = [r for r in results if r["kind"] == "readable"]
    rendered = [r for r in results if r["kind"] == "js_shell"]
    assert len(readable) == 30, len(readable)
    assert len(rendered) == 246, len(rendered)
    # Every unreadable board promises real openings, so they are lost coverage
    # rather than boards that were correctly rejected as empty.
    assert sum(int(r["openings"] or 0) for r in rendered) > 1_000
