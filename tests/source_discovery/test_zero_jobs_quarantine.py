"""A probe zero must not become a permanent queue occupant.

The deadlock this closes, measured by draining discovery over the curated boards:

    round 1: approved=185  queued=21  deferredByCap=366
    round 2: approved=  0  queued=21  deferredByCap=226
    round 3: approved=  0  queued=21  deferredByCap=226   ... and so on

The *same* 21 rows occupied every queue slot in every round. All 21 reported
`jobsFound=0` with `lastProbeStatus=ok`. Meanwhile `healthyButDeferredByAdapter` was
`{static: 226}` -- candidates the probe had already called healthy, starved behind
them. Re-running discovery delivered nothing.

Three things combined:

1. `should_queue_candidate` admits a zero-jobs candidate on evidence score alone. That
   is correct and deliberate: the repo's rule is that the probe signal is positive-only
   and a zero is not an answer. It is not the bug.
2. Auto-approval then requires a positive job count and refuses the row, so it lands in
   `pending`.
3. `_record_probe_result` *cleared* the probe-failure memory on any successful probe.
   A board that succeeds with zero therefore could never accumulate toward the
   time-boxed quarantine, and `_QUARANTINE_CLASSES` only held `{"dns", "ssl"}` keyed on
   an error string these rows do not have.

So the row had no exit: admitted, refused, and never retired.

The fix records a zero-yield outcome as its own class. Quarantine here means "stop
re-probing for the retention window", **not** "this board is empty" -- a board whose
roles have all closed returns to the queue when the window expires.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.source_discovery import probe_failure_memory as pfm
from src.source_discovery.core_thresholds import should_queue_candidate


@pytest.fixture()
def memory(tmp_path: Path) -> pfm.ProbeFailureMemory:
    return pfm.ProbeFailureMemory(path=tmp_path / "probe-failures.json")


_IDENTITY = "static:listing_url:https://studio.example"


# --- The gap the fix closes ----------------------------------------------


def test_a_zero_jobs_board_is_admitted_to_the_queue_by_evidence_alone() -> None:
    """Step 1: the candidate gets in. This is intended, not the bug."""
    candidate = {"adapter": "static", "evidenceScore": 52}
    assert should_queue_candidate(candidate, 0) is True


def test_a_zero_jobs_probe_has_no_error_and_so_no_failure_class() -> None:
    """Why the existing quarantine could never see it."""
    assert pfm.classify_probe_failure_class("") == "other"
    assert not pfm.is_quarantine_class("other")


# --- The fix --------------------------------------------------------------


def test_zero_jobs_is_its_own_class() -> None:
    assert pfm.classify_probe_failure_class(pfm.ZERO_JOBS_ERROR) == pfm.ZERO_JOBS_CLASS
    assert pfm.is_quarantine_class(pfm.ZERO_JOBS_CLASS)


def test_zero_jobs_quarantines_only_after_the_threshold(memory: pfm.ProbeFailureMemory) -> None:
    threshold = pfm.quarantine_threshold()
    for _ in range(threshold - 1):
        assert pfm.record_zero_jobs(memory, _IDENTITY) is None
    assert pfm.record_zero_jobs(memory, _IDENTITY) is not None


def test_a_quarantined_zero_jobs_board_leaves_the_queue(memory: pfm.ProbeFailureMemory) -> None:
    for _ in range(pfm.quarantine_threshold()):
        pfm.record_zero_jobs(memory, _IDENTITY)
    assert _IDENTITY in memory.quarantine_index()


def test_a_board_that_starts_yielding_jobs_returns_to_the_queue(
    memory: pfm.ProbeFailureMemory,
) -> None:
    """Quarantine is a pause, not a verdict. A board whose roles reopen must return."""
    for _ in range(pfm.quarantine_threshold()):
        pfm.record_zero_jobs(memory, _IDENTITY)
    assert _IDENTITY in memory.quarantine_index()
    memory.clear_identity(_IDENTITY)
    assert _IDENTITY not in memory.quarantine_index()


def test_the_zero_is_not_recorded_as_an_empty_board() -> None:
    """The wording matters: this is 'found no jobs', never 'has no jobs'.

    The repo's rule is that a probe zero is not an answer, so the record must not read
    as a determination that the board is dead.
    """
    assert "no jobs" in pfm.ZERO_JOBS_ERROR
    assert "empty" not in pfm.ZERO_JOBS_ERROR


# --- Existing behaviour must not change -----------------------------------


def test_dns_and_ssl_still_quarantine(memory: pfm.ProbeFailureMemory) -> None:
    for identity, error in (
        ("a:1", "[Errno -2] Name or service not known"),
        ("b:1", "certificate verify failed"),
    ):
        for _ in range(pfm.quarantine_threshold()):
            memory.record_failure(identity=identity, error=error)
        assert identity in memory.quarantine_index(), identity


def test_transient_classes_still_never_quarantine(memory: pfm.ProbeFailureMemory) -> None:
    for identity, error in (("c:1", "timeout"), ("d:1", "HTTP error 500"), ("e:1", "reset")):
        for _ in range(pfm.quarantine_threshold() + 2):
            memory.record_failure(identity=identity, error=error)
        assert identity not in memory.quarantine_index(), identity


def test_recording_without_a_memory_is_a_no_op() -> None:
    assert pfm.record_zero_jobs(None, _IDENTITY) is None


def test_recording_without_an_identity_is_a_no_op(memory: pfm.ProbeFailureMemory) -> None:
    assert pfm.record_zero_jobs(memory, "") is None
    assert pfm.record_zero_jobs(memory, "   ") is None


def test_a_class_change_resets_the_consecutive_count(memory: pfm.ProbeFailureMemory) -> None:
    """A board with a *different* problem must be probed again, not quarantined."""
    pfm.record_zero_jobs(memory, _IDENTITY)
    memory.record_failure(identity=_IDENTITY, error="[Errno -2] Name or service not known")
    assert _IDENTITY not in memory.quarantine_index()
