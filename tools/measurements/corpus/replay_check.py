"""Verify the referee listing's claim by replaying every epoch it calls eligible.

``referees.py`` answers "which epoch may referee a delta?" out of what the record *said* -- its
``deltaReferee`` block, its stored truncation audit, or that audit re-run over its artifacts. All
three are audits of the record: they reason about whether a bounded phase dropped work a replay
would walk into, and none of them runs the replay. This command runs it -- one fresh process per
eligible epoch -- so the certification is measured rather than inferred.

Two properties, because a study needs both
------------------------------------------

**End to end.** The replay completed (``parity-ok`` or ``parity-failed``) instead of aborting, and
the corpus answered every request it was offered: ``requestsMissed == 0`` and
``unattributedCount == 0``. The miss count is read out of the run's own ledger rather than inferred
from the exit code, because ``RequestLedger.note_miss`` fires before the ``CorpusMiss`` is raised:
a source that swallowed the exception still shows up here.

**Reproduced.** Today's code, replaying from the corpus, produced the record's own row count,
identity digest, projection digest and per-source status counts -- the driver's own comparison,
``run_fetch.parity_matches``. ``ab.py`` refuses a study whose arms diverge from the epoch, so an
epoch that cannot reproduce its record is not one a delta can rest on; this check says so here
instead of letting a study discover it after paying for pairs.

What each verdict means
-----------------------

``verified``          both properties hold.
``corpus-miss``       the corpus could not answer a request the replay made. Names the URLs and the
                      surface. This is the finding the check exists for: the audit called the epoch
                      replayable, and the replay says otherwise.
``unanswered``        an offer the corpus never resolved -- a body its entry names but does not
                      hold, a malformed entry, a thread that died mid-offer. The ledger's blind
                      spot, and still a corpus that cannot answer.
``pipeline-error``    the run finished but its own report carries an error.
``egress-blocked``    the replay tried the network and the guard aborted the run.
``harness-error``     the replay could not be judged at all: no process exit, no receipts, a timeout.
``parity-diverged``   everything above held and the rows still drifted from the record.
``parity-bound``      the rows drifted, and every differing row belongs to a host the record itself
                      declared as a key conflict (``conflicts.py``) -- the one class a replay
                      cannot reproduce, bound rather than refused (``binding.py``).
``not-eligible``      the listing does not call the epoch eligible; it is never replayed.

**Two verdicts are passes, and they are not the same statement.** ``verified`` means the replay
reproduced the record. ``parity-bound`` means it did not, and that the record's own declaration
explains every differing row: the residue is read from the record's rows and the replay's, every
differing row is attributed to the host of the source that carried it, and the bind is refused the
moment one differing source falls outside the declared set -- as it is for a differing row that
names no source host, for an epoch whose record rows are no longer on disk, and for an epoch that
declares nothing at all. ``corpus-miss``, ``unanswered``, ``pipeline-error`` and
``parity-diverged`` falsify the certification (exit 1); an epoch that could not be judged is a
finding about the harness rather than about the corpus (exit 2), and is never reported as either.
An empty eligible set is the same refusal a record makes about itself (exit 3): a check that
verified nothing must not read as a green light.

Configuration is the record's
-----------------------------

A replay is only evidence if it asks for the same work the record did, so each epoch is replayed
under its own ``recordedConfig``: the source selection, worker count, redirect pool, intercepted
surfaces and the static-clock value that was in force. Two driver defaults are non-empty --
``--sources 8`` and ``--families google_sheets`` -- so a recorded zero or empty list is passed
explicitly; omitting the flag would silently substitute the default and sample boards the record
never fetched. The static clock travels as the environment variable its own knob names
(``truncation.KNOB_STATIC_SOURCE_CLOCK``), so the check cannot replay under a clock different from
the one it reports. An epoch whose record predates ``recordedConfig`` is replayed on the driver
defaults and its row says ``configSource: defaults``: a weaker statement, labelled rather than
blurred.

Usage::

    python tools/measurements/corpus/replay_check.py
    python tools/measurements/corpus/replay_check.py --samples 2 --timeout-s 1800
    python tools/measurements/corpus/replay_check.py _out/perf-runs/corpus-budgetprobe-20261009
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.corpus import binding, harness, referees, truncation  # noqa: E402
from tools.measurements.corpus.capture import ALL_SURFACES  # noqa: E402
from tools.measurements.corpus.run_fetch import (  # noqa: E402
    guard_blocked_source_names,
    new_run_token,
    parity_matches,
    write_json,
)

DEFAULT_CORPUS_PARENT = _ROOT / "_out" / "perf-runs"
DEFAULT_SAMPLES = 1
DEFAULT_TIMEOUT_S = 3600

VERDICT_VERIFIED = "verified"
# The driver's own status strings for the two outcomes that are not a completed, corpus-answered
# run, reused as this tool's verdict names so the two surfaces cannot drift apart.
VERDICT_MISSED = "corpus-miss"
VERDICT_EGRESS = "egress-blocked"
VERDICT_UNANSWERED = "unanswered"
VERDICT_PIPELINE_ERROR = "pipeline-error"
VERDICT_HARNESS = "harness-error"
VERDICT_DIVERGED = "parity-diverged"
#: The replay did not reproduce the record, and every differing row belongs to a conflict class the
#: record declared per host. A pass, and labelled as one rather than blurred into ``verified``: the
#: two statements answer different questions, and a study reading either should see which it got.
VERDICT_BOUND = "parity-bound"
VERDICT_NOT_ELIGIBLE = "not-eligible"
#: The verdicts a caller may treat as a pass. One set, so a study gate, a birth decision and a
#: report cannot disagree about which verdicts open the gate.
ACCEPTED_VERDICTS: frozenset[str] = frozenset({VERDICT_VERIFIED, VERDICT_BOUND})

# Severity order: the first problem a sample carries decides its verdict, so a corpus hole is never
# reported as a fidelity drift. Falsifying kinds first, then "could not be judged", then the
# fidelity claim itself.
PROBLEM_KINDS: tuple[str, ...] = (
    VERDICT_MISSED,
    VERDICT_UNANSWERED,
    VERDICT_PIPELINE_ERROR,
    VERDICT_EGRESS,
    VERDICT_HARNESS,
    VERDICT_DIVERGED,
)
#: A verdict here says the listing's claim did not hold -- the check's finding.
FALSIFYING: frozenset[str] = frozenset(
    {VERDICT_MISSED, VERDICT_UNANSWERED, VERDICT_PIPELINE_ERROR, VERDICT_DIVERGED}
)
#: A verdict here says the epoch could not be judged; it is not evidence either way.
UNJUDGEABLE: frozenset[str] = frozenset({VERDICT_EGRESS, VERDICT_HARNESS})

CONFIG_RECORDED = "recorded"
CONFIG_DEFAULTS = "defaults"
# Where a check's evidence lives, shared by every caller that runs one: a study gate under its
# study directory, a record's birth check under the epoch it just wrote. One spelling, so a reader
# can find a verification wherever it was taken.
VERIFICATION_DIRNAME = "referee-verification"
RECEIPT_NAME = "referee-verification.json"
# No process exit: the driver could not be launched at all (a timeout or a missing interpreter).
_NO_PROCESS_EXIT = -1
# How many miss URLs the markdown names per epoch before counting the rest. The JSON copy keeps
# every name the ledger kept.
_NAMED_MISSES = 5

EXIT_OK = harness.EXIT_OK
EXIT_FALSIFIED = harness.EXIT_PARITY_FAILED
EXIT_INFRASTRUCTURE = harness.EXIT_INFRASTRUCTURE
# Nothing eligible to verify is the same refusal a record makes about itself: "no referee" is one
# fact, and a second number for it would let a script tell the two apart.
EXIT_NOTHING_TO_CHECK = truncation.EXIT_REFEREE_REFUSED

NOT_PRESENT = "-"


def accepted(verdict: Any) -> bool:
    """Whether a verdict opens the gate: the record was reproduced, or its own class explains why
    it could not be."""

    return str(verdict or "") in ACCEPTED_VERDICTS


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def recorded_config(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """The config a replay has to reproduce, or ``{}`` when the record predates it."""

    raw = manifest.get("recordedConfig")
    return dict(raw) if isinstance(raw, Mapping) and raw else {}


def _surface_counts(misses: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in misses:
        surface = str(item.get("surface") or "unknown")
        counts[surface] = counts.get(surface, 0) + 1
    return dict(sorted(counts.items()))


def build_epoch_command(
    *,
    epoch_dir: Path | str,
    run_dir: Path | str,
    scope: str,
    config: Mapping[str, Any],
    label: str,
) -> tuple[list[str], dict[str, str]]:
    """The replay invocation for one epoch: its record's own configuration where it has one.

    Returns the command and the environment additions. ``--sources`` and ``--families`` are passed
    explicitly when the record holds a zero/empty value, because their driver defaults are
    non-empty: an omitted ``--sources`` means 8, not none, and a replay that sampled boards the
    record never fetched would miss and be reported as a corpus hole that does not exist.
    """

    cfg = dict(config or {})
    families = ",".join(str(item) for item in cfg.get("families") or [])
    static_sources = harness.to_int(cfg.get("staticSources"))
    driver_args: list[str] = []
    if "staticSources" in cfg and static_sources <= 0:
        driver_args += ["--sources", "0"]
    if "families" in cfg and not families:
        driver_args += ["--families", ""]
    # A record made with a surface left unpatched replays the same way: intercepting more lanes
    # than the record used would let the replay ask the corpus for requests the record never made.
    surfaces = [str(item) for item in cfg.get("surfaces") or []]
    uncovered = [name for name in ALL_SURFACES if surfaces and name not in surfaces]
    if uncovered:
        driver_args += ["--no-surfaces", ",".join(uncovered)]
    only_sources = [str(item) for item in cfg.get("onlySources") or []]
    command = harness.build_command(
        mode="replay",
        epoch_dir=epoch_dir,
        run_dir=run_dir,
        scope=str(scope or "subset"),
        label=str(label),
        sources=static_sources,
        families=families,
        only_sources=",".join(only_sources),
        all_sources=bool(cfg.get("allSources")),
        max_workers=max(1, harness.to_int(cfg.get("maxWorkers"), default=1)),
        adapter_http_concurrency=harness.to_int(cfg.get("adapterHttpConcurrency")),
        google_sheets_redirect_concurrency=harness.to_int(
            cfg.get("googleSheetsRedirectConcurrency")
        ),
        driver_args=driver_args,
    )
    static_budget = str(cfg.get("staticSourceTimeBudgetS") or "").strip()
    env_extra = {truncation.KNOB_STATIC_SOURCE_CLOCK: static_budget} if static_budget else {}
    return command, env_extra


def judge_sample(
    *,
    run_dir: Path | str,
    exit_code: int,
    recorded: Mapping[str, Any],
    wall_ms: int = 0,
    epoch_dir: Path | str | None = None,
) -> dict[str, Any]:
    """One replay's outcome, read from the receipts it wrote. The verdict is derived, not asserted.

    Every problem the receipts support is collected, and the verdict is the first one in
    :data:`PROBLEM_KINDS` order -- so an epoch that both missed and drifted is reported as a miss,
    with the drift still visible in the row.

    A drift is the one problem with a second answer. When ``epoch_dir`` is given, the differing rows
    are read from the record and from this replay and attributed per source host (``binding``), and
    a drift whose whole residue belongs to hosts the record declared as key conflicts is
    ``parity-bound`` rather than falsifying. Everything else about it refuses exactly as before: a
    residue read from no rows, a row naming no source host, and one differing source outside the
    declared set all leave the verdict at ``parity-diverged``, with the binding's own reason in the
    finding so the refusal says which of them it was.
    """

    parity = harness.read_json_artifact(Path(run_dir), harness.PARITY_FILE)
    parity = dict(parity) if isinstance(parity, Mapping) else {}
    ledger = _as_mapping(parity.get("ledger"))
    guard = _as_mapping(parity.get("guard"))
    misses = [dict(item) for item in parity.get("misses") or [] if isinstance(item, Mapping)]
    status = str(parity.get("status") or "")
    observed = harness.run_summary(Path(run_dir))
    matches = parity_matches(recorded, observed) if recorded else {}
    divergences = [
        {"unit": unit, "expected": recorded.get(unit), "observed": observed.get(unit)}
        for unit, matched in matches.items()
        if not matched
    ]
    problems: list[dict[str, Any]] = []
    if not parity:
        problems.append(
            {
                "kind": VERDICT_HARNESS,
                "detail": (
                    f"no readable {harness.PARITY_FILE} under {run_dir}: the run's own outcome is "
                    "not on disk, so this epoch cannot be judged"
                ),
            }
        )
    else:
        missed = harness.to_int(ledger.get("requestsMissed"))
        if status == VERDICT_MISSED or misses or missed:
            named = sorted({str(item.get("url") or "") for item in misses} - {""})
            problems.append(
                {
                    "kind": VERDICT_MISSED,
                    "detail": (
                        f"{max(missed, len(misses))} request(s) the corpus could not answer; a "
                        "replay without the record's bound walks into them"
                    ),
                    "urls": named,
                    "surfaces": _surface_counts(misses),
                }
            )
        unresolved = harness.to_int(ledger.get("unattributedCount"))
        if unresolved:
            problems.append(
                {
                    "kind": VERDICT_UNANSWERED,
                    "detail": (
                        f"{unresolved} offer(s) the corpus never resolved: a body an entry names "
                        "but does not hold, a malformed entry, or a thread that died mid-offer"
                    ),
                    "urls": [str(item) for item in ledger.get("unattributedUrls") or []],
                }
            )
        pipeline_error = str(parity.get("pipelineError") or "").strip()
        if pipeline_error:
            problems.append(
                {
                    "kind": VERDICT_PIPELINE_ERROR,
                    "detail": (
                        "the run finished but its own report carries an error: "
                        f"{pipeline_error[:300]}"
                    ),
                }
            )
        if status == VERDICT_EGRESS:
            problems.append(
                {
                    "kind": VERDICT_EGRESS,
                    "detail": str(parity.get("error") or "the egress guard aborted the replay"),
                }
            )
    if not problems and int(exit_code) not in (EXIT_OK, EXIT_FALSIFIED):
        problems.append(
            {
                "kind": VERDICT_HARNESS,
                "detail": f"the driver exited {exit_code} with status {status or 'unknown'!r}",
            }
        )
    bound: dict[str, Any] = {}
    if not problems:
        if divergences:
            bound = binding.bind(epoch_dir=epoch_dir, run_dir=run_dir)
            if not bound.get("bound"):
                detail = "the replay did not reproduce the record: " + "; ".join(
                    f"{item['unit']} {item['expected']!r} -> {item['observed']!r}"
                    for item in divergences
                )
                reason = str(bound.get("reason") or "")
                if reason:
                    detail += f". Not bound to the record's declared conflict class: {reason}"
                problems.append(
                    {
                        "kind": VERDICT_DIVERGED,
                        "detail": detail,
                        "units": [str(item["unit"]) for item in divergences],
                        "binding": binding.summary(bound),
                    }
                )
        elif int(exit_code) == EXIT_FALSIFIED:
            problems.append(
                {
                    "kind": VERDICT_HARNESS,
                    "detail": (
                        "the driver reported parity-failed, but no unit re-derived from its "
                        "artifacts diverged from the record: compare its stored matches against "
                        "the run it wrote"
                    ),
                }
            )
    return {
        "runDir": str(run_dir),
        "exitCode": int(exit_code),
        "wallMs": max(0, int(wall_ms)),
        "status": status,
        "verdict": (
            str(problems[0]["kind"])
            if problems
            else (VERDICT_BOUND if bound.get("bound") else VERDICT_VERIFIED)
        ),
        "problems": problems,
        "matches": dict(matches),
        "divergences": divergences,
        "binding": bound,
        "misses": misses,
        # An aborted replay writes only its named misses, so an absent ledger stays absent rather
        # than reading as "offered 0, missed 0": a zero nobody measured is not a measurement.
        "ledger": (
            {
                key: harness.to_int(ledger.get(key))
                for key in (
                    "requestsOffered",
                    "requestsHit",
                    "requestsMissed",
                    "unattributedCount",
                )
            }
            if ledger
            else {}
        ),
        "guard": guard,
        "guardBlockedSources": sorted(guard_blocked_source_names(harness.run_report(run_dir))),
        "pipelineError": str(parity.get("pipelineError") or ""),
        "recordedRowCount": harness.to_int(recorded.get("rowCount")),
        "replayedRowCount": harness.to_int(observed.get("rowCount")),
    }


def run_sample(
    *,
    epoch_dir: Path | str,
    run_dir: Path | str,
    scope: str,
    config: Mapping[str, Any],
    recorded: Mapping[str, Any],
    index: int,
    label: str,
    timeout_s: int,
) -> dict[str, Any]:
    """One fresh-process replay, judged from the receipts it wrote.

    A driver that cannot be launched or that times out is a harness error with no exit code to
    report, not a corpus verdict: ``_NO_PROCESS_EXIT`` says the process never ran.
    """

    command, env_extra = build_epoch_command(
        epoch_dir=epoch_dir,
        run_dir=run_dir,
        scope=scope,
        config=config,
        label=label,
    )
    try:
        outcome = harness.run_driver(command, env_extra=env_extra, timeout_s=int(timeout_s))
    except harness.HarnessError as exc:
        return {
            "index": int(index),
            "runDir": str(run_dir),
            "command": command,
            "envExtra": dict(env_extra),
            "exitCode": _NO_PROCESS_EXIT,
            "wallMs": 0,
            "status": "",
            "verdict": VERDICT_HARNESS,
            "problems": [{"kind": VERDICT_HARNESS, "detail": str(exc)}],
            "matches": {},
            "divergences": [],
            "binding": {},
            "misses": [],
            "ledger": {},
            "guard": {},
            "guardBlockedSources": [],
            "pipelineError": "",
            "recordedRowCount": harness.to_int(recorded.get("rowCount")),
            "replayedRowCount": 0,
            "stderrTail": "",
        }
    sample = judge_sample(
        run_dir=run_dir,
        exit_code=int(outcome["exitCode"]),
        recorded=recorded,
        wall_ms=int(outcome["wallMs"]),
        # The epoch names the record's rows and its own declaration, which is what a drift is
        # bound against; a caller without one gets the plain divergence verdict.
        epoch_dir=epoch_dir,
    )
    sample.update(
        {
            "index": int(index),
            "command": command,
            "envExtra": dict(env_extra),
            # Kept only when the run failed: a healthy replay's stderr is progress chatter.
            "stderrTail": (
                str(outcome["stderr"] or "").strip()[-400:]
                if int(outcome["exitCode"]) != EXIT_OK
                else ""
            ),
        }
    )
    return sample


def _worst(verdicts: Sequence[str]) -> str:
    """The most severe verdict across samples: any finding outranks any pass."""

    failing = [verdict for verdict in verdicts if verdict != VERDICT_VERIFIED]
    if not failing:
        return VERDICT_VERIFIED
    return min(
        failing,
        key=lambda verdict: (
            PROBLEM_KINDS.index(verdict) if verdict in PROBLEM_KINDS else len(PROBLEM_KINDS)
        ),
    )


def _not_eligible_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """A row the listing does not call eligible, carried through so the count is complete."""

    return {
        "epochId": str(row.get("epochId") or ""),
        "dir": str(row.get("dir") or ""),
        "scope": str(row.get("scope") or ""),
        "recordedAt": str(row.get("recordedAt") or ""),
        "supersededBy": str(row.get("supersededBy") or ""),
        "verdictSource": str(row.get("verdictSource") or ""),
        "knobs": [str(knob) for knob in row.get("knobs") or []],
        "blockedBy": [str(item) for item in row.get("blockedBy") or []],
        "verdict": VERDICT_NOT_ELIGIBLE,
        "reason": str(row.get("reason") or ""),
        "samples": [],
        "problems": [],
        "binding": {},
        "misses": [],
    }


def check_epoch(
    row: Mapping[str, Any],
    *,
    out_dir: Path | str,
    samples: int = DEFAULT_SAMPLES,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    label: str = "replay-check",
) -> dict[str, Any]:
    """Replay one eligible epoch (``samples`` times) and report whether the listing's claim held."""

    epoch_dir = Path(str(row.get("dir") or ""))
    epoch_id = str(row.get("epochId") or epoch_dir.name)
    try:
        manifest = harness.epoch_summary(epoch_dir)
    except harness.HarnessError as exc:
        # The listing called it eligible, so this is a finding about the receipt, not a skip.
        return {
            "epochId": epoch_id,
            "dir": str(epoch_dir),
            "scope": str(row.get("scope") or ""),
            "recordedAt": str(row.get("recordedAt") or ""),
            "supersededBy": str(row.get("supersededBy") or ""),
            "verdictSource": str(row.get("verdictSource") or ""),
            "knobs": [str(knob) for knob in row.get("knobs") or []],
            "configSource": CONFIG_DEFAULTS,
            "staticSourceTimeBudgetS": "",
            "recordedRowCount": 0,
            "verdict": VERDICT_HARNESS,
            "problems": [{"kind": VERDICT_HARNESS, "detail": str(exc)}],
            "samples": [],
            "binding": {},
            "misses": [],
        }
    recorded = dict(manifest.get("recordedSummary") or {})
    config = recorded_config(manifest)
    epoch_samples = [
        run_sample(
            epoch_dir=epoch_dir,
            run_dir=Path(str(out_dir)) / epoch_id / f"sample-{index}",
            scope=str(row.get("scope") or manifest.get("scope") or "subset"),
            config=config,
            recorded=recorded,
            index=index,
            label=f"{label} {epoch_id} sample {index}",
            timeout_s=int(timeout_s),
        )
        for index in range(max(1, int(samples)))
    ]
    # The first sample that is *not* a pass, in :data:`ACCEPTED_VERDICTS` terms: a bound drift is a
    # pass, so its epoch keeps the binding block as its explanation rather than a problem.
    failing = next(
        (sample for sample in epoch_samples if not accepted(sample["verdict"])),
        None,
    )
    representative = failing or epoch_samples[0]
    misses = [item for sample in epoch_samples for item in sample.get("misses") or []]
    return {
        "epochId": epoch_id,
        "dir": str(epoch_dir),
        "scope": str(row.get("scope") or manifest.get("scope") or ""),
        "recordedAt": str(row.get("recordedAt") or ""),
        "supersededBy": str(row.get("supersededBy") or ""),
        "verdictSource": str(row.get("verdictSource") or ""),
        "knobs": [str(knob) for knob in row.get("knobs") or []],
        "configSource": CONFIG_RECORDED if config else CONFIG_DEFAULTS,
        "staticSourceTimeBudgetS": str(config.get("staticSourceTimeBudgetS") or ""),
        "recordedRowCount": harness.to_int(recorded.get("rowCount")),
        "epochGitSha": str(manifest.get("gitSha") or ""),
        "verdict": _worst([str(sample["verdict"]) for sample in epoch_samples]),
        "problems": list(representative.get("problems") or []),
        "binding": dict(representative.get("binding") or {}),
        "samples": epoch_samples,
        "misses": misses,
        "durationMs": sum(int(sample.get("wallMs") or 0) for sample in epoch_samples),
    }


def run_check(
    *,
    rows: Sequence[Mapping[str, Any]],
    out_dir: Path | str,
    samples: int = DEFAULT_SAMPLES,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    label: str = "replay-check",
) -> dict[str, Any]:
    """Every eligible row replayed; every other row carried through as ``not-eligible``."""

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    checked: list[dict[str, Any]] = []
    for row in rows:
        if not referees.is_eligible(row):
            checked.append(_not_eligible_row(row))
            continue
        result = check_epoch(
            row, out_dir=out, samples=int(samples), timeout_s=int(timeout_s), label=label
        )
        checked.append(result)
        # Flushed: a redirected run is watched through its log, and this is where a subset epoch
        # (minutes each) becomes visible as it finishes rather than only when the report is written.
        print(
            f"[replay-check] {result['epochId']}: {result['verdict']}"
            f" ({len(result.get('samples') or [])} sample(s),"
            f" {int(result.get('durationMs') or 0)} ms) -> {out / str(result['epochId'])}",
            flush=True,
        )
    counts = summarize(checked)
    return {
        "tool": "corpus-replay-check",
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "outDir": str(out),
        "config": {
            "corpusParent": "",
            "samples": int(samples),
            "timeoutS": int(timeout_s),
            "label": str(label),
        },
        "counts": counts,
        "exitCode": verdict_exit_code(counts),
        "epochs": checked,
    }


def summarize(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    verdicts = [str(row.get("verdict") or "") for row in rows]
    by_verdict = {verdict: verdicts.count(verdict) for verdict in sorted(set(verdicts)) if verdict}
    eligible = sum(1 for verdict in verdicts if verdict != VERDICT_NOT_ELIGIBLE)
    return {
        "epochs": len(rows),
        "eligible": eligible,
        "verified": verdicts.count(VERDICT_VERIFIED),
        "bound": verdicts.count(VERDICT_BOUND),
        "falsified": sum(1 for verdict in verdicts if verdict in FALSIFYING),
        "unjudged": sum(1 for verdict in verdicts if verdict in UNJUDGEABLE),
        "notEligible": verdicts.count(VERDICT_NOT_ELIGIBLE),
        "byVerdict": by_verdict,
    }


def verdict_exit_code(counts: Mapping[str, Any]) -> int:
    """One code for the run: the strongest finding it carries, then the refusal, then pass."""

    if harness.to_int(counts.get("falsified")):
        return EXIT_FALSIFIED
    if harness.to_int(counts.get("unjudged")):
        return EXIT_INFRASTRUCTURE
    if not harness.to_int(counts.get("eligible")):
        return EXIT_NOTHING_TO_CHECK
    return EXIT_OK


def epoch_row_for(epoch_dir: Path | str) -> dict[str, Any]:
    """The listing's own row for one epoch directory.

    Raises :class:`harness.HarnessError` only when no row can be built at all; an epoch the
    listing cannot judge is still a row, and the caller decides what that means for its purpose.
    """

    path = Path(epoch_dir)
    rows = referees.epochs_in_scope(corpus_parent=path.parent, epochs=[str(path)])
    if not rows:
        raise harness.HarnessError(f"no listing row could be built for {path}")
    return dict(rows[0])


def verification_receipt(
    *,
    row: Mapping[str, Any],
    result: Mapping[str, Any],
    out_dir: Path | str,
    code: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """The evidence one epoch's check leaves behind, written to ``out_dir`` and returned.

    One shape for every caller: a study gate and a record's birth check describe the same replay,
    and two shapes would drift. ``code`` is the caller's -- a receipt is a statement about a
    checkout, so it records which one, and a caller that could not read an identity says so with
    an empty one rather than inventing it.
    """

    exit_code = verdict_exit_code(summarize([result]))
    receipt_path = Path(out_dir) / RECEIPT_NAME
    receipt: dict[str, Any] = {
        "tool": "corpus-referee-verification",
        "epochId": str(result.get("epochId") or row.get("epochId") or ""),
        "epochDir": str(row.get("dir") or ""),
        "verdict": str(result.get("verdict") or ""),
        "exitCode": int(exit_code),
        "checkedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "code": {str(key): str(value) for key, value in (code or {}).items()},
        "listingVerdictSource": str(row.get("verdictSource") or ""),
        "configSource": str(result.get("configSource") or ""),
        "problems": [
            dict(item) for item in result.get("problems") or [] if isinstance(item, Mapping)
        ],
        # The full block, hosts and moved fields included: what a drift was bound to is the
        # evidence, and a summary of it would be a second statement about the same residue.
        "binding": dict(result.get("binding") or {}),
        "misses": [dict(item) for item in result.get("misses") or [] if isinstance(item, Mapping)],
        "sampleRunDirs": [
            str(sample.get("runDir") or "")
            for sample in result.get("samples") or []
            if isinstance(sample, Mapping)
        ],
        "reportDir": str(out_dir),
        "receipt": str(receipt_path),
    }
    write_json(receipt_path, receipt)
    return receipt


def manifest_attachment(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """The compact copy of a receipt that belongs in an epoch manifest.

    Verdict, provenance, counts and paths -- not every miss URL: the full evidence stays in the
    receipt file beside it, because a manifest is read by every listing query and two hundred URLs
    in it would tax every reader to answer a question the counts already answer.
    """

    problems = [item for item in receipt.get("problems") or [] if isinstance(item, Mapping)]
    code = receipt.get("code")
    attachment = {
        "verdict": str(receipt.get("verdict") or ""),
        "checkedAt": str(receipt.get("checkedAt") or ""),
        "exitCode": int(receipt.get("exitCode") or 0),
        "configSource": str(receipt.get("configSource") or ""),
        "listingVerdictSource": str(receipt.get("listingVerdictSource") or ""),
        "code": dict(code) if isinstance(code, Mapping) else {},
        "problemKinds": sorted({str(item.get("kind") or "") for item in problems} - {""}),
        "problems": len(problems),
        "misses": len(receipt.get("misses") or []),
        "sampleRunDirs": [str(item) for item in receipt.get("sampleRunDirs") or []],
        "receipt": str(receipt.get("receipt") or ""),
    }
    # Only when there is something to say: a compaction of an absent block would put a zero-
    # shaped binding into every manifest, which reads as a finding about a drift that never was.
    bound_block = receipt.get("binding")
    if isinstance(bound_block, Mapping) and bound_block:
        attachment["binding"] = binding.summary(bound_block)
    return attachment


def certification_held(attachment: Mapping[str, Any]) -> bool:
    """Whether a birth block lets the epoch stand as recorded: a pass, or an override.

    A pass is ``verified`` or ``parity-bound``: a divergence the record's own declaration explains
    is not a hole, so the epoch stays in the parent and supersedes like any certified record. The
    study gate never consults this -- it re-runs the check rather than trusting the block -- so an
    accepted refusal keeps a coverage record rather than making one a referee.
    """

    if accepted(attachment.get("verdict")):
        return True
    return bool(attachment.get("accepted"))


def _when(value: Any) -> str:
    text = str(value or "").strip()
    return text[:19].replace("T", " ") + "Z" if len(text) > 19 else (text or NOT_PRESENT)


def _epoch_cell(row: Mapping[str, Any]) -> str:
    return f"`{row.get('epochId')}`"


def _table(header: Sequence[str], body: Sequence[Sequence[str]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for cells in body:
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def _misses_cell(row: Mapping[str, Any]) -> str:
    misses = row.get("misses") or []
    if not misses:
        return NOT_PRESENT
    surfaces = _surface_counts(misses)
    return f"{len(misses)} ({', '.join(f'{name}: {count}' for name, count in surfaces.items())})"


def _sample_cell(row: Mapping[str, Any]) -> str:
    samples = row.get("samples") or []
    if not samples:
        return NOT_PRESENT
    exits = ",".join(str(sample.get("exitCode")) for sample in samples)
    return f"exit {exits}"


def _rows_cell(row: Mapping[str, Any]) -> str:
    """Recorded -> replayed rows, or ``-`` for an epoch that was never replayed."""

    samples = row.get("samples") or []
    if not samples:
        return NOT_PRESENT
    return f"{row.get('recordedRowCount', 0)} -> {samples[0].get('replayedRowCount', 0)}"


def _problem_lines(row: Mapping[str, Any]) -> list[str]:
    """The finding in the reader's terms: what failed, then the evidence, then what to do about it."""

    lines: list[str] = []
    for problem in row.get("problems") or []:
        lines.append(f"- **{problem.get('kind')}**: {problem.get('detail')}")
        urls = [str(url) for url in problem.get("urls") or [] if str(url)]
        for url in urls[:_NAMED_MISSES]:
            lines.append(f"  - `{url}`")
        if len(urls) > _NAMED_MISSES:
            lines.append(f"  - ... and {len(urls) - _NAMED_MISSES} more (see the JSON copy)")
    samples = row.get("samples") or []
    for sample in samples:
        if sample.get("guardBlockedSources"):
            lines.append(
                "- not covered by the corpus, failed on the guard: "
                + ", ".join(str(name) for name in sample["guardBlockedSources"])
            )
        if sample.get("stderrTail"):
            lines.append(f"- stderr tail: `{str(sample['stderrTail']).strip()}`")
    return lines


def render_markdown(
    rows: Sequence[Mapping[str, Any]], *, corpus_parent: Path | str, out_dir: Path | str
) -> str:
    """The page a reviewer reads instead of the receipts."""

    counts = summarize(rows)
    replays = sum(len(row.get("samples") or []) for row in rows)
    if counts["eligible"] == 0:
        headline = (
            "Nothing to verify: the listing calls no epoch eligible, so no replay could certify "
            "anything. Re-record with the knob the listing names, or pass epoch directories "
            "explicitly."
        )
    elif counts["falsified"]:
        headline = (
            "The certification did not hold: at least one epoch the listing calls eligible could "
            "not be replayed to its record. The findings are below."
        )
    elif counts["unjudged"]:
        headline = (
            "No falsification, but at least one eligible epoch could not be judged: the harness,"
            " not the corpus, is what failed."
        )
    elif counts["bound"]:
        headline = (
            "Every epoch the listing calls eligible replayed end to end. The ones that did not"
            " reproduce their record diverged only inside a conflict class the record declared"
            " per host, and are bound to it rather than refused -- see below for what was bound."
        )
    else:
        headline = (
            "Every epoch the listing calls eligible replayed end to end and reproduced its record."
        )

    lines: list[str] = [
        "# Referee replay check",
        "",
        f"- corpus parent: `{corpus_parent}`",
        f"- out: `{out_dir}`",
        f"- eligible epochs replayed: {counts['eligible']} -- {counts['verified']} verified,"
        f" {counts['bound']} bound to a declared conflict class, {counts['falsified']} falsified,"
        f" {counts['unjudged']} could not be judged",
        f"- listed epochs not replayed (the listing does not call them eligible):"
        f" {counts['notEligible']}",
        "",
        headline,
        "",
    ]

    if rows:
        lines.extend(
            _table(
                ("epoch", "verdict", "config", "exit", "rows rec -> rep", "misses", "recorded"),
                [
                    [
                        _epoch_cell(row),
                        str(row.get("verdict") or NOT_PRESENT),
                        str(row.get("configSource") or NOT_PRESENT),
                        _sample_cell(row),
                        _rows_cell(row),
                        _misses_cell(row),
                        _when(row.get("recordedAt")),
                    ]
                    for row in rows
                ],
            )
        )
        lines.append("")

    bound_rows = [
        row for row in rows if str(row.get("verdict") or "") == VERDICT_BOUND and row.get("binding")
    ]
    if bound_rows:
        lines.extend(["## Bound to a conflict class the record declared", ""])
        lines.append(
            "These epochs did not reproduce their record. Every row they differ by belongs to a"
            " host the record's own conflict audit declared as a key conflict -- one request key the"
            " live run was served more than one response for, which a corpus of one response per key"
            " cannot replay -- so the drift is explained by the record rather than refused. The bind"
            " is refused the moment a differing source falls outside the declared hosts."
        )
        lines.append("")
        for row in bound_rows:
            block = row.get("binding") or {}
            declaration = block.get("declared") or {}
            analysis = block.get("residue") or {}
            lines.append(f"### `{row.get('epochId')}` -- {row.get('verdict')}")
            lines.append("")
            lines.append(f"- {block.get('reason') or NOT_PRESENT}")
            lines.append(
                f"- declared by `{declaration.get('document') or NOT_PRESENT}`"
                f" ({declaration.get('source') or 'unknown'}"
                f"{'' if declaration.get('complete') else ', top hosts only'}):"
                f" {declaration.get('hostCount') or 0} host(s)"
            )
            moved = ", ".join(
                f"{name}: {count}" for name, count in (analysis.get("fields") or {}).items()
            )
            lines.append(
                f"- residue: {analysis.get('rows') or 0} row(s) -- {analysis.get('recordOnly') or 0}"
                f" record-only, {analysis.get('replayOnly') or 0} replay-only,"
                f" {analysis.get('drifted') or 0} with moved fields ({moved or 'none'})"
            )
            for item in analysis.get("hosts") or []:
                lines.append(
                    f"  - `{item.get('host')}`: {item.get('recordOnly')} record-only,"
                    f" {item.get('replayOnly')} replay-only, {item.get('drifted')} drifted"
                )
            lines.append("")

    failed = [row for row in rows if row.get("verdict") in FALSIFYING | UNJUDGEABLE]
    if failed:
        lines.extend(["## What did not verify", ""])
        for row in failed:
            lines.append(
                f"### `{row.get('epochId')}` -- {row.get('verdict')}"
                f" (listing: {row.get('verdictSource') or 'unknown'})"
            )
            lines.append("")
            lines.extend(_problem_lines(row))
            lines.append("")

    skipped = [row for row in rows if row.get("verdict") == VERDICT_NOT_ELIGIBLE]
    if skipped:
        lines.extend(["## Not replayed", ""])
        lines.extend(
            _table(
                ("epoch", "listing said", "blocked by", "raise"),
                [
                    [
                        _epoch_cell(row),
                        str(row.get("verdictSource") or NOT_PRESENT),
                        "; ".join(row.get("blockedBy") or [])
                        or str(row.get("reason") or NOT_PRESENT),
                        " ".join(f"`{knob}`" for knob in row.get("knobs") or []) or NOT_PRESENT,
                    ]
                    for row in skipped
                ],
            )
        )
        lines.append("")

    lines.extend(
        [
            "## Caveats",
            "",
            "- The verdict is about **this machine today**: that the corpus answered every request"
            " the replay offered and that today's code reproduced the record's rows, digests and"
            " per-source statuses. Replay removes the network, so this is not a statement about"
            " live wall-clock behaviour.",
            f"- {replays} replay(s) were run; one sample per epoch unless `--samples` says more."
            " A single clean replay is evidence, not a proof of stability.",
            "- Coverage is the intercepted lanes. A source that leaves them fails against the"
            " guard and is named per sample; a divergence caused by that is a coverage limit rather"
            " than a corpus hole, and it is reported as a divergence because a study would be"
            " refused on it either way.",
            "- An epoch whose record predates `recordedConfig` is replayed on the driver defaults,"
            " and its row says `configSource: defaults` -- a weaker statement than a recorded"
            " configuration, labelled rather than blurred.",
            "- Parity is the driver's own comparison (`run_fetch.parity_matches`), so"
            " `parity-diverged` means exactly what `parity-failed` means in the run's own receipts.",
            "- `parity-bound` is a labelled pass, not a reproduction. The replay's differing rows"
            " were read from the record's own rows and attributed to the host of the source that"
            " carried each one, and every one of those hosts is a key conflict the record itself"
            " declared -- so replay cannot reach the record's rows for them. The bind refuses on a"
            " single differing source outside the declared set, on a differing row that names no"
            " source host, when the record's rows are no longer on disk, and when the epoch declares"
            " nothing; it is host-level, so a divergence inside a declared host for another reason"
            " is bound too, which is why the moved fields are listed above.",
            "",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "epochs",
        nargs="*",
        help="explicit epoch directories to verify; default: every epoch under --corpus-parent",
    )
    parser.add_argument("--corpus-parent", default=str(DEFAULT_CORPUS_PARENT))
    parser.add_argument(
        "--out",
        default="",
        help="where the check writes its run directories and report (default: an _out token)",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=DEFAULT_SAMPLES,
        help="replays per epoch; every one must hold for the epoch to verify",
    )
    parser.add_argument("--timeout-s", type=int, default=DEFAULT_TIMEOUT_S)
    parser.add_argument(
        "--no-recompute",
        action="store_true",
        help=(
            "resolve the listing without re-running an epoch's truncation audit; an epoch with no "
            "recorded verdict is then not judgeable and is not replayed"
        ),
    )
    parser.add_argument("--label", default="replay-check")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    parent = Path(args.corpus_parent)
    epochs = [str(item) for item in args.epochs or []]
    if not epochs and not parent.is_dir():
        print(f"[replay-check] no epoch parent at {parent}", file=sys.stderr)
        return EXIT_INFRASTRUCTURE
    out_dir = Path(args.out) if str(args.out).strip() else parent / new_run_token("replay-check")

    rows = referees.epochs_in_scope(
        corpus_parent=parent, recompute=not args.no_recompute, epochs=epochs
    )
    if not rows:
        print(f"[replay-check] no recorded epochs under {parent}", file=sys.stderr)
        return EXIT_INFRASTRUCTURE

    report = run_check(
        rows=rows,
        out_dir=out_dir,
        samples=int(args.samples),
        timeout_s=int(args.timeout_s),
        label=str(args.label),
    )
    report["config"]["corpusParent"] = str(parent)
    write_json(out_dir / "replay-check.json", report)
    # Rendered from the *checked* rows: a listing row's ``verdict`` is the delta-referee block the
    # audit produced, and this page reports what the replay did.
    markdown = render_markdown(report["epochs"], corpus_parent=parent, out_dir=out_dir)
    (out_dir / "replay-check.md").write_text(markdown + "\n", encoding="utf-8")
    print(markdown)
    print(f"[replay-check] wrote {out_dir / 'replay-check.json'}", flush=True)

    if report["exitCode"] == EXIT_FALSIFIED:
        print(
            "[replay-check] refusing the certification: the listing called an epoch eligible that"
            " the replay could not substantiate",
            file=sys.stderr,
        )
    return int(report["exitCode"])


if __name__ == "__main__":
    raise SystemExit(main())
