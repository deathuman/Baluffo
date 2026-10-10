"""S7: one command answering "is this change faster?" over replay, and saying when it cannot.

The plan asks for paired, alternating-order arms, a confidence interval and an explicit
``inconclusive`` outcome -- because a harness that always names a winner is how an 11% effect gets
manufactured out of two runs that differed by 438 rows. Three decisions carry that:

**Arms alternate, and never run at once.** Pair *i* runs A then B, pair *i+1* runs B then A, so a
warming cache or a machine getting busier mid-study cannot favour one arm. The runs of a pair are
strictly sequential: concurrent runs would make CPU contention, not the change under test, the
largest term in the delta.

**The verdict comes from a paired statistic, not from two medians.** Each pair yields one delta;
the study reports the median of those deltas and a 95% percentile bootstrap interval over the
*pair deltas*, so drift between pairs widens the interval instead of silently moving the verdict.
The interval must exclude zero *and* the median effect must clear ``--min-effect-pct``, which is
what makes ``inconclusive`` a reachable answer rather than a formality.

**The report says what it measured and what it did not.** Replay removes network time, so a result
here is a statement about the engine, while the shipped claim this tool exists to adjudicate was
about wall clock including the wire. Every report carries that banner, both arms' medians, the
parity line the comparison is only valid under, and the retry/backoff share that sits inside the
wall clock but not inside the CPU story.

**An interrupted study is resumed, not repeated.** ``--resume`` reuses any pair whose *both* arms
finished hermetically under the configuration this command asks for, and re-runs everything else in
full -- never half a pair. A reused arm's delta still comes from its run's own report window, which
is the metric the verdict already uses; its process wall clock and exit code do not survive on disk,
so those are marked as read from the artifacts rather than presented as observed.

**A study verifies its referee before it starts.** The listing is a claim about the record;
``replay_check.py`` is the fresh replay that tests it. Before the first pair, the pinned epoch goes
through that same check -- one replay, under the epoch's own recorded configuration where it has
one -- and the study refuses with the check's own semantics unless the listing certifies the epoch
and the replay substantiates it: 1 when the replay falsifies the certification (a corpus miss, an
unanswered offer, a pipeline error, a parity divergence), 2 when it cannot be judged at all, 3 when
the listing does not certify the epoch. That ordering is the point: discovering a corpus hole after
paying for six pairs is the failure this gate exists to prevent, so no arm runs on a refusal. The
check's report and a receipt of the code that ran it stay in the study directory, and the A/B
report names them.

**A drift the record itself declared is bound, not refused.** One class a corpus cannot replay is a
request key the live run was served more than one response for -- ``conflicts.py`` declares those
keys per host, and ``replay_check`` answers ``parity-bound`` when every differing row belongs to a
host the record declared. The same bind is then applied to the arms, and a pair whose arms both
drift inside that class *and agree with each other* stays comparable: the study's question is which
arm is faster at the same work, so the arms are graded against each other as well as against the
record, and one unit on a host the record never declared still refuses the pair. See
``binding.py``.

The statistic is not invented here: the delta and its pass/warn/fail line come from
``scripts/perf_compare.py``, and each arm's samples are written as baseline-shaped records through
``scripts/perf_baseline.py`` -- into this study's own directory, so an ad-hoc A/B cannot displace
the repository's shared baseline. Per D7 the command is advisory: it exits non-zero only when the
comparison is *invalid* (a parity divergence or a failed run), never because a result was
inconvenient.

Usage::

    python tools/measurements/corpus/ab.py run --epoch _out/perf-runs/corpus-subset-<stamp> \\
        --pairs 6 --arm-a-label baseline --arm-b-label candidate \\
        --arm-a-concurrency 32 --arm-b-concurrency 48
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.perf_baseline import build_baseline_record, write_baseline_record  # noqa: E402
from scripts.perf_compare import compare_duration  # noqa: E402
from tools.measurements.corpus import (
    binding,  # noqa: E402
    harness,  # noqa: E402
    referees,  # noqa: E402
)
from tools.measurements.corpus import epoch as epoch_mod  # noqa: E402
from tools.measurements.corpus import replay_check as check_mod  # noqa: E402

# One shared reader for every corpus number: the local body was a third copy of this.
from tools.measurements.corpus.harness import to_int as _int
from tools.measurements.corpus.run_fetch import new_run_token, write_json  # noqa: E402

DEFAULT_PAIRS = 6
DEFAULT_MIN_EFFECT_PCT = 2.0
CI_SEED = 20261008
CI_ITERATIONS = 8000
PARITY_UNITS: tuple[str, ...] = (
    "rowCount",
    "identityDigest",
    "rowProjectionDigest",
    "sourceStatusCounts",
)
ARMS: tuple[str, ...] = ("a", "b")
PARITY_OK = "parity-ok"
PARITY_FAILED = "parity-failed"
NOT_PRODUCTION_BANNER = (
    "Replay measures the engine, not the wire: network time is removed, so a result here is a "
    "statement about the pipeline's own work. A refutation is trustworthy; a confirmation still "
    "needs a live paired run before anyone quotes it as a production wall-clock win."
)
EXIT_OK = 0
EXIT_PARITY_FAILED = 1
EXIT_INFRASTRUCTURE = 2
# The listing's own refusal code, shared with the record it comes from: "this epoch cannot referee
# a delta" is one fact, and a second number for it would let a script tell the two apart.
EXIT_NO_REFEREE = referees.EXIT_NO_REFEREE
VERIFICATION_DIRNAME = check_mod.VERIFICATION_DIRNAME
VERIFICATION_FILE = check_mod.RECEIPT_NAME
# How many miss URLs a refusal names before counting the rest; the receipt keeps every one.
_REFUSAL_URLS = 5


def alternating_order(index: int) -> tuple[str, str]:
    """Pair *i*'s run order: A first on even pairs, B first on odd ones."""

    return ("a", "b") if int(index) % 2 == 0 else ("b", "a")


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _median(values: Sequence[float]) -> float:
    return round(float(statistics.median(values)), 3) if values else 0.0


def _median_int(values: Any) -> int:
    numbers = [_int(value) for value in (values or [])]
    return _int(statistics.median(numbers)) if numbers else 0


def _ms_cell(value: Any) -> str:
    """A millisecond cell, or ``n/a`` when the number was never observed (a resumed arm)."""

    amount = _int(value)
    return f"{amount} ms" if amount > 0 else "n/a"


def effective_wall_ms(sample: Mapping[str, Any]) -> int:
    """The run's own report window when it has one, else the process wall clock as a fallback."""

    report_wall = _int(sample.get("reportWallMs"))
    return report_wall if report_wall > 0 else _int(sample.get("wallMs"))


def paired_deltas(pairs: Sequence[Mapping[str, Any]]) -> list[float]:
    """One delta per pair: ``(b - a) / a * 100``, so negative means B was faster."""

    deltas: list[float] = []
    for pair in pairs:
        a_wall = effective_wall_ms(pair.get("a") or {})
        b_wall = effective_wall_ms(pair.get("b") or {})
        if a_wall > 0 and b_wall > 0:
            deltas.append(round(((b_wall - a_wall) / a_wall) * 100.0, 3))
    return deltas


def bootstrap_ci(
    values: Sequence[float],
    *,
    seed: int = CI_SEED,
    iterations: int = CI_ITERATIONS,
    alpha: float = 0.05,
) -> list[float]:
    """Percentile bootstrap CI of the median, seeded so a rerun of the same data agrees.

    Pure Python on purpose: the repo takes no new dependencies and resampling a handful of pair
    deltas needs none. One value has no spread to estimate, so its interval is itself.
    """

    clean = [float(value) for value in values]
    if not clean:
        return [0.0, 0.0]
    if len(clean) == 1:
        return [round(clean[0], 3), round(clean[0], 3)]
    rng = random.Random(int(seed))
    count = len(clean)
    medians = sorted(
        statistics.median(clean[rng.randrange(count)] for _ in range(count))
        for _ in range(max(1, int(iterations)))
    )
    last = len(medians) - 1
    low = medians[min(last, int((alpha / 2.0) * last))]
    high = medians[min(last, int((1.0 - alpha / 2.0) * last))]
    return [round(float(low), 3), round(float(high), 3)]


def verdict_for(median: float, ci: Sequence[float], min_effect_pct: float) -> str:
    """``inconclusive`` unless the interval excludes zero *and* the effect is worth naming."""

    low, high = (_float(ci[0]), _float(ci[1])) if len(ci) >= 2 else (0.0, 0.0)
    if low <= 0.0 <= high or abs(_float(median)) < abs(_float(min_effect_pct)):
        return "inconclusive"
    return "arm-b-faster" if _float(median) < 0 else "arm-a-faster"


def parity_divergences(
    units_by_run: Sequence[Mapping[str, Any]], recorded: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Every unit of every run that differs from the epoch's recorded units.

    A divergence is not a slow arm, it is an invalid comparison: the arms no longer produced the
    same jobs, so their wall clocks are not measuring the same work.
    """

    divergences: list[dict[str, Any]] = []
    for entry in units_by_run:
        units = entry.get("units") or {}
        for unit in PARITY_UNITS:
            actual = units.get(unit) if isinstance(units, Mapping) else None
            if actual != recorded.get(unit):
                divergences.append(
                    {
                        "run": str(entry.get("run") or ""),
                        "unit": unit,
                        "expected": recorded.get(unit),
                        "actual": actual,
                    }
                )
    return divergences


def decide(
    divergences: Sequence[Mapping[str, Any]],
    failures: Sequence[Mapping[str, Any]],
    *,
    median: float,
    ci: Sequence[float],
    min_effect_pct: float,
) -> tuple[str, str]:
    """Status and verdict from the evidence, in one place so no caller has to guess.

    Invalid comparison first: a failed run, or arms that produced different jobs, has no verdict
    to give -- reporting one would be the confident wrong answer this spine exists to prevent.
    """

    if failures:
        return "harness-error", "inconclusive"
    if divergences:
        return "parity-failed", "inconclusive"
    return "ok", verdict_for(median, ci, min_effect_pct)


def classify_pair_divergences(
    pairs: Sequence[Mapping[str, Any]],
    recorded: Mapping[str, Any],
    *,
    units: Sequence[str] = PARITY_UNITS,
) -> dict[str, Any]:
    """Split every arm divergence into the ones the record's declaration explains and the rest.

    ``parity_divergences`` answers whether an arm reproduced the record. That stops being the right
    question once the record carries a declared conflict class, because *no* arm can reproduce it: a
    request key the live run was served several responses for has one response in the corpus, so the
    rows built from the others are unreachable by construction. The right question is whether the
    record's own declaration explains every unit an arm differs by -- the same bind
    ``replay_check`` applies to the birth replay, read from the same artifacts (``binding.py``) --
    and, because the arms are no longer being graded against the record alone, whether the two arms
    of a pair **agree with each other**. The study's question is which arm is faster at the same
    work, so two arms that produced different jobs have no delta to report, declared class or not:
    a unit the arms disagree on is unexplained however well each arm's own residue is declared.

    A pair with no divergence at all contributes nothing, which is what leaves a clean study's
    report exactly as it was. ``decide`` refuses on the unexplained part alone, so a declaration
    that explains every arm's residue turns a refusal into a measurement -- and one unit on a host
    the record never declared keeps the refusal, because that arm's bind failed.
    """

    declared: list[dict[str, Any]] = []
    unexplained: list[dict[str, Any]] = []
    residue_hosts: list[str] = []
    diverging_arms = 0
    bound_arms = 0
    bound_pairs = 0
    for pair in pairs:
        by_arm = {arm: dict(pair.get(arm) or {}) for arm in ARMS}
        values = {arm: dict(by_arm[arm].get("units") or {}) for arm in ARMS}
        blocks = {arm: dict(by_arm[arm].get("binding") or {}) for arm in ARMS}
        diverging = {
            arm: [unit for unit in units if values[arm].get(unit) != recorded.get(unit)]
            for arm in ARMS
        }
        if not any(diverging.values()):
            continue
        diverging_arms += sum(1 for arm in ARMS if diverging[arm])
        # The arms first, because they are the study's own comparison: a pair whose arms produced
        # different work has no delta to report, so it is refused whole rather than partly bound --
        # attributing its *other* units to the declaration would claim a class explains a pair that
        # is invalid for a reason the declaration knows nothing about.
        if not all(values["a"].get(unit) == values["b"].get(unit) for unit in units):
            for unit in units:
                a_value, b_value = values["a"].get(unit), values["b"].get(unit)
                if a_value == b_value:
                    continue
                unexplained.append(
                    {
                        "pair": pair.get("index"),
                        "arm": "a|b",
                        "run": f"{by_arm['a'].get('runDir')} vs {by_arm['b'].get('runDir')}",
                        "unit": unit,
                        "expected": a_value,
                        "actual": b_value,
                        "reason": (
                            "the two arms produced different work for this unit, so the pair has "
                            "no comparable delta -- a class the record declared cannot explain two "
                            "arms that disagree with each other"
                        ),
                    }
                )
            continue
        bound_arms += sum(1 for arm in ARMS if diverging[arm] and blocks[arm].get("bound"))
        # The arms agree, so their divergences are the same units: the pair is bound when every arm
        # that differs from the record has a binding that explains it.
        if all(blocks[arm].get("bound") for arm in ARMS if diverging[arm]):
            bound_pairs += 1
        for unit in units:
            if unit not in diverging["a"]:
                continue
            for arm in ARMS:
                entry: dict[str, Any] = {
                    "pair": pair.get("index"),
                    "arm": arm,
                    "run": str(by_arm[arm].get("runDir") or ""),
                    "unit": unit,
                    "expected": recorded.get(unit),
                    "actual": values[arm].get(unit),
                }
                if blocks[arm].get("bound"):
                    declared.append({**entry, "binding": binding.summary(blocks[arm])})
                    for host in (blocks[arm].get("residue") or {}).get("hosts") or []:
                        name = str(host.get("host") or "")
                        if name and name not in residue_hosts:
                            residue_hosts.append(name)
                else:
                    unexplained.append(
                        {
                            **entry,
                            "reason": str(
                                blocks[arm].get("reason") or "no binding could be computed"
                            ),
                        }
                    )
    return {
        "declared": declared,
        "unexplained": unexplained,
        "divergingArms": diverging_arms,
        "boundArms": bound_arms,
        "boundPairs": bound_pairs,
        "residueHosts": residue_hosts,
    }


def arm_env(extra: Mapping[str, str] | None) -> dict[str, str]:
    """Arm environment: counters on by default so the retry/backoff share exists in the report."""

    env = {str(name): str(value) for name, value in (extra or {}).items()}
    env.setdefault(harness.COUNTERS_ENV, "1")
    return env


class RefereeRefused(RuntimeError):
    """The pinned epoch may not referee: the listing refuses it, or a replay does not certify it."""

    def __init__(
        self,
        message: str,
        *,
        exit_code: int,
        listing: Mapping[str, Any] | None = None,
        verification: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.exit_code = int(exit_code)
        self.listing = dict(listing or {})
        self.verification = dict(verification or {})


def _listing_refusal(row: Mapping[str, Any]) -> str:
    """Why the listing will not certify the epoch, with the knob and the ways forward."""

    epoch_id = str(row.get("epochId") or "?")
    source = str(row.get("verdictSource") or "unknown")
    blocked = "; ".join(str(item) for item in row.get("blockedBy") or [])
    if not blocked:
        blocked = str(row.get("reason") or "no reason was recorded")
    knobs = ", ".join(str(knob) for knob in row.get("knobs") or [])
    remedy = (
        f"raise {knobs} and re-record it, or pin an epoch the listing certifies"
        if knobs
        else "re-record it, or pin an epoch the listing certifies"
    )
    note = (
        " (recorded as accepted by --accept-truncated-referee: an override, not a certification)"
        if referees.is_accepted(row)
        else ""
    )
    return (
        f"refusing to pin `{epoch_id}`: the listing does not certify it as a delta referee "
        f"(verdict from {source}){note}: {blocked}; {remedy}"
    )


def _replay_refusal(result: Mapping[str, Any], *, receipt: Mapping[str, Any]) -> str:
    """Why the replay will not substantiate the certification, naming the evidence it left."""

    epoch_id = str(result.get("epochId") or "?")
    verdict = str(result.get("verdict") or "unknown")
    problems = [item for item in result.get("problems") or [] if isinstance(item, Mapping)]
    first = dict(problems[0]) if problems else {}
    detail = str(first.get("detail") or "no finding was recorded")
    urls = [str(url) for url in first.get("urls") or [] if str(url)]
    named = ""
    if urls:
        named = " [" + ", ".join(f"`{url}`" for url in urls[:_REFUSAL_URLS])
        if len(urls) > _REFUSAL_URLS:
            named += f", and {len(urls) - _REFUSAL_URLS} more"
        named += "]"
    outcome = (
        "could not be judged" if verdict in check_mod.UNJUDGEABLE else "could not substantiate that"
    )
    return (
        f"refusing to pin `{epoch_id}`: the listing calls it a delta referee, but one fresh "
        f"replay {outcome} ({verdict}: {detail}{named}). No pair ran; the check's evidence is "
        f"`{receipt.get('receipt')}`."
    )


def verify_referee(
    epoch_dir: Path | str,
    *,
    out_dir: Path | str,
    timeout_s: int = 3600,
    label: str = "referee verification",
) -> dict[str, Any]:
    """One fresh replay of the pinned epoch, or a refusal: the precondition a study starts from.

    The listing answers which epoch *may* referee from what the record said; this runs the one
    thing that tests the answer -- ``replay_check.check_epoch``, the same replay the check command
    performs, under the epoch's own ``recordedConfig`` where it has one. The study is never the
    place to discover a corpus hole after paying for pairs: a refusal exits before the first arm,
    with the check's own semantics (1 falsified, 2 unjudgeable, 3 the listing refuses the epoch),
    and the check's report stays on disk as the evidence for it.
    """

    try:
        row = check_mod.epoch_row_for(epoch_dir)
    except harness.HarnessError as exc:
        raise RefereeRefused(str(exc), exit_code=EXIT_NO_REFEREE) from exc
    if not referees.is_eligible(row):
        raise RefereeRefused(_listing_refusal(row), exit_code=EXIT_NO_REFEREE, listing=row)

    verification_dir = Path(out_dir)
    verification_dir.mkdir(parents=True, exist_ok=True)
    result = check_mod.check_epoch(
        row,
        out_dir=verification_dir,
        samples=1,
        timeout_s=int(timeout_s),
        label=str(label),
    )
    git_sha, dirty_hash = epoch_mod.git_identity(_ROOT)
    receipt = check_mod.verification_receipt(
        row=row,
        result=result,
        out_dir=verification_dir,
        code={"gitSha": git_sha, "dirtyDiffHash": dirty_hash},
    )
    # ``parity-bound`` is a pass, and a labelled one: the epoch did not reproduce itself and every
    # row it differs by belongs to a conflict class the record declared per host, so no pair is
    # refused for a drift replay cannot avoid. ``classify_pair_divergences`` applies the same bind
    # to the arms, which is what makes the study measurable rather than merely admissible.
    if not check_mod.accepted(receipt["verdict"]):
        raise RefereeRefused(
            _replay_refusal(result, receipt=receipt),
            exit_code=int(receipt["exitCode"]),
            listing=row,
            verification=receipt,
        )
    return receipt


def pair_dir(out_dir: Path | str, index: int, arm: str) -> Path:
    """Where pair *index*'s arm writes its run: the layout ``--resume`` reads back."""

    return Path(out_dir) / f"pair-{int(index) + 1:02d}" / f"arm-{arm}"


def _config_mismatch(
    parity: Mapping[str, Any],
    *,
    epoch_id: str,
    scope: str,
    sources: int,
    families: str,
    all_sources: bool,
    max_workers: int,
    concurrency: int,
) -> str:
    """How a completed arm's recorded run differs from this invocation's request, or ``""``.

    A resumed study must not splice two different experiments together: an arm left by another
    epoch, scope, worker count or pool size is a different measurement wearing the same path.
    """

    run_config = parity.get("runConfig")
    run_config = run_config if isinstance(run_config, Mapping) else {}
    recorded_epoch = str(parity.get("epochId") or "")
    if recorded_epoch and epoch_id and recorded_epoch != epoch_id:
        return f"recorded epoch {recorded_epoch} != {epoch_id}"
    if str(run_config.get("mode") or "") != "replay":
        return f"recorded mode {run_config.get('mode')!r} is not a replay"
    recorded_scope = str(parity.get("epochScope") or run_config.get("scope") or "")
    if scope and recorded_scope and recorded_scope != str(scope):
        return f"recorded scope {recorded_scope!r} != {scope!r}"
    if _int(run_config.get("maxWorkers")) != int(max_workers):
        return f"recorded maxWorkers {run_config.get('maxWorkers')} != {int(max_workers)}"
    if bool(run_config.get("allSources")) != bool(all_sources):
        return (
            f"recorded allSources {bool(run_config.get('allSources'))} != {bool(all_sources)} "
            "for this arm"
        )
    if _int(run_config.get("adapterHttpConcurrency")) != int(concurrency):
        return (
            f"recorded adapterHttpConcurrency {run_config.get('adapterHttpConcurrency')} != "
            f"{int(concurrency)} for this arm"
        )
    if int(sources) > 0 and _int(run_config.get("staticSources")) != int(sources):
        return f"recorded staticSources {run_config.get('staticSources')} != {int(sources)}"
    recorded_families = run_config.get("families")
    asked = [part.strip() for part in str(families).split(",") if part.strip()]
    if isinstance(recorded_families, Sequence) and asked:
        recorded_list = sorted(str(item) for item in recorded_families)
        if recorded_list != sorted(asked):
            return f"recorded families {recorded_list} != {sorted(asked)}"
    guard = parity.get("guard")
    guard = guard if isinstance(guard, Mapping) else {}
    if _int(guard.get("blockedRemote")) > 0:
        return (
            f"recorded {_int(guard.get('blockedRemote'))} blocked remote connect(s): not hermetic"
        )
    return ""


def reused_pair(
    *,
    index: int,
    out_dir: Path,
    epoch_id: str,
    labels: Mapping[str, str],
    scope: str,
    sources: int,
    families: str,
    all_sources: bool,
    max_workers: int,
    concurrency: Mapping[str, int],
    epoch_dir: Path | str | None = None,
) -> tuple[dict[str, Any] | None, str]:
    """A completed pair rebuilt from its own artifacts, or why it cannot be reused.

    Added because long studies get interrupted: a finished pair is minutes of machine time, and
    re-running it would also fold two machine states into one study. The delta comes from each
    arm's own report window -- the metric the verdict already uses -- so a reused pair is
    comparable, not approximated. Only a pair whose *both* arms are complete, hermetic and
    configured as requested is reused; anything partial or mismatched is re-run in full rather
    than half-adopted. An absent pair directory is silent (there is nothing to explain); a pair
    that exists but cannot be used returns the reason, so the report says what was re-run.
    """

    if not (Path(out_dir) / f"pair-{int(index) + 1:02d}").is_dir():
        return None, ""
    samples: dict[str, Any] = {}
    for arm in ARMS:
        run_dir = pair_dir(out_dir, index, arm)
        observation = harness.observe(run_dir)
        parity = observation["parity"]
        summary = observation["summary"]
        if not parity or _int(summary.get("rowCount") or 0) <= 0:
            return None, f"arm {arm} left no complete run artifacts"
        status = str(parity.get("status") or "")
        # A ``parity-failed`` arm is reusable when the record's own declaration explains the drift,
        # for the same reason a fresh one is admissible: the pair is graded on the arms agreeing
        # with each other, and re-running an arm a declaration already explains would spend the
        # machine time ``--resume`` exists to save.
        block = binding.bind(epoch_dir=epoch_dir, run_dir=run_dir) if epoch_dir else {}
        if status != PARITY_OK and not (status == PARITY_FAILED and block.get("bound")):
            return None, f"arm {arm} parity status {status!r}"
        mismatch = _config_mismatch(
            parity,
            epoch_id=epoch_id,
            scope=scope,
            sources=int(sources),
            families=families,
            all_sources=bool(all_sources),
            max_workers=int(max_workers),
            concurrency=int(concurrency[arm]),
        )
        if mismatch:
            return None, f"arm {arm} {mismatch}"
        samples[arm] = {
            "label": labels[arm],
            "runDir": str(run_dir),
            "command": "",
            # No process exit code survives on disk, so completion is taken from the artifacts
            # above and labelled as such rather than presented as an observed exit.
            "exitCode": 0,
            "exitCodeSource": "artifacts (pair reused with --resume)",
            "wallMs": 0,
            "reportWallMs": _int(observation["wallMs"]),
            "rows": _int(summary.get("rowCount") or 0),
            "units": {unit: summary.get(unit) for unit in PARITY_UNITS},
            "binding": block,
            "repeatedFetchSharePct": _float((observation["backoff"] or {}).get("sharePct")),
            "guard": dict(parity.get("guard") or {}),
            "stderrTail": "",
            "resumed": True,
        }
    deltas = paired_deltas([samples])
    return {
        "index": int(index),
        "order": list(alternating_order(index)),
        **samples,
        "deltaPct": deltas[0] if deltas else 0.0,
        "resumed": True,
    }, ""


def _run_arm(
    *,
    arm: str,
    position: int,
    index: int,
    epoch_dir: Path,
    out_dir: Path,
    labels: Mapping[str, str],
    envs: Mapping[str, Mapping[str, str]],
    args_by_arm: Mapping[str, Sequence[str]],
    concurrency: Mapping[str, int],
    scope: str,
    sources: int,
    families: str,
    all_sources: bool,
    max_workers: int,
    timeout_s: int,
) -> dict[str, Any]:
    """One arm of one pair: a fresh replay process, then the receipts it wrote."""

    run_dir = out_dir / f"pair-{index + 1:02d}" / f"arm-{arm}"
    result = harness.run_driver(
        harness.build_command(
            mode="replay",
            epoch_dir=epoch_dir,
            run_dir=run_dir,
            scope=scope,
            label=f"{labels[arm]} pair {index + 1} position {position + 1}",
            sources=sources,
            families=families,
            all_sources=bool(all_sources),
            max_workers=max_workers,
            adapter_http_concurrency=concurrency[arm],
            driver_args=args_by_arm[arm],
        ),
        env_extra=envs[arm],
        timeout_s=timeout_s,
    )
    observation = harness.observe(run_dir)
    summary = observation["summary"]
    exit_code = int(result["exitCode"])
    return {
        "label": labels[arm],
        "runDir": str(run_dir),
        "command": result["command"],
        "exitCode": exit_code,
        "wallMs": int(result["wallMs"]),
        "reportWallMs": int(observation["wallMs"]),
        "rows": int(summary.get("rowCount") or 0),
        "units": {unit: summary.get(unit) for unit in PARITY_UNITS},
        # Read here rather than at comparison time: the arm's own rows are the evidence, and the
        # record's run directory can be rotated away between the run and the report.
        "binding": binding.bind(epoch_dir=epoch_dir, run_dir=run_dir),
        "repeatedFetchSharePct": _float((observation["backoff"] or {}).get("sharePct")),
        "guard": dict((observation["parity"] or {}).get("guard") or {}),
        "stderrTail": str(result["stderr"] or "").strip()[-400:] if exit_code else "",
    }


def run_ab(
    *,
    epoch_dir: Path | str,
    out_dir: Path | str,
    pairs: int = DEFAULT_PAIRS,
    arm_a_label: str = "a",
    arm_b_label: str = "b",
    arm_a_env: Mapping[str, str] | None = None,
    arm_b_env: Mapping[str, str] | None = None,
    arm_a_args: Sequence[str] = (),
    arm_b_args: Sequence[str] = (),
    arm_a_concurrency: int = 0,
    arm_b_concurrency: int = 0,
    scope: str = "subset",
    sources: int = 0,
    families: str = "google_sheets",
    all_sources: bool = False,
    max_workers: int = 1,
    min_effect_pct: float = DEFAULT_MIN_EFFECT_PCT,
    timeout_s: int = 3600,
    resume: bool = False,
) -> dict[str, Any]:
    """Run the paired study, write ``ab-report.json`` + ``ab-report.md``, and return the report."""

    epoch_path = harness.resolve_epoch(epoch_dir)
    epoch = harness.epoch_summary(epoch_path)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    # Before the first pair, so a corpus hole costs one replay rather than a whole study.
    referee = verify_referee(
        epoch_path,
        out_dir=out / VERIFICATION_DIRNAME,
        timeout_s=int(timeout_s),
        label=f"referee verification {epoch['epochId']}",
    )
    print(
        f"[ab] referee `{epoch['epochId']}` verified by one fresh replay; starting "
        f"{max(1, int(pairs))} pair(s)",
        flush=True,
    )
    shared: dict[str, Any] = {
        "epoch_dir": epoch_path,
        "out_dir": out,
        "labels": {"a": str(arm_a_label), "b": str(arm_b_label)},
        "envs": {"a": arm_env(arm_a_env), "b": arm_env(arm_b_env)},
        "args_by_arm": {
            "a": tuple(str(item) for item in arm_a_args),
            "b": tuple(str(item) for item in arm_b_args),
        },
        "concurrency": {"a": int(arm_a_concurrency), "b": int(arm_b_concurrency)},
        "scope": scope,
        "sources": int(sources),
        "families": families,
        "all_sources": bool(all_sources),
        "max_workers": int(max_workers),
        "timeout_s": int(timeout_s),
    }
    pair_reports: list[dict[str, Any]] = []
    resume_notes: list[str] = []
    resumed_arms = 0
    for index in range(max(1, int(pairs))):
        if resume:
            reused, reason = reused_pair(
                index=index,
                out_dir=out,
                epoch_id=str(epoch["epochId"]),
                labels=shared["labels"],
                scope=scope,
                sources=int(sources),
                families=families,
                all_sources=bool(all_sources),
                max_workers=int(max_workers),
                concurrency=shared["concurrency"],
                epoch_dir=epoch_path,
            )
            if reused is not None:
                pair_reports.append(reused)
                resumed_arms += len(ARMS)
                continue
            if reason:
                resume_notes.append(f"pair {index + 1} was re-run rather than reused: {reason}")
        order = alternating_order(index)
        samples = {
            arm: _run_arm(arm=arm, position=position, index=index, **shared)
            for position, arm in enumerate(order)
        }
        deltas = paired_deltas([samples])
        pair_reports.append(
            {
                "index": index,
                "order": list(order),
                **samples,
                "deltaPct": deltas[0] if deltas else 0.0,
            }
        )
    failures = [
        {
            "run": sample["runDir"],
            "exitCode": sample["exitCode"],
            "stderrTail": sample["stderrTail"],
        }
        for pair in pair_reports
        for sample in (pair["a"], pair["b"])
        if int(sample["exitCode"]) != 0
    ]
    # An arm that did not reproduce the record is admissible while the record's own declaration
    # explains every unit it differs by *and* both arms of its pair agree with each other. Anything
    # else is the divergence that invalidates a comparison, and it is the only thing that does.
    classified = classify_pair_divergences(pair_reports, epoch["recordedSummary"])
    divergences = classified["unexplained"]
    deltas = paired_deltas(pair_reports)
    median = _median(deltas)
    ci = bootstrap_ci(deltas)
    status, verdict = decide(
        divergences, failures, median=median, ci=ci, min_effect_pct=float(min_effect_pct)
    )
    arms = {
        arm: {
            "label": shared["labels"][arm],
            "medianWallMs": _int(statistics.median([s[arm]["wallMs"] for s in pair_reports])),
            "medianReportWallMs": _int(
                statistics.median([s[arm]["reportWallMs"] for s in pair_reports])
            ),
            "samples": [s[arm]["reportWallMs"] for s in pair_reports],
            "rows": [s[arm]["rows"] for s in pair_reports],
            "repeatedFetchSharePct": _median(
                [s[arm]["repeatedFetchSharePct"] for s in pair_reports]
            ),
        }
        for arm in ARMS
    }
    report: dict[str, Any] = {
        "status": status,
        "verdict": verdict,
        "medianDeltaPct": median,
        "ci95": ci,
        "minEffectPct": float(min_effect_pct),
        "pairs": pair_reports,
        "deltasPct": deltas,
        "arms": arms,
        "parity": {
            "epochId": epoch["epochId"],
            "units": list(PARITY_UNITS),
            "recorded": {unit: epoch["recordedSummary"].get(unit) for unit in PARITY_UNITS},
            "divergences": divergences,
            "declaredClass": {
                "divergences": classified["declared"],
                "divergingArms": classified["divergingArms"],
                "boundArms": classified["boundArms"],
                "boundPairs": classified["boundPairs"],
                "residueHosts": classified["residueHosts"],
            },
        },
        "repeatedFetches": {arm: arms[arm]["repeatedFetchSharePct"] for arm in ARMS},
        "epoch": epoch,
        "referee": referee,
        "perfCompare": compare_duration(
            current_duration_ms=arms["b"]["medianReportWallMs"] or arms["b"]["medianWallMs"],
            baseline_duration_ms=arms["a"]["medianReportWallMs"] or arms["a"]["medianWallMs"],
        ),
        "runConfig": {
            "scope": scope,
            "sources": int(sources),
            "families": families,
            "allSources": bool(all_sources),
            "maxWorkers": int(max_workers),
            "armAConcurrency": int(arm_a_concurrency),
            "armBConcurrency": int(arm_b_concurrency),
            "armAEnv": shared["envs"]["a"],
            "armBEnv": shared["envs"]["b"],
            "pairs": len(pair_reports),
        },
        "banner": NOT_PRODUCTION_BANNER,
        "resume": {
            "requested": bool(resume),
            "reusedArms": resumed_arms,
            "reusedPairs": resumed_arms // len(ARMS),
            "rerunPairs": len(resume_notes),
        },
        "notes": [
            f"both arms ran with {harness.COUNTERS_ENV}=1 so the retry/backoff share exists; an "
            "arm that sets that key explicitly overrides it",
            "pairs run sequentially, one arm after the other: concurrent runs would measure "
            "contention instead of the change under test",
            "the pinned epoch was verified by one fresh replay before any pair ran; the check's "
            "report sits under referee-verification/, and a listing that refuses the epoch or a "
            "replay that cannot substantiate it never reaches a pair",
            *(
                [
                    f"{resumed_arms} arm(s) were reused from disk (--resume): their deltas come "
                    "from each run's own report window, their process wall clock was not "
                    "observed, and their completion is read from their artifacts"
                ]
                if resumed_arms
                else []
            ),
            *(
                [
                    f"{classified['boundPairs']} pair(s) diverged from the record inside a conflict "
                    "class the pinned epoch declared per host "
                    f"({', '.join(classified['residueHosts']) or 'no named host'}), and both arms "
                    "of each agreed with each other, so those pairs stayed comparable: replay "
                    "cannot hold the several responses the live run was served for one request "
                    "key, and the record declares those keys per host rather than pretending "
                    "they do not exist"
                ]
                if classified["boundPairs"]
                else []
            ),
            *resume_notes,
        ],
        "advisory": "not CI-gating (D7): the existing smoke gate already blocks PRs",
    }
    json_path = out / "ab-report.json"
    md_path = out / "ab-report.md"
    report["artifacts"] = {"json": str(json_path), "md": str(md_path)}
    markdown = render_markdown(report)
    write_json(json_path, report)
    md_path.write_text(markdown + "\n", encoding="utf-8")
    write_arm_baselines(report, out)
    print(markdown)
    return report


def write_arm_baselines(report: Mapping[str, Any], out_dir: Path) -> None:
    """Record both arms through ``perf_baseline``, into this study's own directory.

    So an A/B's arms land in the repository's baseline *format* without touching its shared
    ``_out/perf-baseline``: a throwaway study must not displace the baseline the CI gate compares
    against. One file per arm, because ``perf_baseline`` keys its path by mode and a shared mode
    would let the second arm overwrite the first.
    """

    epoch_id = str((report.get("epoch") or {}).get("epochId") or "epoch")
    for arm in ARMS:
        arm_report = (report.get("arms") or {}).get(arm) or {}
        record = build_baseline_record(
            mode=f"replay-ab-{epoch_id}-arm-{arm}",
            total_duration_ms=_int(arm_report.get("medianReportWallMs")),
            status=str(report.get("status") or ""),
            source_count=int((report.get("runConfig") or {}).get("sources") or 0),
            wall_clock_ms=_int(arm_report.get("medianWallMs")),
            artifact=str(out_dir),
        )
        record["arm"] = str(arm_report.get("label") or arm)
        record["samples"] = list(arm_report.get("samples") or [])
        write_baseline_record(
            record, baseline_dir=out_dir / "baselines", trend_path=out_dir / "perf-trend.ndjson"
        )


def _referee_line(report: Mapping[str, Any]) -> list[str]:
    """The study's precondition as one line: which epoch, verified, and where the receipt is."""

    referee = report.get("referee") or {}
    verdict = str(referee.get("verdict") or "")
    if not verdict:
        return []
    bound = (
        " The replay did not reproduce the record, and every row it differs by belongs to a host"
        " the epoch declared as a key conflict, so the drift is bound rather than refused."
        if verdict == check_mod.VERDICT_BOUND
        else ""
    )
    return [
        f"**Referee verification:** `{referee.get('epochId')}` — {verdict} by one "
        f"fresh replay before any pair ran (receipt `{referee.get('receipt')}`).{bound}"
    ]


def render_markdown(report: Mapping[str, Any]) -> str:
    """The human-actionable report: verdict, interval, both arms, and what it cannot claim."""

    arms = report.get("arms") or {}
    arm_a, arm_b = arms.get("a") or {}, arms.get("b") or {}
    ci = report.get("ci95") or [0.0, 0.0]
    divergences = (report.get("parity") or {}).get("divergences") or []
    declared_class = (report.get("parity") or {}).get("declaredClass") or {}
    lines = [
        f"# Replay A/B — {report.get('verdict')} ({report.get('status')})",
        "",
        f"**Verdict:** `{report.get('verdict')}` — median paired delta "
        f"{_float(report.get('medianDeltaPct')):+.2f}% (95% CI {_float(ci[0]):+.2f}% to "
        f"{_float(ci[1]):+.2f}%); negative means B faster.",
        f"**Materiality bar:** below {_float(report.get('minEffectPct'))}% reads as inconclusive "
        f"even when the interval excludes zero.",
        "",
        "| arm | label | median report wall | median process wall | rows |",
        "|---|---|---|---|---|",
        f"| A | {arm_a.get('label')} | {arm_a.get('medianReportWallMs')} ms | "
        f"{_ms_cell(arm_a.get('medianWallMs'))} | {_median_int(arm_a.get('rows'))} |",
        f"| B | {arm_b.get('label')} | {arm_b.get('medianReportWallMs')} ms | "
        f"{_ms_cell(arm_b.get('medianWallMs'))} | {_median_int(arm_b.get('rows'))} |",
        "",
        f"**Pairs:** {len(report.get('pairs') or [])} (alternating order) · **deltas:** "
        f"{[round(_float(value), 2) for value in (report.get('deltasPct') or [])]}",
        *(
            [
                f"**Resumed:** {_int((report.get('resume') or {}).get('reusedPairs'))} of "
                f"{len(report.get('pairs') or [])} pair(s) rebuilt from disk (`--resume`): the "
                "deltas come from each run's own report window, and the process wall clock of "
                "those arms was not observed."
            ]
            if _int((report.get("resume") or {}).get("reusedPairs")) > 0
            else []
        ),
        f"**Parity vs epoch:** {(report.get('parity') or {}).get('epochId')} — "
        f"{len(divergences)} divergence(s); a divergence invalidates the comparison and "
        f"suppresses the verdict.",
        *(
            [
                f"**Divergences bound to a declared class:** {_int(declared_class.get('boundPairs'))}"
                f" of {len(report.get('pairs') or [])} pair(s) -- every arm difference lands on "
                f"{', '.join(str(host) for host in declared_class.get('residueHosts') or []) or 'no named host'},"
                " the hosts the epoch's own conflict audit declared, and both arms of each of those"
                " pairs agreed with each other, so their deltas still compare the same work."
            ]
            if _int(declared_class.get("boundPairs"))
            else []
        ),
        *_referee_line(report),
        f"**Repeated-fetch exposure:** A {arm_a.get('repeatedFetchSharePct')}% · "
        f"B {arm_b.get('repeatedFetchSharePct')}% of summed source time — the share in sources "
        "that asked for the same URL twice (a retry or an intentional re-fetch, which the counter "
        "cannot separate); retry backoff still sleeps in replay, so it sits inside the wall clock "
        "this compares.",
        "",
        NOT_PRODUCTION_BANNER,
        "",
        f"Advisory: {report.get('advisory')}",
        *[f"- {note}" for note in (report.get("notes") or [])],
    ]
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run a paired A/B over one recorded epoch")
    run.add_argument("--epoch", required=True, help="epoch directory to replay")
    run.add_argument("--out", default="", help="study directory (default: an _out/perf-runs token)")
    for name, kind, default in (
        ("--pairs", int, DEFAULT_PAIRS),
        ("--arm-a-concurrency", int, 0),
        ("--arm-b-concurrency", int, 0),
        ("--sources", int, 0),
        ("--max-workers", int, 1),
        ("--min-effect-pct", float, DEFAULT_MIN_EFFECT_PCT),
        ("--timeout-s", int, 3600),
    ):
        run.add_argument(name, type=kind, default=default)
    for name, default in (
        ("--arm-a-label", "a"),
        ("--arm-b-label", "b"),
        ("--scope", "subset"),
        ("--families", "google_sheets"),
    ):
        run.add_argument(name, default=default)
    run.add_argument(
        "--all-sources",
        action="store_true",
        help="replay every registered loader (a full-source epoch) instead of a stride sample",
    )
    for name in ("--arm-a-env", "--arm-b-env", "--arm-a-arg", "--arm-b-arg"):
        run.add_argument(name, action="append", default=[])
    run.add_argument("--json", action="store_true", help="print report JSON instead of markdown")
    run.add_argument(
        "--resume",
        action="store_true",
        help="reuse complete pairs already under --out instead of re-running them",
    )
    return parser


def parse_env_pairs(values: Sequence[str]) -> dict[str, str]:
    """``KEY=VALUE`` pairs from the CLI, with a loud failure rather than a dropped key."""

    env: dict[str, str] = {}
    for item in values:
        name, separator, value = str(item).partition("=")
        if not separator or not name.strip():
            raise ValueError(f"expected KEY=VALUE, got {item!r}")
        env[name.strip()] = value
    return env


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    out_dir = (
        Path(args.out) if str(args.out) else _ROOT / "_out" / "perf-runs" / new_run_token("ab")
    )
    try:
        report = run_ab(
            epoch_dir=args.epoch,
            out_dir=out_dir,
            pairs=args.pairs,
            arm_a_label=args.arm_a_label,
            arm_b_label=args.arm_b_label,
            arm_a_env=parse_env_pairs(args.arm_a_env),
            arm_b_env=parse_env_pairs(args.arm_b_env),
            arm_a_args=tuple(args.arm_a_arg),
            arm_b_args=tuple(args.arm_b_arg),
            arm_a_concurrency=args.arm_a_concurrency,
            arm_b_concurrency=args.arm_b_concurrency,
            scope=args.scope,
            sources=args.sources,
            families=args.families,
            all_sources=bool(args.all_sources),
            max_workers=args.max_workers,
            min_effect_pct=args.min_effect_pct,
            timeout_s=args.timeout_s,
            resume=bool(args.resume),
        )
    except (harness.HarnessError, ValueError) as exc:
        print(f"[ab] {exc}", file=sys.stderr)
        return EXIT_INFRASTRUCTURE
    except RefereeRefused as exc:
        print(f"[ab] {exc}", file=sys.stderr)
        return int(exc.exit_code)
    if bool(args.json):
        print(json.dumps({k: v for k, v in report.items() if k != "pairs"}, indent=2, default=str))
    status = str(report.get("status") or "")
    if status == "harness-error":
        return EXIT_INFRASTRUCTURE
    if status == "parity-failed":
        return EXIT_PARITY_FAILED
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
