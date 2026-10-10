"""S0: adjudicate the shipped pool claim on this harness -- and say what the harness can see.

``61c43fab`` ships *"−11.3% wall clock"* from four Docker arms. The plan defers correcting or
confirming it until replay exists (D3), then runs the pool A/B **on the harness**, paired and
alternating, and accepts either a reproducible verdict or a refutation, with *inconclusive*
permitted rather than forced.

The one thing this tool must never do is turn a harness limitation into a verdict. Replay removes
network time, so it can refute a claim about *work* and can never confirm a claim about *wire*
time -- and this particular claim changes ``adapter_http_concurrency``, whose only in-process
consumer is ``AsyncHttpTextFetcher``, which a replay never constructs because a custom
``fetch_text`` short-circuits ``resolve_fetch_text_impl``. So the adjudication carries two
independent lines of evidence:

1. **Structural** -- the variable is bypassed in replay, checked against the product's own source
   (and the run's own report, which records ``fetchClient: custom``).
2. **Empirical** -- the paired arms' delta and its confidence interval.

When the structural check says the variable is out of reach, the verdict is
``inconclusive-by-harness`` no matter what the numbers did, and the report names the live,
non-hermetic experiment that would actually settle it. Reporting a confident "no effect" from a
harness that cannot see the mechanism is exactly the failure mode the plan was written against.
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

from tools.measurements.corpus import ab, harness  # noqa: E402
from tools.measurements.corpus.run_fetch import new_run_token, write_json  # noqa: E402

CLAIM: dict[str, Any] = {
    "id": "61c43fab",
    "statement": "perf(fetch): give the adapter HTTP pool room to hide latency (-11.3% wall)",
    "claimPct": -11.3,
    "metric": "wall clock",
    "variable": "adapter_http_concurrency -> httpx pool connections",
    "baseline": {"label": "pool-64", "concurrency": 32, "note": "cap 32, maxWorkers*4 = 48"},
    "candidate": {"label": "pool-96", "concurrency": 48, "note": "cap 64, maxWorkers*4 = 48"},
    "measuredHow": "four Docker arms, frozen seed, live HTTP, --cpus 2.0 --memory 2.5g",
}
SHORT_CIRCUIT_FILE = "src/jobs/transport.py"
SHORT_CIRCUIT_NEEDLE = "if fetch_text is not default_fetch_text"
VARIABLE_TOKEN = "adapter_http_concurrency"
LIVE_PROTOCOL = (
    "a live paired study, not a replay: two container arms on the same frozen seed "
    "(--cpus 2.0 --memory 2.5g), adapterHttpConcurrency 32 vs 48, alternating order, >=6 pairs, "
    "wall clock as the metric, on a day the boards are stable -- because the effect this claim "
    "asserts is hidden network latency, which replay removes by construction."
)
VERDICT_CONFIRMED = "confirmed"
VERDICT_REFUTED = "refuted"
VERDICT_INCONCLUSIVE = "inconclusive"
KIND_NEEDS_LIVE = "replay-confirmation-needs-live-run"
KIND_REFUTED = "refuted-on-engine-time"
KIND_UNREACHABLE = "inconclusive-by-harness"
KIND_NO_EFFECT = "inconclusive-no-separating-effect"
KIND_HARNESS = "harness-failed"


def short_circuit_evidence(root: Path | None = None) -> dict[str, Any]:
    """Does the product still short-circuit a custom ``fetch_text``?

    Read from the source rather than asserted: if that early return ever went away, this
    adjudication's central claim about what replay can see would silently stop being true.
    """

    path = Path(root or _ROOT) / SHORT_CIRCUIT_FILE
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return {"file": SHORT_CIRCUIT_FILE, "readable": False, "error": str(exc)}
    return {
        "file": SHORT_CIRCUIT_FILE,
        "readable": True,
        "shortCircuitsCustomFetch": SHORT_CIRCUIT_NEEDLE in text,
        "needle": SHORT_CIRCUIT_NEEDLE,
    }


def variable_consumers(root: Path | None = None) -> dict[str, Any]:
    """Where ``adapter_http_concurrency`` is read, so the bypass is inspectable not asserted."""

    base = Path(root or _ROOT)
    occurrences: list[str] = []
    for path in sorted((base / "src").rglob("*.py")):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        hits = [
            f"{index}: {line.strip()}"
            for index, line in enumerate(text.splitlines(), start=1)
            if VARIABLE_TOKEN in line
        ]
        if hits:
            occurrences.append(f"{path.relative_to(base).as_posix()} -> {'; '.join(hits[:4])}")
    return {"token": VARIABLE_TOKEN, "occurrences": occurrences, "count": len(occurrences)}


def reachability(
    *, epoch_dir: Path | str, run_dir: Path | str, root: Path | None = None
) -> dict[str, Any]:
    """Can a replay of this epoch see a change to the claim's variable? Evidence, then answer."""

    evidence = {
        "shortCircuit": short_circuit_evidence(root),
        "consumers": variable_consumers(root),
        "runConfig": _run_runtime(Path(run_dir)),
        "epoch": harness.epoch_summary(epoch_dir),
    }
    runtime = evidence["runConfig"]
    fetch_client = str(runtime.get("fetchClient") or "")
    bypassed = bool(evidence["shortCircuit"].get("shortCircuitsCustomFetch"))
    proven_bypass = bypassed and fetch_client == "custom"
    # Reachability is only claimed when it is observed: an unreadable run configuration leaves
    # the variable's reach unproven, and an unproven reach must not become a confident "no
    # effect" verdict. The conservative answer here is the honest one.
    reachable = bool(fetch_client) and not proven_bypass
    if proven_bypass:
        reason = (
            "a replay supplies a custom fetch_text, which short-circuits resolve_fetch_text_impl, "
            "so AsyncHttpTextFetcher -- the only in-process reader of adapter_http_concurrency -- "
            "is never constructed and the pool size is never applied"
        )
    elif not fetch_client:
        reason = (
            "the run reported no fetch client, so the variable's reach in a replay is unproven; "
            "an unproven reach is reported as unreachable rather than as no effect"
        )
    else:
        reason = f"the variable appears reachable: the run reported fetchClient={fetch_client}"
    return {
        "reachable": reachable,
        "reachabilityProven": bool(proven_bypass or reachable),
        "fetchClient": fetch_client,
        "reason": reason,
        "evidence": evidence,
    }


def _run_runtime(run_dir: Path) -> dict[str, Any]:
    data_dir = run_dir / "data" if (run_dir / "data").is_dir() else run_dir
    report = harness.read_json_artifact(data_dir, harness.REPORT_FILE)
    runtime = (report or {}).get("runtime") if isinstance(report, Mapping) else None
    if not isinstance(runtime, Mapping):
        return {}
    keys = (
        "maxWorkers",
        "fetchClient",
        "adapterHttpConcurrency",
        "forceRefreshAll",
        "selectedSourceCount",
    )
    return {key: runtime.get(key) for key in keys}


def claim_inside_ci(claim_pct: float, ci95: Sequence[float]) -> bool:
    if len(list(ci95)) < 2:
        return False
    low, high = float(ci95[0]), float(ci95[1])
    return low <= float(claim_pct) <= high


def decide_verdict(
    *,
    ab_report: Mapping[str, Any],
    reachable: bool,
    claim_pct: float,
    min_effect_pct: float,
) -> dict[str, Any]:
    """The verdict and, just as important, which *kind* of verdict the evidence supports."""

    status = str(ab_report.get("status") or "")
    if status != "ok":
        return {
            "verdict": VERDICT_INCONCLUSIVE,
            "verdictKind": KIND_HARNESS,
            "reason": f"the A/B did not produce comparable arms (status {status or 'unknown'})",
        }
    median = float(ab_report.get("medianDeltaPct") or 0.0)
    ci = [float(value) for value in (ab_report.get("ci95") or [])]
    inside = claim_inside_ci(claim_pct, ci)
    shared = {
        "medianDeltaPct": median,
        "ci95": ci,
        "claimPct": round(float(claim_pct), 3),
        "claimInsideCi": inside,
        "minEffectPct": round(float(min_effect_pct), 3),
    }
    if not reachable:
        return {
            **shared,
            "verdict": VERDICT_INCONCLUSIVE,
            "verdictKind": KIND_UNREACHABLE,
            "reason": (
                "the claim's variable is bypassed in a replay, so no delta here -- large or small "
                "-- bears on it; the arms agreeing is evidence of the bypass, not of no effect"
            ),
            "whatWouldSettleIt": LIVE_PROTOCOL,
        }
    separates = bool(ci) and not (ci[0] <= 0.0 <= ci[1]) and abs(median) >= float(min_effect_pct)
    if not separates:
        return {
            **shared,
            "verdict": VERDICT_INCONCLUSIVE,
            "verdictKind": KIND_NO_EFFECT,
            "reason": "the paired interval crosses zero, or the effect is below the materiality bar",
            "whatWouldSettleIt": LIVE_PROTOCOL,
        }
    faster_arm = "candidate" if median < 0 else "baseline"
    kind = KIND_REFUTED if faster_arm == "baseline" else KIND_NEEDS_LIVE
    return {
        **shared,
        "verdict": VERDICT_REFUTED if kind == KIND_REFUTED else VERDICT_CONFIRMED,
        "verdictKind": kind,
        "fasterArm": faster_arm,
        "reason": (
            "a replay can refute a wall-clock claim: the engine time demonstrably did not shrink"
            if kind == KIND_REFUTED
            else "engine time moved in the claim's direction, but a replay removes network time, "
            "so this is not yet a production wall-clock win"
        ),
        "whatWouldSettleIt": LIVE_PROTOCOL,
    }


def release_note_text(verdict: Mapping[str, Any]) -> str:
    """The correction D2 asks for, drafted only when there is a refutation to correct."""

    if str(verdict.get("verdictKind")) != KIND_REFUTED:
        return ""
    return (
        "### Fetch HTTP pool tuning: claim not reproducible on the hermetic harness\n\n"
        f"`{CLAIM['id']}` recorded `{CLAIM['statement']}`. Re-measured on the corpus replay "
        f"harness (paired, alternating, {verdict.get('medianDeltaPct')}% median, 95% CI "
        f"{verdict.get('ci95')}), the engine time did not shrink: the pool change acts on network "
        "latency hiding, which the harness removes by construction. A live paired run is still "
        "required before the number is quoted as a production wall-clock win.\n"
    )


def run_adjudication(
    *,
    epoch_dir: Path | str,
    out_dir: Path | str,
    pairs: int = 6,
    min_effect_pct: float = 2.0,
    scope: str = "subset",
    sources: int = 0,
    families: str = "google_sheets",
    max_workers: int = 1,
    timeout_s: int = 3600,
    resume: bool = False,
) -> dict[str, Any]:
    """Run the claim's A/B on the harness, then decide -- with the harness's reach stated."""

    out_dir = Path(out_dir)
    ab_report = ab.run_ab(
        epoch_dir=epoch_dir,
        out_dir=out_dir / "ab",
        pairs=pairs,
        arm_a_label=str(CLAIM["baseline"]["label"]),
        arm_b_label=str(CLAIM["candidate"]["label"]),
        arm_a_concurrency=int(CLAIM["baseline"]["concurrency"]),
        arm_b_concurrency=int(CLAIM["candidate"]["concurrency"]),
        scope=scope,
        sources=sources,
        families=families,
        max_workers=max_workers,
        min_effect_pct=min_effect_pct,
        timeout_s=timeout_s,
        resume=bool(resume),
    )
    first_run = _first_run_dir(ab_report)
    reach = (
        reachability(epoch_dir=epoch_dir, run_dir=first_run)
        if first_run
        else {"reachable": False, "fetchClient": "", "reason": "no run artifacts", "evidence": {}}
    )
    verdict = decide_verdict(
        ab_report=ab_report,
        reachable=bool(reach["reachable"]),
        claim_pct=float(CLAIM["claimPct"]),
        min_effect_pct=min_effect_pct,
    )
    return {
        "tool": "corpus-s0-adjudication",
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "claim": dict(CLAIM),
        "epoch": ab_report.get("epoch") or {},
        "config": {
            "pairs": int(pairs),
            "minEffectPct": float(min_effect_pct),
            "scope": scope,
            "sources": int(sources),
            "families": families,
            "maxWorkers": int(max_workers),
            "resume": bool(resume),
        },
        "reachability": reach,
        "ab": ab_report,
        "verdict": verdict,
        "releaseNoteDraft": release_note_text(verdict),
    }


def _first_run_dir(ab_report: Mapping[str, Any]) -> Path | None:
    pairs = ab_report.get("pairs") or []
    if not pairs:
        return None
    first = pairs[0] if isinstance(pairs[0], Mapping) else {}
    for arm in ("a", "b"):
        candidate = (
            (first.get(arm) or {}).get("runDir") if isinstance(first.get(arm), Mapping) else ""
        )
        if candidate:
            return Path(str(candidate))
    return None


def _ms_cell(value: Any) -> str:
    """A millisecond cell, or ``n/a`` when nothing observed it (a pair reused from disk)."""

    try:
        amount = int(float(value))
    except (TypeError, ValueError):
        return "n/a"
    return f"{amount} ms" if amount > 0 else "n/a"


def render_markdown(payload: Mapping[str, Any]) -> str:
    """The one page a reviewer reads: claim, reach, numbers, verdict kind, what would settle it."""

    claim = payload.get("claim") or {}
    verdict = payload.get("verdict") or {}
    reach = payload.get("reachability") or {}
    ab_report = payload.get("ab") or {}
    arms = ab_report.get("arms") or {}
    referee = ab_report.get("referee") or {}
    lines = [
        f"# S0 adjudication — `{claim.get('id', '?')}` on the corpus harness",
        "",
        f"Claim: *{claim.get('statement', '')}*",
        "",
        f"- variable: `{claim.get('variable', '')}` — {claim.get('baseline', {}).get('label', '?')} "
        f"({claim.get('baseline', {}).get('concurrency', '?')}) vs "
        f"{claim.get('candidate', {}).get('label', '?')} "
        f"({claim.get('candidate', {}).get('concurrency', '?')}), originally "
        f"{claim.get('claimPct', '?')}% ({claim.get('measuredHow', '')})",
        f"- epoch: `{payload.get('epoch', {}).get('epochId', '?')}` "
        f"({payload.get('config', {}).get('pairs', 0)} pairs, alternating order, "
        f"scope `{payload.get('config', {}).get('scope', '')}`)",
        f"- referee verification: `{referee.get('epochId', '?')}` — "
        f"{referee.get('verdict', 'not recorded in this report')}",
        "",
        "## Verdict",
        "",
        f"**{verdict.get('verdict', '?')}** — kind `{verdict.get('verdictKind', '?')}`",
        "",
        f"- median delta {verdict.get('medianDeltaPct', '?')}% (negative = "
        f"{claim.get('candidate', {}).get('label', 'candidate')} faster), 95% CI "
        f"{verdict.get('ci95', [])}",
        f"- the shipped {claim.get('claimPct', '?')}% lies inside that interval: "
        f"**{verdict.get('claimInsideCi', '?')}**",
        f"- {verdict.get('reason', '')}",
    ]
    if verdict.get("whatWouldSettleIt"):
        lines += ["", f"What would settle it: {verdict['whatWouldSettleIt']}"]
    lines += [
        "",
        "## What the harness can see",
        "",
        f"- reachable: **{reach.get('reachable', '?')}** (run reported `fetchClient="
        f"{reach.get('fetchClient', '?')}`)",
        f"- {reach.get('reason', '')}",
        f"- short-circuit present in `{SHORT_CIRCUIT_FILE}`: "
        f"**{(reach.get('evidence', {}).get('shortCircuit') or {}).get('shortCircuitsCustomFetch', '?')}**",
        "",
        "## Arms",
        "",
        "| arm | label | median report wall | median proc wall | rows |",
        "|---|---|---|---|---|",
    ]
    for key in ("a", "b"):
        arm = arms.get(key) or {}
        lines.append(
            f"| {key} | {arm.get('label', arm.get('name', '?'))} | "
            f"{arm.get('medianReportWallMs', arm.get('medianWallMs', '?'))} ms | "
            f"{_ms_cell(arm.get('medianWallMs'))} | {arm.get('rows', '?')} |"
        )
    parity = ab_report.get("parity") or {}
    divergences = [str(item) for item in (parity.get("divergences") or [])]
    repeated = ab_report.get("repeatedFetches") or {}
    lines += [
        "",
        f"- parity: `{ab_report.get('status', '?')}` against `{parity.get('epochId', '?')}` — "
        f"{'all four units matched the recorded run' if not divergences else '; '.join(divergences[:4])}",
        f"- repeated-fetch exposure: A {repeated.get('a', '?')}% · B {repeated.get('b', '?')}% of "
        "summed source time — the share in sources that asked for the same URL twice (retry "
        "backoff still sleeps in replay, so it sits inside the wall clock this compares)",
        "",
        "## Caveats",
        "",
        "- a **replay** measures the engine, not the wire: it removes network time, so it can "
        "refute a wall-clock claim but never confirm one.",
        "- a confirmation therefore still needs a live paired run before anyone quotes it as a "
        "production win; a refutation does not.",
        "- advisory only, per D7: this does not gate CI.",
    ]
    if payload.get("releaseNoteDraft"):
        lines += [
            "",
            "## Release note draft (D2, only for a refutation)",
            "",
            str(payload["releaseNoteDraft"]),
        ]
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--epoch", default="", help="epoch directory to adjudicate against")
    parser.add_argument("--corpus-parent", default="_out/perf-runs")
    parser.add_argument("--pairs", type=int, default=6)
    parser.add_argument("--min-effect-pct", type=float, default=2.0)
    parser.add_argument("--scope", default="subset")
    parser.add_argument("--sources", type=int, default=4)
    parser.add_argument("--families", default="google_sheets")
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--out", default="")
    parser.add_argument("--label", default="")
    parser.add_argument("--timeout-s", type=int, default=3600)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="reuse complete pairs already under --out instead of re-running them",
    )
    parser.add_argument(
        "--apply-release-note",
        action="store_true",
        help="append the D2 correction to release-notes.md, and only when the verdict is a refutation",
    )
    return parser


def append_release_note(text: str, *, path: Path | None = None) -> Path:
    target = Path(path) if path is not None else _ROOT / "release-notes.md"
    with target.open("a", encoding="utf-8") as handle:
        handle.write("\n" + text)
    return target


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    out_dir = Path(args.out) if args.out else Path("_out/perf-runs") / new_run_token("s0")
    try:
        epoch_dir = harness.resolve_epoch(args.epoch, args.corpus_parent)
        payload = run_adjudication(
            epoch_dir=epoch_dir,
            out_dir=out_dir,
            pairs=int(args.pairs),
            min_effect_pct=float(args.min_effect_pct),
            scope=args.scope,
            sources=int(args.sources),
            families=args.families,
            max_workers=int(args.max_workers),
            timeout_s=int(args.timeout_s),
            resume=bool(args.resume),
        )
    except harness.HarnessError as exc:
        print(f"[s0] {exc}", file=sys.stderr)
        return harness.EXIT_INFRASTRUCTURE
    except ab.RefereeRefused as exc:
        # The study never started: the A/B verifies its referee before the first pair, and its
        # refusal carries the check's own code, so the S0 command reports the same fact.
        print(f"[s0] {exc}", file=sys.stderr)
        return int(exc.exit_code)
    write_json(out_dir / "S0-VERDICT.json", payload)
    markdown = render_markdown(payload)
    (out_dir / "S0-VERDICT.md").write_text(markdown, encoding="utf-8")
    print(markdown)
    ab_status = str((payload.get("ab") or {}).get("status") or "")
    if ab_status == "parity-failed":
        return harness.EXIT_PARITY_FAILED
    if ab_status != "ok":
        return harness.EXIT_INFRASTRUCTURE
    draft = str(payload.get("releaseNoteDraft") or "")
    if draft and args.apply_release_note:
        written = append_release_note(draft)
        print(f"[s0] release note appended to {written}")
    elif draft:
        print(
            "[s0] refuted: pass --apply-release-note to append the D2 correction", file=sys.stderr
        )
    return harness.EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
