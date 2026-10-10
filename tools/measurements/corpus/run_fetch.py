"""Drive one real fetch-lane run through the corpus: ``--mode record|replay`` (S4/S5).

This is the only place that decides *what a corpus epoch is*, so it is also the only place
that can state a run's scope honestly. It calls the real
``src.jobs.pipeline.run_pipeline`` with the real parameter surface — no adapter changes, no
product seam — supplying the ``fetch_text`` callable that the pipeline's own
short-circuit already documents.

What it always does, in both modes:

* ``force_refresh_all=True`` (D5): replay cannot reproduce a run whose fetch set depends on
  the wall clock, and a corpus recorded from a *scheduled* run would be missing every
  skipped source. Both modes therefore cover the same full-fetch set.
* ``browser_fallback_max_workers=0`` and ``BALUFFO_BROWSER_POOL=0``: Playwright is a Node
  driver outside this seam (D9), so it is disabled in both arms and named in the coverage
  report rather than left as a hidden live-egress path.
* The same source selection and worker count in both modes, so the two arms differ only in
  where bytes come from.

Replay additionally arms the socket-level egress guard for the whole run: it finishes only
if nothing reached a socket, which is what makes "zero egress" a demonstrated property
instead of a claim. A run that parity-fails, misses the corpus, or trips the guard exits
non-zero with the reason written into its artifacts.

A record then audits itself for work a bounded phase dropped (``truncation.py``) and for request
keys it was served more than one response for (``conflicts.py``, declared beside the verdict
rather than refusing it), and decides whether it may be replayed against as a **delta referee**. An unrequested redirect candidate, or a
static source the clock stopped, is a bound replay removes rather than a fact the epoch can
substantiate a delta on: the record refuses that certification, names the knob that would unbind it,
and exits non-zero after writing the epoch. ``--accept-truncated-referee`` is the explicit override
-- the epoch is recorded and labelled accepted-but-truncated, so a study pinned to it can see the
bound it ran under.

A record whose audit *certifies* the epoch then tests the certification before the manifest is
final: one fresh replay of the epoch just recorded, through the same check ``replay_check.py``
runs, with the result attached to ``EPOCH.json`` as ``replayVerification`` and the full receipt
under ``<epoch>/referee-verification/``. A birth replay that does not verify is refused the same
way the audit's refusal is: the epoch is written and remains a coverage record, and the record
exits with the check's own code (1 falsified, 2 unjudgeable). A replay that did not reproduce the
record is not automatically a hole: when every differing row belongs to a host the record's own
conflict audit declared as a key conflict, the check answers ``parity-bound`` and the epoch stands,
because replay cannot hold the responses the board rendered for the other callers of that key
(``binding.py``). A refused epoch is also moved out of
the parent's direct children into a sibling ``rejected/`` directory -- whole, with its receipt and a
``quarantine`` block saying why and where -- so no listing that scans the parent holds an epoch its
own record would not certify, and it does not supersede the parent's certified epochs.
``--accept-unverified-referee`` keeps it in place instead, labelled accepted-but-unverified in the
manifest -- a label a study sees and still refuses, because it re-runs the check rather than
trusting the block. ``--no-referee-verification`` skips the check for a fast record, and an epoch
whose certification the audit already refuses is not checked at birth at all: there is no
certification to test.

Usage::

    python tools/measurements/corpus/run_fetch.py --mode record --scope subset \\
        --sources 8 --families google_sheets
    python tools/measurements/corpus/run_fetch.py --mode replay --corpus-dir <epoch-dir>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.shared.json_io import (
    read_json as read_product_json,
)
from tools.measurements.corpus import (  # noqa: E402
    conflicts,
    harness,
    truncation,
)
from tools.measurements.corpus import epoch as epoch_mod  # noqa: E402
from tools.measurements.corpus.capture import (  # noqa: E402
    CaptureRecorder,
    CorpusReplayer,
    EgressBlocked,
    install_egress_guard,
)
from tools.measurements.corpus.harness import (  # noqa: E402
    REPORT_FILE_CANDIDATES,
    ROWS_FILE_CANDIDATES,
)
from tools.measurements.corpus.store import (  # noqa: E402
    CorpusMiss,
    CorpusStore,
    RequestLedger,
)

DEFAULT_CORPUS_PARENT = _ROOT / "_out" / "perf-runs"
DEFAULT_FAMILIES = ("google_sheets",)
DEFAULT_STATIC_SOURCES = 8
STATIC_LOADER_PREFIX = "static_source::"
EXIT_OK = 0
EXIT_PARITY_FAILED = 1
EXIT_INFRASTRUCTURE = 2
# Distinct from the parities: the record itself is complete and the epoch is written. What failed is
# the *certification* -- this corpus cannot substantiate a replay delta until the bound is raised.
# Defined beside the verdict that returns it (``truncation``), because a second copy of the number
# is how the two surfaces start disagreeing.
EXIT_REFEREE_REFUSED = truncation.EXIT_REFEREE_REFUSED
# One hour: the same default the check command and the study gate use, so no caller of the same
# check waits a different length of time for it.
DEFAULT_REFEREE_VERIFICATION_TIMEOUT_S = 3600
# Where a refused epoch goes: a sibling of the epoch directory that no discovery scans, because
# ``epoch_dirs`` reads only a parent's direct children. The move is the mechanism -- nothing has to
# learn a "rejected" flag -- and the epoch stays a readable coverage record with its receipt.
REJECTED_DIRNAME = "rejected"


def new_run_token(mode: str) -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    return f"{mode}-{stamp}"


def write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def read_json(path: Path) -> Any:
    """Read a pipeline artifact through the product's own gzip-aware reader.

    ``jobs-unified.json`` is *gzip-backed* (``src/shared/json_io.py``), so it lands on disk as
    ``jobs-unified.json.gz``. A driver that looked only for the plain name would read no rows
    from a real run and report a confident zero -- which is exactly what this driver did
    before the first real round trip caught it. Delegating keeps the corpus driver honest
    about what the pipeline actually wrote.
    """

    return read_product_json(Path(path), None)


def _loaders() -> list[tuple[str, Any]]:
    from src.jobs_fetcher import default_source_loaders

    return list(default_source_loaders())


def select_loaders(
    all_loaders: Sequence[tuple[str, Any]],
    *,
    only_sources: Sequence[str] = (),
    families: Sequence[str] = DEFAULT_FAMILIES,
    static_sources: int = DEFAULT_STATIC_SOURCES,
    all_sources: bool = False,
) -> list[tuple[str, Any]]:
    """A deterministic, labelled subset: named families plus a stride sample of boards.

    The stride (not "the first N") is deliberate: registry order is roughly alphabetical by
    URL, and the first N would sample one hosting era. The selection is a pure function of
    the registry, so record and replay pick the same set without storing it.

    ``all_sources`` is the other end of that trade: every registered loader in registry
    order, for a reference epoch that has to cover the whole registry rather than a sample
    of it (D6). It supersedes the families and the stride sample, so on the command line
    the two selectors are mutually exclusive instead of silently combined.
    """

    if only_sources:
        wanted = {str(name) for name in only_sources}
        return [pair for pair in all_loaders if pair[0] in wanted]
    if all_sources:
        return list(all_loaders)
    by_name = {name: loader for name, loader in all_loaders}
    chosen: list[tuple[str, Any]] = [(name, by_name[name]) for name in families if name in by_name]
    statics = [pair for pair in all_loaders if str(pair[0]).startswith(STATIC_LOADER_PREFIX)]
    count = max(0, int(static_sources))
    if count and statics:
        stride = max(1, len(statics) // count)
        chosen.extend(statics[::stride][:count])
    return chosen


@contextmanager
def browser_fallback_disabled() -> Any:
    """Playwright is out of corpus scope (D9): disable it in both arms and say so."""

    previous = os.environ.get("BALUFFO_BROWSER_POOL")
    os.environ["BALUFFO_BROWSER_POOL"] = "0"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("BALUFFO_BROWSER_POOL", None)
        else:
            os.environ["BALUFFO_BROWSER_POOL"] = previous


def _first_existing(directory: Path, names: Sequence[str]) -> Path | None:
    """Locate an artifact under either of its storage names (``x.json`` / ``x.json.gz``)."""

    return harness.first_artifact(directory, names)


def job_identity(row: Any) -> str:
    """One string naming one row's job, in the spelling :func:`job_identities` sorts.

    The link when the row has one, and otherwise the company and title it was built from -- the
    fallback the digest has always used, kept here so the birth-replay binding can key a row by
    "the same job" without a second definition of it drifting from the parity units'.
    """

    if not isinstance(row, Mapping):
        return ""
    link = str(row.get("jobLink") or row.get("link") or row.get("url") or "").strip()
    if link:
        return link
    company = str(row.get("company") or "").strip()
    title = str(row.get("title") or "").strip()
    return f"{company}|{title}"


def job_identities(rows: Any) -> list[str]:
    """One string per job, in a stable order. The parity unit for record versus replay."""

    if not isinstance(rows, list):
        return []
    return sorted(job_identity(row) for row in rows if isinstance(row, Mapping))


def identity_digest(identities: Sequence[str]) -> str:
    """Order-independent digest: the parity unit is the *set* of jobs, never their order.

    Sorting here (rather than trusting every caller to) means a replay that emits the same
    jobs in a different order cannot be scored as a parity failure by accident.
    """

    digest = hashlib.sha256("\n".join(sorted(identities)).encode("utf-8")).hexdigest()
    return digest[:32]


# What identifies a *job*, as opposed to when a run happened to observe it. Any two real
# runs differ in ``id``/``fetchedAt``/``firstSeenAt``/``lastSeenAt``, so a comparison that
# included them would fail on a perfectly faithful replay; these fields must not drift.
STABLE_ROW_FIELDS: tuple[str, ...] = (
    "title",
    "company",
    "city",
    "country",
    "workType",
    "contractType",
    "jobLink",
    "sector",
    "profession",
    "source",
    "sourceJobId",
    "status",
    "postedAt",
)


def row_projection(rows: Any, fields: Sequence[str] = STABLE_ROW_FIELDS) -> list[dict[str, Any]]:
    """The run-independent view of each row, in a stable order.

    This is the third parity unit: the identity digest says *which* jobs came back and the
    row count says how many, while this says the fields a consumer reads did not drift.
    """

    projected: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, Mapping):
            continue
        projected.append({str(name): row.get(name, "") for name in fields})
    return sorted(projected, key=lambda item: json.dumps(item, sort_keys=True))


def run_summary(output_dir: Path) -> dict[str, Any]:
    """What a replay is checked against: row count, identity set, per-source statuses."""

    rows_path = _first_existing(output_dir, ROWS_FILE_CANDIDATES)
    rows = read_json(rows_path) if rows_path is not None else None
    identities = job_identities(rows)
    report_path = _first_existing(output_dir, REPORT_FILE_CANDIDATES)
    report = read_json(report_path) if report_path is not None else None
    status_counts: dict[str, int] = {}
    for source_row in (report or {}).get("sources") or []:
        if not isinstance(source_row, Mapping):
            continue
        status = str(source_row.get("status") or "unknown")
        status_counts[status] = int(status_counts.get(status, 0)) + 1
    return {
        "rowsFile": rows_path.name if rows_path is not None else "",
        "reportFile": report_path.name if report_path is not None else "",
        "rowCount": len(identities),
        "identityDigest": identity_digest(identities),
        "rowProjectionDigest": identity_digest(
            [json.dumps(item, sort_keys=True) for item in row_projection(rows)]
        ),
        "sourceStatusCounts": dict(sorted(status_counts.items())),
    }


def parity_matches(recorded: Mapping[str, Any], replayed: Mapping[str, Any]) -> dict[str, bool]:
    """The three parity units an S5 acceptance run is judged on.

    Row count alone would accept a replay that produced the same number of *different*
    jobs, so the identity set is compared too. The reverse is also true: an identity set can
    match while a row count drifts. The projection digest is the third: the jobs can be
    identical while a field a consumer reads has drifted, which neither of the other two
    would notice. Each unit catches a defect the others cannot, so all three are reported
    separately rather than folded into one yes/no.

    A unit whose value is absent from either side counts as a mismatch: a corpus recorded
    before a unit existed cannot substantiate parity for it, and a silent pass would be the
    over-claim this comparison exists to prevent.
    """

    return {
        "rowCount": int(replayed.get("rowCount") or 0) == int(recorded.get("rowCount") or 0),
        "identityDigest": str(replayed.get("identityDigest") or "")
        == str(recorded.get("identityDigest") or ""),
        "rowProjectionDigest": str(replayed.get("rowProjectionDigest") or "")
        == str(recorded.get("rowProjectionDigest") or "")
        and bool(str(replayed.get("rowProjectionDigest") or "")),
        "sourceStatusCounts": dict(replayed.get("sourceStatusCounts") or {})
        == dict(recorded.get("sourceStatusCounts") or {})
        and bool(replayed.get("sourceStatusCounts")),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--mode", required=True, choices=("record", "replay"))
    parser.add_argument("--scope", default="subset", help="epoch scope label")
    parser.add_argument("--label", default="", help="free-form note stored with the run")
    parser.add_argument(
        "--corpus-dir",
        default="",
        help="record: corpus directory to write; replay: epoch directory to read",
    )
    parser.add_argument("--corpus-parent", default=str(DEFAULT_CORPUS_PARENT))
    parser.add_argument("--run-dir", default="", help="where the pipeline artifacts go")
    parser.add_argument("--sources", type=int, default=DEFAULT_STATIC_SOURCES)
    parser.add_argument("--families", default=",".join(DEFAULT_FAMILIES))
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--only-sources", default="")
    selection.add_argument(
        "--all-sources",
        action="store_true",
        help=(
            "select every registered loader in registry order instead of a stride sample "
            "(a reference epoch; mutually exclusive with --only-sources)"
        ),
    )
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument(
        "--adapter-http-concurrency",
        type=int,
        default=0,
        help=(
            "pass through to the pipeline's httpx pool sizing; 0 leaves the pipeline default. "
            "This is the variable an S0 pool A/B varies."
        ),
    )
    parser.add_argument(
        "--google-sheets-redirect-concurrency",
        type=int,
        default=0,
        help=(
            "sheet redirect-resolution pool; 0 leaves the pipeline default (16). Raise it in "
            "record and replay alike: the resolver's host breaker stops requesting a failing "
            "host after ~2x this many response-less failures, so a pool this small records only "
            "a prefix of a failing host's candidates and the replay then misses them."
        ),
    )
    parser.add_argument(
        "--accept-truncated-referee",
        action="store_true",
        help=(
            "record mode only: certify the epoch as a delta referee even though the truncation "
            "audit says a bounded phase dropped work replay will walk into. The default is to "
            "refuse (exit 3) after writing the epoch, and to name the knob that would unbind the "
            "bound; this records the epoch anyway, marked accepted-but-truncated in EPOCH.json."
        ),
    )
    parser.add_argument(
        "--accept-unverified-referee",
        action="store_true",
        help=(
            "record mode only: keep an epoch whose birth replay did not verify (falsified or "
            "unjudgeable). The default is to refuse (exit 1 falsified, 2 unjudgeable), move the "
            "epoch to a sibling rejected/ directory, and supersede nothing; this keeps it in "
            "place instead, marked accepted-but-unverified in EPOCH.json. A study re-runs the "
            "check before pinning, so this labels the hole rather than opening the gate."
        ),
    )
    parser.add_argument(
        "--no-referee-verification",
        action="store_true",
        help=(
            "record mode only: skip the birth replay check that is otherwise attached to "
            "EPOCH.json, with its receipt under referee-verification/. A study re-runs the "
            "check before pinning either way, so this only omits the birth receipt."
        ),
    )
    parser.add_argument(
        "--no-surfaces",
        default="",
        help="comma-separated surfaces to leave unpatched (fetch_text,urlopen,httpx)",
    )
    return parser


def _selected_surfaces(raw: str) -> tuple[str, ...]:
    from tools.measurements.corpus.capture import ALL_SURFACES

    disabled = {item.strip() for item in str(raw or "").split(",") if item.strip()}
    return tuple(surface for surface in ALL_SURFACES if surface not in disabled)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = (
        Path(args.run_dir) if args.run_dir else DEFAULT_CORPUS_PARENT / new_run_token(args.mode)
    )
    output_dir = run_dir / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    ledger = RequestLedger()
    surfaces = _selected_surfaces(args.no_surfaces)
    families = tuple(item.strip() for item in str(args.families).split(",") if item.strip())
    only_sources = tuple(item.strip() for item in str(args.only_sources).split(",") if item.strip())
    loaders = select_loaders(
        _loaders(),
        only_sources=only_sources,
        families=families,
        static_sources=args.sources,
        all_sources=bool(args.all_sources),
    )
    if not loaders:
        print("[corpus] no loaders selected; nothing to run", file=sys.stderr)
        return EXIT_INFRASTRUCTURE
    git_sha, dirty_hash = epoch_mod.git_identity(_ROOT)

    if args.mode == "record":
        return _record(args, run_dir, output_dir, loaders, ledger, surfaces, git_sha, dirty_hash)
    return _replay(args, run_dir, output_dir, loaders, ledger, surfaces)


def guard_blocked_source_names(report: Mapping[str, Any]) -> list[str]:
    """Sources whose replay error came from the guard, i.e. that left the captured lane."""

    names: list[str] = []
    for row in (report or {}).get("sources") or []:
        if not isinstance(row, Mapping):
            continue
        if "egress blocked during replay" in str(row.get("error") or ""):
            names.append(str(row.get("name") or ""))
    return sorted(name for name in names if name)


def run_config(args: argparse.Namespace, surfaces: Sequence[str]) -> dict[str, Any]:
    """What actually varied in this run. An A/B arm is only readable if it states its variable."""

    return {
        "mode": str(args.mode),
        "scope": str(args.scope),
        "families": [item.strip() for item in str(args.families).split(",") if item.strip()],
        "onlySources": [item.strip() for item in str(args.only_sources).split(",") if item.strip()],
        "allSources": bool(getattr(args, "all_sources", False)),
        "staticSources": int(args.sources),
        "maxWorkers": int(args.max_workers),
        "adapterHttpConcurrency": int(args.adapter_http_concurrency or 0),
        # Sheet redirect resolution is bounded by a host-level breaker, not by the wall clock
        # alone (see docs/snapshots/fetch-pipeline-full-source-epoch-2026-10-09.md). Record and
        # replay must use the same pool or the replay asks for links the record short-circuited.
        "googleSheetsRedirectConcurrency": int(args.google_sheets_redirect_concurrency or 0),
        "surfaces": list(surfaces),
        "forceRefreshAll": True,
        "browserFallback": "disabled (D9)",
        # The static per-source time budget is a wall clock, and replay removes wall clock: a
        # source truncated at 90 s live keeps exploring in replay and lands on a corpus miss
        # (observed on the first full-source epoch). So the value in force is recorded here --
        # record and replay must run with the same one. Empty means the product default (90 s).
        "staticSourceTimeBudgetS": str(os.environ.get("BALUFFO_STATIC_SOURCE_TIME_BUDGET_S", "")),
        "label": str(args.label or ""),
    }


def _run_pipeline(
    *,
    output_dir: Path,
    loaders: Sequence[tuple[str, Any]],
    fetch_text: Any,
    max_workers: int,
    run_id: str,
    adapter_http_concurrency: int = 0,
    google_sheets_redirect_concurrency: int = 0,
) -> dict[str, Any]:
    from src.jobs.pipeline import run_pipeline

    extra: dict[str, Any] = {}
    if int(adapter_http_concurrency or 0) > 0:
        extra["adapter_http_concurrency"] = int(adapter_http_concurrency)
    if int(google_sheets_redirect_concurrency or 0) > 0:
        extra["google_sheets_redirect_concurrency"] = int(google_sheets_redirect_concurrency)
    return run_pipeline(
        output_dir=output_dir,
        run_id=run_id,
        source_loaders=list(loaders),
        fetch_text=fetch_text,
        force_refresh_all=True,
        browser_fallback_max_workers=0,
        max_workers=max(1, int(max_workers)),
        show_progress=False,
        **extra,
    )


def run_rows(output_dir: Path) -> list[dict[str, Any]]:
    """The rows this run kept, as the record-time truncation audit reads them."""

    return harness.run_rows(output_dir)


def _truncation_audit(
    *,
    output_dir: Path,
    report: Mapping[str, Any],
    store: CorpusStore,
    scope: str,
) -> dict[str, Any]:
    """Audit a finished record for work a bounded phase dropped, before it is called done.

    The effective redirect pool comes from the run's own report (``runtime``), not from the
    flag: the flag's 0 means "product default", and the number that bound the phase is the
    resolved one.

    ``sources`` is the report's own per-source accounting: the static family reads it for a
    source clock or a detail cap that dropped page and detail work. It is the run's declaration;
    the corpus is what verifies which of the refused URLs it really holds no request for.
    """

    runtime = report.get("runtime") if isinstance(report.get("runtime"), Mapping) else {}
    return truncation.audit(
        rows=run_rows(output_dir),
        requested_urls=store.iter_requested_urls(),
        pool=int((runtime or {}).get("googleSheetsRedirectConcurrency") or 0),
        scope=scope,
        sources=report.get("sources") or [],
    )


def _verify_referee_at_birth(
    *,
    corpus_dir: Path,
    code: Mapping[str, str],
    accepted: bool = False,
) -> dict[str, Any]:
    """One fresh replay of the epoch just recorded, as the block its manifest carries.

    The check is imported here rather than at module level because ``replay_check`` imports this
    module. The block is attached either way; whether a non-verifying one is *kept* is the
    caller's decision, folded into ``accepted`` so EPOCH.json states it. A study re-runs the
    check before pinning rather than trusting the block, so acceptance labels a hole -- it does
    not certify the epoch the hole is in. The refused case prints nothing here: the record owns
    that line, because it names where the epoch went.
    """

    from tools.measurements.corpus import replay_check as check_mod

    out_dir = corpus_dir / check_mod.VERIFICATION_DIRNAME
    try:
        row = check_mod.epoch_row_for(corpus_dir)
        result = check_mod.check_epoch(
            row,
            out_dir=out_dir,
            samples=1,
            timeout_s=DEFAULT_REFEREE_VERIFICATION_TIMEOUT_S,
            label=f"birth verification {corpus_dir.name}",
        )
        receipt = check_mod.verification_receipt(row=row, result=result, out_dir=out_dir, code=code)
    except harness.HarnessError as exc:
        # Only reachable when no listing row can be built, which is a harness failure rather than a
        # finding about the corpus: attach it as such instead of aborting a finished record. The
        # exit code is derived from that verdict like every other, so the code the record returns
        # and the code its receipt carries cannot disagree.
        verdict = check_mod.VERDICT_HARNESS
        receipt = {
            "verdict": verdict,
            "exitCode": check_mod.verdict_exit_code(check_mod.summarize([{"verdict": verdict}])),
            "problems": [{"kind": verdict, "detail": str(exc)}],
            "code": dict(code),
        }
    attachment = check_mod.manifest_attachment(receipt)
    verdict = str(attachment.get("verdict") or "unknown")
    attachment["accepted"] = bool(accepted) and verdict != check_mod.VERDICT_VERIFIED
    if verdict == check_mod.VERDICT_VERIFIED:
        print(
            f"[corpus] replay verification: verified -- the epoch replayed end to end and "
            f"reproduced its record; receipt {attachment.get('receipt')}"
        )
    elif check_mod.accepted(verdict):
        bound = attachment.get("binding") or {}
        print(
            "[corpus] replay verification: parity-bound -- the epoch replayed end to end and did "
            "not reproduce its record; every differing row belongs to a host the record declared "
            f"as a key conflict ({bound.get('residueRows') or 0} row(s) on "
            f"{', '.join(bound.get('residueHosts') or []) or 'no named host'}), so the epoch "
            f"stands; receipt {attachment.get('receipt')}"
        )
    elif attachment["accepted"]:
        print(
            "[corpus] accepted anyway by --accept-unverified-referee: EPOCH.json records this "
            "epoch as accepted-but-unverified, so a study pinned to it can see the hole; a study "
            "re-running the check will still refuse it",
            file=sys.stderr,
        )
    return attachment


def _path_rewrites(old: Path, new: Path) -> list[tuple[str, str]]:
    """The substitutions that move every spelling of ``old`` in a file's text onto ``new``.

    Both separator spellings and the JSON-escaped form of each: a Windows path is written with
    doubled backslashes inside a JSON document, so a bare ``str(old)`` never appears in the text
    of the file that names it. Longest first, so an escaped form is never half-replaced.
    """

    pairs: dict[str, str] = {}
    for before, after in ((str(old), str(new)), (old.as_posix(), new.as_posix())):
        if not before or before == after:
            continue
        pairs[json.dumps(before)[1:-1]] = json.dumps(after)[1:-1]
        pairs[before] = after
    return sorted(pairs.items(), key=lambda pair: -len(pair[0]))


def _retarget_text(text: str, pairs: Sequence[tuple[str, str]]) -> str:
    for before, after in pairs:
        text = text.replace(before, after)
    return text


def _retarget_paths(value: Any, *, pairs: Sequence[tuple[str, str]]) -> Any:
    """Rewrite every path in a loaded document so it names where the evidence moved to.

    A walk rather than a list of field names: the receipt and the manifest's birth block carry
    more than one path each (``epochDir``, ``reportDir``, ``receipt``, and one ``sampleRunDirs``
    entry per sample run), and a hand-maintained list is how one of them gets left pointing at a
    directory that no longer exists. Only text matching the moved directory is touched, so the
    run directory beside the epoch -- which does not move -- is left exactly as recorded.
    """

    if isinstance(value, str):
        return _retarget_text(value, pairs)
    if isinstance(value, Mapping):
        return {key: _retarget_paths(item, pairs=pairs) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_retarget_paths(item, pairs=pairs) for item in value]
    return value


def _quarantine_epoch(
    corpus_dir: Path,
    *,
    manifest: dict[str, Any],
    birth: dict[str, Any],
) -> Path:
    """Move an epoch its own birth replay did not verify out of the parent's candidates.

    ``epoch_dirs`` -- and so ``resolve_current``, the referee listing and every study's pick --
    reads only a parent's *direct* children, so a sibling ``rejected/`` takes the epoch out of
    every query without any of them learning a flag. The epoch is not discarded: it moves whole,
    a coverage record with its receipt, and every path it carries that named the old location is
    rewritten to where the evidence now lives -- otherwise the receipt a reader follows to the
    sample runs points at a directory the move removed. Called *instead of* supersession: a
    rejected record must not retire the parent's last certified epoch. Returns the directory the
    epoch now lives in.
    """

    from tools.measurements.corpus import replay_check as check_mod

    target = corpus_dir.parent / REJECTED_DIRNAME / corpus_dir.name
    suffix = 1
    while target.exists():
        suffix += 1
        target = corpus_dir.parent / REJECTED_DIRNAME / f"{corpus_dir.name}-{suffix}"
    target.parent.mkdir(parents=True, exist_ok=True)
    corpus_dir.rename(target)
    pairs = _path_rewrites(corpus_dir, target)
    # Every JSON document the epoch moved with is swept, the receipt included: the audit reports
    # and the sample run's own artifacts name the epoch directory too, and a pointer into a
    # directory the move removed is worse than no pointer at all. The sweep is textual, so a
    # finding inside one of these files cannot be changed by it -- only a path. ``EPOCH.json`` is
    # skipped because the manifest is written from memory below, where the block that records the
    # move must keep the *old* path it exists to report.
    for path in sorted(target.rglob("*.json")):
        if path.name == epoch_mod.EPOCH_FILE:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        retargeted = _retarget_text(text, pairs)
        if retargeted != text:
            path.write_text(retargeted, encoding="utf-8")
    receipt_path = target / check_mod.VERIFICATION_DIRNAME / check_mod.RECEIPT_NAME
    if receipt_path.is_file():
        # Only a receipt that exists has a path to correct: the harness-error branch never wrote
        # one, and an empty string says that honestly where a path to nothing would not.
        birth["receipt"] = str(receipt_path)
    # The block is the manifest's own copy of the receipt and names the same places, sample runs
    # included, so it is retargeted by the same walk rather than by a second list of keys.
    birth.update(_retarget_paths(dict(birth), pairs=pairs))
    manifest["quarantine"] = {
        "reason": "birth-replay-not-verified",
        "verdict": str(birth.get("verdict") or ""),
        "exitCode": int(birth.get("exitCode") or 0),
        "originalDir": str(corpus_dir),
        "movedTo": str(target),
        "movedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    epoch_mod.write_epoch(target, manifest)
    return target


def _record(
    args: argparse.Namespace,
    run_dir: Path,
    output_dir: Path,
    loaders: Sequence[tuple[str, Any]],
    ledger: RequestLedger,
    surfaces: Sequence[str],
    git_sha: str,
    dirty_hash: str,
) -> int:
    corpus_dir = (
        Path(args.corpus_dir)
        if args.corpus_dir
        else epoch_mod.epoch_dir(Path(args.corpus_parent), args.scope)
    )
    store = CorpusStore(corpus_dir)
    run_id = run_dir.name
    with CaptureRecorder(store, scope=args.scope, ledger=ledger, surfaces=surfaces) as recorder:
        with browser_fallback_disabled():
            report = _run_pipeline(
                output_dir=output_dir,
                loaders=loaders,
                fetch_text=recorder.fetch_text,
                max_workers=args.max_workers,
                run_id=run_id,
                adapter_http_concurrency=args.adapter_http_concurrency,
                google_sheets_redirect_concurrency=args.google_sheets_redirect_concurrency,
            )
    summary = run_summary(output_dir)
    truncation_report = _truncation_audit(
        output_dir=output_dir,
        report=report,
        store=store,
        scope=args.scope,
    )
    truncation_path = truncation.write_report(
        truncation_report, corpus_dir / truncation.REPORT_NAME
    )
    conflict_report = conflicts.audit(corpus_dir=corpus_dir, scope=args.scope)
    conflict_path = conflicts.write_report(conflict_report, corpus_dir / conflicts.REPORT_NAME)
    coverage_path, size_path = store.write_reports(
        ledger=ledger,
        scope=args.scope,
        mode="record",
        extra={
            **recorder.coverage_extra(),
            "label": args.label,
            "runDir": str(run_dir),
            "runConfig": run_config(args, surfaces),
            "rowCount": summary["rowCount"],
            "identityDigest": summary["identityDigest"],
            "pipelineError": str(report.get("error") or ""),
        },
    )
    # Whether this record may be replayed against as a delta referee is a decision about the
    # record, so it is made here, recorded in the manifest, and enforced by the exit code -- a
    # verdict that lives only in a warning is one an unattended script walks straight past.
    accepted = bool(args.accept_truncated_referee)
    # The truncation verdict decides; the conflict audit declares. A key the run answered with
    # more than one response cannot be replayed from one stored entry, but the differing
    # responses are what the board served rather than a bound the run could raise, so the class
    # travels as a note -- the material a study gate binds a divergence by, not a reason to
    # refuse the record.
    verdict = conflicts.carry(
        truncation.referee_verdict(truncation_report, accepted=accepted), conflict_report
    )
    counts = store.counts()
    manifest = epoch_mod.create_epoch(
        corpus_dir,
        scope=args.scope,
        recorded_with="force_refresh_all",
        counts=counts,
        coverage_summary={
            "requestsOffered": ledger.offered,
            "requestsRecorded": ledger.recorded,
            "unattributedCount": ledger.as_report()["unattributedCount"],
            "sourceCount": len(loaders),
        },
        git_sha=git_sha,
        dirty_diff_hash=dirty_hash,
        notes=args.label,
        extra={
            "recordedSummary": summary,
            "recordedRunDir": str(run_dir),
            "recordedSurfaces": list(surfaces),
            "recordedConfig": run_config(args, surfaces),
            "browserFallback": "disabled (D9)",
            # What the record may not have: a bounded phase that dropped candidates leaves no
            # request behind, so only a record-time join can see it.
            "truncation": truncation.summary(truncation_report),
            # And what a replay cannot reconstruct from one stored response per key: the bodies
            # the run's own re-fetches orphaned, split per host by the store's replacement log.
            "conflicts": conflicts.summary(conflict_report),
            # And what the record may not be, without being asked: a bound replay removes rather
            # than a fact a delta can rest on. `accepted` travels with the epoch so a study can
            # tell a deliberate exception from a clean record.
            "deltaReferee": verdict,
        },
    )
    # The certification is tested before the manifest is final, so an operator reading EPOCH.json
    # sees whether the claim held on the day it was made -- while a study still re-runs the check
    # rather than trusting the block. A non-verifying check refuses the record unless asked to
    # keep it: the exit code is read off the attached block, so it cannot disagree with the
    # receipt on disk.
    # Imported at the birth decision rather than at module level for the usual reason:
    # replay_check imports this module.
    from tools.measurements.corpus import replay_check as check_mod

    birth: dict[str, Any] | None = None
    if verdict["eligible"] and not bool(args.no_referee_verification):
        birth = _verify_referee_at_birth(
            corpus_dir=corpus_dir,
            code={"gitSha": git_sha, "dirtyDiffHash": dirty_hash},
            accepted=bool(args.accept_unverified_referee),
        )
        manifest["replayVerification"] = birth
        epoch_mod.write_epoch(corpus_dir, manifest)
    # A record its own birth replay did not certify is moved out of the parent's direct children
    # and supersedes nothing: the last certified epoch must not be retired for an unusable one.
    rejected_to: Path | None = None
    if birth is not None and not check_mod.certification_held(birth):
        rejected_to = _quarantine_epoch(corpus_dir, manifest=manifest, birth=birth)
    # And neither does a record the audit already refused. Supersession is a claim about the *current*
    # reference, so only a record that may itself referee a delta may retire one that could. The
    # second 2026-10-09 epoch -- refused for 252 unrequested candidates, every one of them an audit
    # defect that was later fixed -- retired the certified 2026-10-09 reference, leaving the listing
    # with only epochs no study could be measured on. That was an inconsistency in the gate rather
    # than a judgement about the epochs: a failed *birth replay* already quarantines instead of
    # superseding, while a failed *audit* -- decided earlier, on the same artifact -- superseded
    # unconditionally. Eligibility is the rule; `--accept-truncated-referee` is the explicit override
    # and travels in the manifest, so an accepted-but-truncated record supersedes and says so.
    referee_usable = bool(verdict["eligible"]) or bool(verdict["accepted"])
    superseded = (
        []
        if rejected_to is not None or not referee_usable
        else epoch_mod.supersede_older_epochs(corpus_dir.parent, current=corpus_dir.name)
    )
    print(
        f"[corpus] recorded {manifest['epochId']}: {counts['entries']} entries, "
        f"{counts['uniqueBodies']} bodies, {counts['compressedBodyBytes']} compressed bytes"
    )
    print(f"[corpus] rows={summary['rowCount']} identityDigest={summary['identityDigest']}")
    print(f"[corpus] coverage {coverage_path}")
    print(f"[corpus] size     {size_path}")
    print(f"[corpus] truncation {truncation_path}")
    print(f"[corpus] conflicts {conflict_path}")
    for line in truncation.warning_lines(truncation_report):
        print(line, file=sys.stderr)
    for line in conflicts.warning_lines(conflict_report):
        print(line, file=sys.stderr)
    for line in truncation.referee_lines(truncation_report, accepted=accepted):
        print(line, file=sys.stderr)
    if superseded:
        print(f"[corpus] superseded {', '.join(superseded)}")
    elif rejected_to is None and not referee_usable:
        print(
            "[corpus] superseding nothing: this record may not referee a delta, so it cannot "
            "retire an epoch that can -- the parent's certified epochs are unchanged",
            file=sys.stderr,
        )
    if not verdict["eligible"] and not verdict["accepted"]:
        return EXIT_REFEREE_REFUSED
    if rejected_to is not None and birth is not None:
        kinds = ", ".join(str(item) for item in birth.get("problemKinds") or []) or "unknown"
        print(
            f"[corpus] refusing the record: the birth replay did not verify "
            f"({birth.get('verdict')} ({kinds})) -- the epoch is written and was moved to "
            f"{rejected_to}; it remains a coverage record and the parent's certified epochs are "
            "untouched. Re-record it after fixing the hole, or pass "
            "--accept-unverified-referee to keep it in place as accepted-but-unverified; receipt "
            f"{birth.get('receipt')}",
            file=sys.stderr,
        )
        return int(birth.get("exitCode") or 0) or EXIT_INFRASTRUCTURE
    return EXIT_OK


def _replay(
    args: argparse.Namespace,
    run_dir: Path,
    output_dir: Path,
    loaders: Sequence[tuple[str, Any]],
    ledger: RequestLedger,
    surfaces: Sequence[str],
) -> int:
    corpus_dir = (
        Path(args.corpus_dir)
        if args.corpus_dir
        else epoch_mod.resolve_current(Path(args.corpus_parent))
    )
    if corpus_dir is None:
        print("[corpus] no epoch found to replay", file=sys.stderr)
        return EXIT_INFRASTRUCTURE
    manifest = epoch_mod.read_epoch(corpus_dir)
    store = CorpusStore(corpus_dir)
    recorded = dict(manifest.get("recordedSummary") or {})
    parity: dict[str, Any] = {
        "epochId": manifest.get("epochId") or corpus_dir.name,
        "epochScope": manifest.get("scope") or "",
        "mode": "replay",
        "label": args.label,
        "runDir": str(run_dir),
        "corpusDir": str(corpus_dir),
        "runConfig": run_config(args, surfaces),
        "recorded": recorded,
        "replayed": {},
        "guard": {},
        "misses": [],
        "surfaces": list(surfaces),
        "status": "unknown",
    }
    guard = install_egress_guard()
    try:
        with guard:
            with CorpusReplayer(
                store, scope=str(manifest.get("scope") or ""), ledger=ledger, surfaces=surfaces
            ) as replayer:
                with browser_fallback_disabled():
                    report = _run_pipeline(
                        output_dir=output_dir,
                        loaders=loaders,
                        fetch_text=replayer.fetch_text,
                        max_workers=args.max_workers,
                        run_id=run_dir.name,
                        adapter_http_concurrency=args.adapter_http_concurrency,
                        google_sheets_redirect_concurrency=(
                            args.google_sheets_redirect_concurrency
                        ),
                    )
    except EgressBlocked as exc:
        parity["guard"] = guard.as_report()
        parity["status"] = "egress-blocked"
        parity["error"] = str(exc)
        write_json(run_dir / "replay-parity.json", parity)
        print(f"[corpus] replay tripped the egress guard: {exc}", file=sys.stderr)
        return EXIT_INFRASTRUCTURE
    except CorpusMiss as exc:
        parity["guard"] = guard.as_report()
        parity["status"] = "corpus-miss"
        parity["error"] = str(exc)
        parity["misses"] = ledger.as_report()["misses"]
        write_json(run_dir / "replay-parity.json", parity)
        print(f"[corpus] replay hit a corpus miss: {exc}", file=sys.stderr)
        return EXIT_INFRASTRUCTURE

    summary = run_summary(output_dir)
    parity["replayed"] = summary
    parity["guard"] = guard.as_report()
    parity["ledger"] = ledger.as_report()
    parity["misses"] = ledger.as_report()["misses"]
    parity["pipelineError"] = str(report.get("error") or "")
    # A source that died on the guard is not a corpus failure and not a live fetch either: it
    # is a source the corpus never covered (the static listing lane's async client, observed on
    # a real run). Row parity can still hold while these sources fail, so they are named here
    # rather than left for a reader to infer from a status histogram.
    guard_blocked_sources = guard_blocked_source_names(report)
    parity["guardBlockedSources"] = guard_blocked_sources
    parity["scopeNote"] = (
        "parity covers the intercepted lanes; "
        f"{len(guard_blocked_sources)} source(s) left the corpus uncovered and failed on the guard"
        if guard_blocked_sources
        else "parity covers the intercepted lanes; no source tripped the guard"
    )
    matches = parity_matches(recorded, summary)
    parity["matches"] = matches
    parity["status"] = "parity-ok" if all(matches.values()) else "parity-failed"
    write_json(run_dir / "replay-parity.json", parity)

    print(
        f"[corpus] replayed {parity['epochId']}: rows={summary.get('rowCount')} "
        f"recorded={recorded.get('rowCount')}"
    )
    if guard_blocked_sources:
        print(
            f"[corpus] not covered by the corpus, failed on the guard: "
            f"{', '.join(guard_blocked_sources)}"
        )
    print(
        f"[corpus] egress guard: blocked={guard.blocked} "
        f"(remote={guard.remote_blocked}, loopback={guard.loopback_blocked}; "
        "remote=0 is the egress bar, loopback is in-process plumbing such as asyncio's "
        "self-pipe on Windows)"
    )
    if parity["status"] != "parity-ok":
        print(
            "[corpus] parity failed; check cache-decision differences first -- a corpus "
            "recorded without force_refresh_all shows up here",
            file=sys.stderr,
        )
        return EXIT_PARITY_FAILED
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
