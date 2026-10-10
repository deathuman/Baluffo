"""Which recorded epochs can referee a delta, and what would certify the rest.

A study that measures a delta over replay pins itself to one recorded corpus -- the *referee* --
and `run_fetch.py` decides, at record time, whether an epoch may be one (`truncation.py`). That
decision is written into `EPOCH.json`'s `deltaReferee` block, which is fine for the epoch recorded
after the check existed and useless for the ones recorded before it: the question "which epoch do I
pin?" was answered by opening manifests by hand and knowing the block even exists. This is that
question as a query.

Where a verdict comes from, stated per row
-----------------------------------------

The three sources are not interchangeable, so each row says which one it used:

``recorded``
    the epoch's own ``deltaReferee`` block -- what that record decided under the code of its day,
    override included.
``report``
    the epoch's own ``truncation-report.json``, re-derived through the same verdict function. The
    audit ran; the record predates the manifest block.
``recomputed``
    the audit re-run over the epoch's artifacts now (``truncation.audit_epoch``). This is what
    makes the older epochs answerable at all, and it is labelled because it is a claim about today's
    code rather than a recorded fact: where the two disagree, the recorded block is the epoch's own
    statement.

An epoch with no verdict from any source -- or one whose manifest cannot be read -- is listed as
**not judgeable** rather than dropped or reported as clean. A corpus nobody can judge is not a
referee, and a listing that left it out would make it look like a corpus that was never there.

What the pick means
-------------------

The pick is the newest eligible epoch by ``recordedAt``, preferring one supersession has not
retired. Supersession does not make an old epoch unreplayable (studies pinned to the subset epoch
cite it by path and still run), so a superseded epoch remains a fallback -- it is just not the one
to choose without being told. ``--require-eligible`` turns the query into the study's gate: it
exits ``truncation.EXIT_REFEREE_REFUSED``, the same code a record returns when it refuses its own
certification, so a script cannot read a listing that has no referee as a green light.

Usage::

    python tools/measurements/corpus/referees.py
    python tools/measurements/corpus/referees.py --require-eligible --json _out/referees.json
    python tools/measurements/corpus/referees.py --no-recompute <epoch-dir> ...
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.corpus import epoch as epoch_mod  # noqa: E402
from tools.measurements.corpus import truncation  # noqa: E402

DEFAULT_CORPUS_PARENT = _ROOT / "_out" / "perf-runs"
EXIT_OK = 0
EXIT_INFRASTRUCTURE = 2
# The study gate, and deliberately the record's own refusal code rather than a new one: "this epoch
# cannot referee a delta" is one fact, and two numbers for it would let a script tell them apart.
EXIT_NO_REFEREE = truncation.EXIT_REFEREE_REFUSED

VERDICT_RECORDED = "recorded"
VERDICT_REPORT = "report"
VERDICT_RECOMPUTED = "recomputed"
VERDICT_UNAVAILABLE = "unavailable"

# How many blocker subjects a table cell names before it counts the rest.
_NAMED_SUBJECTS = 2

SOURCE_TEXT = {
    VERDICT_RECORDED: "EPOCH.json",
    VERDICT_REPORT: "stored report",
    VERDICT_RECOMPUTED: "recomputed now",
    VERDICT_UNAVAILABLE: "not judgeable",
}
NOT_PRESENT = "-"


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _when(value: Any) -> str:
    """``recordedAt`` at second precision: a listing compares epochs, it does not time them."""

    text = str(value or "").strip()
    if not text:
        return NOT_PRESENT
    return text[:19].replace("T", " ") + "Z" if len(text) > 19 else text


def _counts(manifest: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(manifest.get("counts"))
    return {"entries": int(raw.get("entries") or 0), "bodies": int(raw.get("uniqueBodies") or 0)}


def _blocked_text(blocker: Mapping[str, Any]) -> str:
    """One cell for one blocker: its reason, the counts it named, and the subjects it named.

    Deliberately reason-agnostic -- the full sentences live in the blocker's own ``detail`` and in
    the JSON copy of this listing, so a table cell only has to be truthful and short.
    """

    text = str(blocker.get("reason") or "blocked")
    amounts = [
        f"{key}={blocker[key]}"
        for key in ("unrequestedCandidates", "unrequestedUrls")
        if blocker.get(key) is not None
    ]
    if amounts:
        text += f" ({', '.join(amounts)})"
    subjects = [str(item) for item in blocker.get("hosts") or blocker.get("sources") or []]
    # Named, not exhaustive: a clock-stopped family can hold many sources, and the full list is in
    # the JSON copy and in the blocker's own ``sources``. The cell stays readable; the artifact does
    # not lose anything.
    if subjects:
        text += f" on {', '.join(subjects[:_NAMED_SUBJECTS])}"
        if len(subjects) > _NAMED_SUBJECTS:
            text += f" (+{len(subjects) - _NAMED_SUBJECTS} more)"
    return text


def _note_text(note: Mapping[str, Any]) -> str:
    reason = str(note.get("reason") or "note")
    amount = note.get("declaredDroppedDetails")
    replaced = note.get("replacedKeys")
    orphans = note.get("orphanedBodies")
    if amount is not None:
        text = f"{reason}: {amount} dropped"
    elif replaced is not None or orphans is not None:
        # A conflict note counts both, because either can be the only number there is: an epoch
        # recorded before the replacement log existed has orphans and no key count.
        text = f"{reason}: {int(replaced or 0)} key(s) replaced, {int(orphans or 0)} orphaned body(ies)"
    else:
        text = reason
    knobs = [str(knob) for knob in note.get("knobs") or []]
    return text + (f" (knob: {', '.join(knobs)})" if knobs else " (no knob)")


def _row(
    *,
    epoch_dir: Path,
    manifest: Mapping[str, Any],
    verdict: Mapping[str, Any] | None,
    verdict_source: str,
    reason: str = "",
) -> dict[str, Any]:
    blocks = [_as_mapping(item) for item in (verdict or {}).get("blocking") or []]
    notes = [_as_mapping(item) for item in (verdict or {}).get("notes") or []]
    birth = manifest.get("replayVerification")
    return {
        "epochId": str(manifest.get("epochId") or epoch_dir.name),
        "dir": str(epoch_dir),
        "scope": str(manifest.get("scope") or ""),
        "recordedAt": str(manifest.get("recordedAt") or ""),
        "supersededBy": str(manifest.get("supersededBy") or ""),
        **_counts(manifest),
        "verdictSource": str(verdict_source),
        "verdict": dict(verdict) if verdict is not None else None,
        # The record's birth replay, where it ran: a receipt beside the verdict, never a substitute
        # for the study's own fresh check.
        "replayVerification": dict(birth) if isinstance(birth, Mapping) else None,
        "knobs": [str(item.get("knob")) for item in blocks if item.get("knob")],
        "blockedBy": [_blocked_text(item) for item in blocks],
        "notes": [_note_text(item) for item in notes],
        "reason": str(reason),
    }


def _stored_report(epoch_dir: Path) -> tuple[dict[str, Any] | None, str]:
    """The epoch's own written audit, and why it could not be read when it could not.

    Absent and unreadable are different states and are kept apart: absence falls through to a
    recomputation, while an unreadable report is a finding about the epoch that a silent fallback
    would erase.
    """

    path = Path(epoch_dir) / truncation.REPORT_NAME
    if not path.is_file():
        return None, ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return None, f"its stored audit {truncation.REPORT_NAME} is unreadable: {exc}"
    if not isinstance(payload, Mapping):
        return None, f"its stored audit {truncation.REPORT_NAME} is not a JSON object"
    return dict(payload), ""


def _run_dir_from_manifest(recorded: Any, corpus_parent: Path) -> Path | None:
    """Where the run a record was taken from left its artifacts, or ``None``.

    Three spellings are real: an absolute path (same checkout), a path relative to the repo root
    (a moved checkout, or one recorded elsewhere), and a directory that only shares its name with
    the record. Normalising the separators first keeps a record written on Windows findable on a
    POSIX checkout, where its backslashes are one long file name rather than a path.
    """

    text = str(recorded or "").strip()
    if not text:
        return None
    literal = Path(text)
    if literal.is_dir():
        return literal
    normalized = text.replace("\\", "/").rstrip("/")
    for candidate in (_ROOT / normalized, Path(corpus_parent) / normalized.rsplit("/", 1)[-1]):
        if str(candidate) not in ("", ".") and candidate.is_dir():
            return candidate
    return None


def epoch_row(
    epoch_dir: Path | str,
    *,
    corpus_parent: Path,
    recompute: bool = True,
) -> dict[str, Any]:
    """One epoch's line in the listing: its verdict, where that came from, and what would change it."""

    path = Path(epoch_dir)
    try:
        manifest = epoch_mod.read_epoch(path)
    except epoch_mod.EpochError as exc:
        return _row(
            epoch_dir=path,
            manifest={"epochId": path.name},
            verdict=None,
            verdict_source=VERDICT_UNAVAILABLE,
            reason=str(exc),
        )

    recorded = manifest.get("deltaReferee")
    if isinstance(recorded, Mapping):
        return _row(
            epoch_dir=path,
            manifest=manifest,
            verdict=recorded,
            verdict_source=VERDICT_RECORDED,
        )

    report, unreadable = _stored_report(path)
    if unreadable:
        return _row(
            epoch_dir=path,
            manifest=manifest,
            verdict=None,
            verdict_source=VERDICT_UNAVAILABLE,
            reason=unreadable,
        )
    if report is not None:
        return _row(
            epoch_dir=path,
            manifest=manifest,
            verdict=truncation.referee_verdict(report),
            verdict_source=VERDICT_REPORT,
        )

    if not recompute:
        return _row(
            epoch_dir=path,
            manifest=manifest,
            verdict=None,
            verdict_source=VERDICT_UNAVAILABLE,
            reason=(
                "no recorded verdict and no stored audit; pass without --no-recompute to re-run "
                "the audit over its artifacts"
            ),
        )

    run_dir = _run_dir_from_manifest(manifest.get("recordedRunDir"), corpus_parent)
    if run_dir is None:
        return _row(
            epoch_dir=path,
            manifest=manifest,
            verdict=None,
            verdict_source=VERDICT_UNAVAILABLE,
            reason=(
                "no recorded verdict, a stored audit absent, and no recorded run directory to "
                "re-run the audit over (EPOCH.json.recordedRunDir)"
            ),
        )
    try:
        replayed = truncation.audit_epoch(path, run_dir, scope=str(manifest.get("scope") or ""))
    except (OSError, ValueError) as exc:
        return _row(
            epoch_dir=path,
            manifest=manifest,
            verdict=None,
            verdict_source=VERDICT_UNAVAILABLE,
            reason=f"the audit could not be re-run over {run_dir}: {exc}",
        )
    return _row(
        epoch_dir=path,
        manifest=manifest,
        verdict=truncation.referee_verdict(replayed),
        verdict_source=VERDICT_RECOMPUTED,
    )


def epochs_in_scope(
    *,
    corpus_parent: Path,
    recompute: bool = True,
    epochs: Sequence[str] = (),
) -> list[dict[str, Any]]:
    """Every epoch in scope, newest first for a scan, with corpora that cannot be judged
    included as such.

    The scan adds directories that *look* like epochs but have no readable manifest -- a crashed
    partial record sits under the same parent -- because those are exactly what an operator
    choosing from a directory listing should not have to discover by hand.
    """

    parent = Path(corpus_parent)
    if epochs:
        return [
            epoch_row(item, corpus_parent=parent, recompute=recompute)
            for item in epochs
            if str(item).strip()
        ]

    rows = [
        epoch_row(path, corpus_parent=parent, recompute=recompute)
        for path in reversed(epoch_mod.epoch_dirs(parent))
    ]
    listed = {str(row["dir"]) for row in rows}
    if parent.is_dir():
        for child in sorted(parent.iterdir()):
            if not child.is_dir() or not child.name.startswith(epoch_mod.EPOCH_PREFIX):
                continue
            if str(child) in listed:
                continue
            rows.append(
                _row(
                    epoch_dir=child,
                    manifest={"epochId": child.name},
                    verdict=None,
                    verdict_source=VERDICT_UNAVAILABLE,
                    reason=f"no readable {epoch_mod.EPOCH_FILE}",
                )
            )
    return rows


def is_eligible(row: Mapping[str, Any]) -> bool:
    verdict = row.get("verdict")
    return bool(isinstance(verdict, Mapping) and verdict.get("eligible"))


def is_accepted(row: Mapping[str, Any]) -> bool:
    """Refused, and certified anyway by the operator's explicit override."""

    verdict = row.get("verdict")
    return bool(
        isinstance(verdict, Mapping) and verdict.get("accepted") and not verdict.get("eligible")
    )


def pick_referee(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    """The epoch to pin. Rows are newest first, so the pick is the first eligible one.

    Supersession is a tiebreak rather than a filter: an epoch a newer one replaced is still
    replayable, but it is the fallback, not the default.
    """

    eligible = [dict(row) for row in rows if is_eligible(row)]
    if not eligible:
        return None
    return next(
        (row for row in eligible if not str(row.get("supersededBy") or "")),
        eligible[0],
    )


def pick_line(rows: Sequence[Mapping[str, Any]], pick: Mapping[str, Any] | None) -> str:
    """The pick and its currency: a line that recommends a retired epoch has to say that it is one."""

    if pick is None:
        return (
            "- pick: **none** -- no listed epoch may be replayed against as a delta referee;"
            " re-record with the knob named below"
        )
    superseded_by = str(pick.get("supersededBy") or "")
    current = any(is_eligible(row) and not str(row.get("supersededBy") or "") for row in rows)
    if superseded_by and not current:
        tail = (
            f"; every eligible epoch is superseded (this one by `{superseded_by}`), so a re-record"
            " is the way to a current referee"
        )
    elif superseded_by:
        tail = f"; superseded by `{superseded_by}`, so prefer the newer eligible epoch below"
    else:
        tail = ""
    return (
        f"- pick: `{pick.get('epochId')}` -- scope {pick.get('scope') or 'unknown'}, recorded"
        f" {_when(pick.get('recordedAt'))}{tail}"
    )


def summarize(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    eligible = sum(1 for row in rows if is_eligible(row))
    accepted = sum(1 for row in rows if is_accepted(row))
    unjudgeable = sum(1 for row in rows if row.get("verdict") is None)
    return {
        "epochs": len(rows),
        "eligible": eligible,
        "refused": len(rows) - eligible - accepted - unjudgeable,
        "acceptedByOverride": accepted,
        "notJudgeable": unjudgeable,
    }


def build_payload(rows: Sequence[Mapping[str, Any]], *, corpus_parent: Path) -> dict[str, Any]:
    pick = pick_referee(rows)
    return {
        "corpusParent": str(corpus_parent),
        "counts": summarize(rows),
        "pick": pick if pick is None else {key: pick[key] for key in ("epochId", "dir", "scope")},
        "epochs": [dict(row) for row in rows],
    }


def _table(header: Sequence[str], body: Sequence[Sequence[str]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for cells in body:
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def _source_text(row: Mapping[str, Any]) -> str:
    key = str(row.get("verdictSource") or VERDICT_UNAVAILABLE)
    return SOURCE_TEXT.get(key, key)


def _superseded(row: Mapping[str, Any]) -> str:
    superseded_by = str(row.get("supersededBy") or "")
    return f"`{superseded_by}`" if superseded_by else NOT_PRESENT


def _birth_cell(row: Mapping[str, Any]) -> str:
    """The record's birth replay, if it ran: the verdict, its miss count, and any override."""

    birth = row.get("replayVerification")
    if not isinstance(birth, Mapping) or not birth.get("verdict"):
        return NOT_PRESENT
    notes: list[str] = []
    misses = int(birth.get("misses") or 0)
    if misses:
        notes.append(str(misses))
    if birth.get("accepted"):
        notes.append("accepted")
    verdict = str(birth["verdict"])
    return f"{verdict} ({', '.join(notes)})" if notes else verdict


def _epoch_cell(row: Mapping[str, Any]) -> str:
    return f"`{row.get('epochId')}`"


def render_markdown(rows: Sequence[Mapping[str, Any]], *, corpus_parent: Path) -> str:
    """The page a human reads instead of the manifests."""

    counts = summarize(rows)
    pick = pick_referee(rows)
    lines: list[str] = [
        "# Which epochs can referee a delta",
        "",
        f"- parent: `{corpus_parent}`",
        f"- {counts['epochs']} epoch(s): {counts['eligible']} eligible, {counts['refused']} refused,"
        f" {counts['acceptedByOverride']} accepted by override, {counts['notJudgeable']} not"
        " judgeable",
    ]
    lines.append(pick_line(rows, pick))
    lines.extend(
        [
            "- a refused epoch is still a coverage record: raise the named knob and re-record it, or"
            " record with `--accept-truncated-referee` to label it accepted-but-truncated",
            "",
        ]
    )

    refused = [row for row in rows if row.get("verdict") is not None and not is_eligible(row)]
    if refused:
        lines.extend(
            [
                "## Not a delta referee -- and the knob that would change that",
                "",
                *_table(
                    ("epoch", "scope", "recorded", "blocked by", "raise", "verdict from"),
                    [
                        [
                            _epoch_cell(row) + (" (accepted)" if is_accepted(row) else ""),
                            str(row.get("scope") or NOT_PRESENT),
                            _when(row.get("recordedAt")),
                            "; ".join(row.get("blockedBy") or []) or NOT_PRESENT,
                            " ".join(f"`{knob}`" for knob in row.get("knobs") or []) or NOT_PRESENT,
                            _source_text(row),
                        ]
                        for row in refused
                    ],
                ),
                "",
            ]
        )

    eligible = [row for row in rows if is_eligible(row)]
    if eligible:
        lines.extend(
            [
                "## Eligible referees (newest first)",
                "",
                *_table(
                    (
                        "epoch",
                        "scope",
                        "recorded",
                        "superseded by",
                        "birth replay",
                        "notes",
                        "verdict from",
                    ),
                    [
                        [
                            _epoch_cell(row),
                            str(row.get("scope") or NOT_PRESENT),
                            _when(row.get("recordedAt")),
                            _superseded(row),
                            _birth_cell(row),
                            "; ".join(row.get("notes") or []) or NOT_PRESENT,
                            _source_text(row),
                        ]
                        for row in eligible
                    ],
                ),
                "",
            ]
        )

    unjudgeable = [row for row in rows if row.get("verdict") is None]
    if unjudgeable:
        lines.extend(
            [
                "## Not judgeable (no verdict recorded or re-runnable)",
                "",
                *_table(
                    ("epoch", "why"),
                    [
                        [_epoch_cell(row), str(row.get("reason") or NOT_PRESENT)]
                        for row in unjudgeable
                    ],
                ),
                "",
            ]
        )

    lines.extend(
        [
            "The verdict is the record's, not this listing's: `truncation.referee_verdict` decides it",
            "-- a redirect pool that abandoned a host, or a source clock that stopped a source, is a",
            "bound a replay does not share, so it refuses certification -- and this command only says",
            "which epoch said what, and where that answer came from. The `birth replay` column is the",
            "check a record ran before finalizing its own manifest (`EPOCH.json.replayVerification`);",
            "it is a receipt, not a permission -- a study re-runs the check before pinning, and an",
            "accepted block (`--accept-unverified-referee`) labels a kept hole without certifying it.",
            "An epoch a record refused was moved to a sibling `rejected/` directory, which this scan",
            "does not read; pass its path explicitly to inspect it.",
            "",
        ]
    )
    return "\n".join(lines)


def write_json(payload: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "epochs",
        nargs="*",
        help="explicit epoch directories; default: every epoch under --corpus-parent",
    )
    parser.add_argument("--corpus-parent", default=str(DEFAULT_CORPUS_PARENT))
    parser.add_argument(
        "--no-recompute",
        action="store_true",
        help=(
            "report only what the epochs already recorded; an epoch with no recorded verdict is "
            "listed as not judgeable instead of having its audit re-run"
        ),
    )
    parser.add_argument(
        "--require-eligible",
        action="store_true",
        help=(
            "exit 3 when no listed epoch may referee a delta, so a study can gate on the answer "
            "instead of parsing it"
        ),
    )
    parser.add_argument("--json", default="", help="also write the machine copy to this path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    parent = Path(args.corpus_parent)
    epochs = [str(item) for item in args.epochs or []]
    if not epochs and not parent.is_dir():
        print(f"[referees] no epoch parent at {parent}", file=sys.stderr)
        return EXIT_INFRASTRUCTURE

    rows = epochs_in_scope(corpus_parent=parent, recompute=not args.no_recompute, epochs=epochs)
    payload = build_payload(rows, corpus_parent=parent)
    print(render_markdown(rows, corpus_parent=parent))
    if str(args.json or "").strip():
        print(f"[referees] wrote {write_json(payload, Path(args.json))}")

    counts = payload["counts"]
    if not rows:
        print(f"[referees] no recorded epochs under {parent}", file=sys.stderr)
    if args.require_eligible and payload["pick"] is None:
        print(
            f"[referees] refusing: {counts['refused']} refused,"
            f" {counts['acceptedByOverride']} accepted by override,"
            f" {counts['notJudgeable']} not judgeable, 0 eligible",
            file=sys.stderr,
        )
        return EXIT_NO_REFEREE
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
