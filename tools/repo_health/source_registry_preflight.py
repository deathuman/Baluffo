"""Live source-registry preflight: the surface the seed-only guardrails miss.

Every structural guardrail in
``source_registry_duplicate_url_policy`` reads the **committed seed**
(``data/defaults/*.seed.json``). The registry the pipeline actually fetches is
the live one, loaded from SQLite when the store owns the registry and from
``data/source-registry-active.json`` otherwise. Those two can diverge, and until
2026-09-25 nothing surfaced the difference: the commit-time gate stayed green
while the live registry carried 2,245 rows against the seed's 1,893, including
31 canonical-URL collision groups that existed only in the live view and were
therefore invisible to every guard.

This module closes that gap by reusing the existing, already-tested predicates
against the live rows, and additionally reports live-versus-seed drift. It
deliberately reuses those predicates rather than reimplementing them, so the two
sides cannot disagree about what counts as a defect.

**Advisory by default.** The live registry carries real operator work, and a
repair decision is a human one, so this reports and exits 0 unless ``--strict``
is passed. The strict mode exists for the point at which drift has been
reconciled and should be held shut; turning it on before then would fail on
every invocation and get ignored, which is worse than not having it.

Run it directly::

    python tools/repo_health/source_registry_preflight.py
    python tools/repo_health/source_registry_preflight.py --json
    python tools/repo_health/source_registry_preflight.py --strict
    python tools/repo_health/source_registry_preflight.py \\
        --worksheet _out/registry-repair/worksheet.json

``--worksheet`` writes the per-row adjudication inventory: every structural
finding and every live-versus-seed drift item, grouped by the decision each one
implies, each carrying its evidence, suggested dispositions, and an explicit
``disposition: "undecided"``. It records what is outstanding so a human can
adjudicate 31 collisions and 165 seed-only rows in one pass instead of
re-deriving them by hand; it deliberately does not decide any of them.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.repo_health.source_registry_duplicate_url_policy import (  # noqa: E402
    _load_known_collisions,
    list_active_url_collisions,
    list_definitionless_static_rows,
    list_duplicate_source_ids,
    list_rows_with_inline_asset_urls,
    list_rows_with_malformed_page_refs,
    list_stale_known_collisions,
)

_MAX_SAMPLES = 5


def load_live_rows(data_root: Path) -> tuple[list[dict[str, Any]], str]:
    """Load the live active registry the way the pipeline does.

    Delegates to the runtime loader so this tool cannot disagree with the
    fetcher about which rows are live.

    The loader is deliberately forgiving: a missing live file resolves to the
    committed seed (correct for a fresh install), and a *corrupt* live file
    also degrades to the seed rather than taking the pipeline down. That
    second behavior is right at runtime and wrong to hide here, because it
    silently discards the operator's live registry. So the file is also parsed
    directly, and a live file that exists but is not a JSON array is reported
    as a warning even though the loader returned the seed without complaint.
    """
    from src.jobs.common.sources import load_runtime_studio_source_registry

    path = Path(data_root) / "source-registry-active.json"
    try:
        rows = load_runtime_studio_source_registry(path)
    except Exception as exc:  # noqa: BLE001 - a preflight must not hard-fail
        return [], f"could not load live registry {path}: {exc}"

    warning = ""
    if path.exists():
        raw: Any = None
        parse_error = ""
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            parse_error = str(exc)
        if parse_error:
            warning = (
                f"live registry {path} is unreadable ({parse_error}); the runtime "
                "loader silently served the committed seed instead, so the live "
                "rows reported below are NOT the ones on disk"
            )
        elif not isinstance(raw, list):
            warning = (
                f"live registry {path} is a {type(raw).__name__}, not a JSON array; "
                "the runtime loader silently served the committed seed instead"
            )
    return [dict(row) for row in rows if isinstance(row, dict)], warning


def load_seed_rows(data_root: Path) -> tuple[list[dict[str, Any]], str]:
    path = Path(data_root) / "defaults" / "source-registry-active.seed.json"
    if not path.exists():
        return [], f"active seed not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [], f"could not read active seed {path}: {exc}"
    if not isinstance(payload, list):
        return [], f"active seed {path} is not a JSON array"
    return [row for row in payload if isinstance(row, dict)], ""


def _ids(rows: list[dict[str, Any]]) -> set[str]:
    return {str(row.get("id") or "").strip() for row in rows if str(row.get("id") or "").strip()}


def load_known_collisions(data_root: Path) -> set[str]:
    """The reviewed-collision baseline for *this* data root.

    Resolved relative to ``data_root`` rather than the repository root, so a
    preflight pointed at a different data directory is judged by that
    directory's baseline. Hard-wiring the repo copy would make every other
    data root look like it had seven stale baseline entries.
    """
    from src.source_registry_data import load_known_collision_urls

    candidate = Path(data_root) / "defaults" / "source-registry-known-url-collisions.json"
    if candidate.exists():
        return load_known_collision_urls(candidate)
    return _load_known_collisions(ROOT)


def build_report(data_root: Path) -> dict[str, Any]:
    """Run every structural predicate plus a drift check over live rows."""
    data_root = Path(data_root)
    live_rows, live_warning = load_live_rows(data_root)
    seed_rows, seed_warning = load_seed_rows(data_root)
    known = load_known_collisions(data_root)

    live_ids, seed_ids = _ids(live_rows), _ids(seed_rows)
    live_only = sorted(live_ids - seed_ids)
    seed_only = sorted(seed_ids - live_ids)

    # The duplicate-collision set is the one place the seed and live views can
    # be compared meaningfully: a URL covered by the reviewed baseline in one
    # view and not the other is exactly the drift the commit-time gate misses.
    def _uncovered(rows: list[dict[str, Any]]) -> list[str]:
        return list_active_url_collisions(rows, known_urls=known)

    live_uncovered = _uncovered(live_rows)
    seed_uncovered = _uncovered(seed_rows)
    live_only_collisions = [m for m in live_uncovered if m not in seed_uncovered]

    # Every structural finding is a human repair decision, so the report carries
    # the full per-item detail rather than a sample: an operator adjudicating
    # 31 collisions and 165 seed-only rows needs the list, not a preview. These
    # lists are bounded by the registry size, which is small and local.
    definitionless = list_definitionless_static_rows(live_rows)
    malformed = list_rows_with_malformed_page_refs(live_rows)

    return {
        "liveRowCount": len(live_rows),
        "seedRowCount": len(seed_rows),
        "liveOnlyRowCount": len(live_only),
        "seedOnlyRowCount": len(seed_only),
        "liveOnlyRowIds": live_only,
        "seedOnlyRowIds": seed_only,
        "uncoveredCollisionsLive": len(live_uncovered),
        "uncoveredCollisionsSeed": len(seed_uncovered),
        "uncoveredCollisionsLiveOnly": live_only_collisions,
        "definitionlessRows": definitionless,
        "definitionlessSourceIds": [
            str(row.get("id") or "").strip()
            for row in live_rows
            if str(row.get("id") or "").strip().startswith("static:")
            and not str(row.get("listing_url") or "").strip()
            and not any(str(page or "").strip() for page in (row.get("pages") or []))
        ],
        "malformedPageRows": malformed,
        "inlineAssetRows": list_rows_with_inline_asset_urls(live_rows),
        "duplicateIdRows": list_duplicate_source_ids(live_rows),
        "staleBaselineEntries": list_stale_known_collisions(known, active_rows=live_rows),
        "warnings": [warning for warning in (live_warning, seed_warning) if warning],
    }


def _has_structural_defects(report: dict[str, Any]) -> list[str]:
    sections = {
        "definitionless static rows": report["definitionlessRows"],
        "inline asset rows": report["inlineAssetRows"],
        "malformed page rows": report["malformedPageRows"],
        "duplicate registry ids": report["duplicateIdRows"],
    }
    found = [f"{count} {label}" for label, rows in sections.items() if (count := len(rows))]
    if report["uncoveredCollisionsLive"]:
        found.append(f"{report['uncoveredCollisionsLive']} uncovered duplicate-URL groups (live)")
    return found


def render_text(report: dict[str, Any], *, strict: bool) -> str:
    lines = [
        "source registry preflight (live view)",
        f"  live rows            : {report['liveRowCount']}",
        f"  seed rows            : {report['seedRowCount']}",
        f"  live-only rows       : {report['liveOnlyRowCount']}",
        f"  seed-only rows       : {report['seedOnlyRowCount']}",
        f"  uncovered collisions : {report['uncoveredCollisionsLive']} live / "
        f"{report['uncoveredCollisionsSeed']} seed",
        f"  stale baseline       : {len(report['staleBaselineEntries'])}",
    ]
    if report["liveOnlyRowIds"]:
        lines.append(f"  live-only sample     : {', '.join(report['liveOnlyRowIds'][:3])}")
    if report["seedOnlyRowIds"]:
        lines.append(f"  seed-only sample     : {', '.join(report['seedOnlyRowIds'][:3])}")

    defects = _has_structural_defects(report)
    if defects:
        lines.append("")
        lines.append("structural findings in the live registry:")
        lines.extend(f"  - {item}" for item in defects)
    else:
        lines.append("")
        lines.append("no structural defects in the live registry")

    if report["warnings"]:
        lines.append("")
        lines.append("warnings:")
        lines.extend(f"  ! {warning}" for warning in report["warnings"])

    if defects or report["warnings"]:
        lines.append("")
        if strict:
            lines.append("STRICT: failing because the live registry has findings.")
        else:
            lines.append(
                "advisory: pass --strict to fail on these. Each is a human repair "
                "decision, not a mechanical fix."
            )
    return "\n".join(lines)


def write_worksheet(report: dict[str, Any], path: Path) -> None:
    """Write the adjudication worksheet.

    Grouped by decision rather than by check, because the operator's unit of
    work is "decide what happens to these rows", not "satisfy this predicate".
    Each item carries the evidence needed to decide without re-running the
    tool, and an explicit ``disposition`` of ``undecided`` -- the worksheet
    records what is outstanding, it does not pre-empt the call.
    """
    items: list[dict[str, Any]] = []
    for source_id in report["definitionlessSourceIds"]:
        items.append(
            {
                "kind": "definitionless_static_row",
                "sourceId": source_id,
                "evidence": "no listing_url and no pages; fetches nothing while reporting ok",
                "suggestedDispositions": ["retire", "repoint"],
            }
        )
    for message in report["uncoveredCollisionsLiveOnly"]:
        items.append(
            {
                "kind": "uncovered_duplicate_url",
                "sourceId": message.split(" is registered")[0],
                "evidence": message,
                "suggestedDispositions": ["baseline", "retire_one_twin"],
            }
        )
    for source_id in report["seedOnlyRowIds"]:
        items.append(
            {
                "kind": "seed_only_row",
                "sourceId": source_id,
                "evidence": "present in the committed seed but not live; a seed "
                "restore would resurrect it",
                "suggestedDispositions": ["prune_from_seed", "restore_to_live"],
            }
        )
    for source_id in report["liveOnlyRowIds"]:
        items.append(
            {
                "kind": "live_only_row",
                "sourceId": source_id,
                "evidence": "live but absent from the committed seed; a fresh install "
                "would not have it",
                "suggestedDispositions": ["add_to_seed", "retire_from_live"],
            }
        )
    for entry in report["staleBaselineEntries"]:
        items.append(
            {
                "kind": "stale_baseline_entry",
                "sourceId": entry,
                "evidence": entry,
                "suggestedDispositions": ["prune_entry", "restore_twin"],
            }
        )

    for item in items:
        item["disposition"] = "undecided"
        item["decidedBy"] = ""
        item["decidedAt"] = ""
        item["note"] = ""

    payload = {
        "schemaVersion": "1.0",
        "summary": {
            "liveRowCount": report["liveRowCount"],
            "seedRowCount": report["seedRowCount"],
            "undecidedCount": len(items),
        },
        "warnings": report["warnings"],
        "items": items,
    }
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-root", default=str(ROOT / "data"))
    parser.add_argument("--json", action="store_true", help="emit the raw report")
    parser.add_argument(
        "--worksheet",
        metavar="PATH",
        help="write a per-row adjudication worksheet for the human repair decision",
    )
    parser.add_argument(
        "--strict", action="store_true", help="exit 1 when the live registry has findings"
    )
    args = parser.parse_args(argv)

    report = build_report(Path(args.data_root))
    if args.worksheet:
        write_worksheet(report, Path(args.worksheet))
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print(render_text(report, strict=args.strict))

    if args.strict and (_has_structural_defects(report) or report["warnings"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
