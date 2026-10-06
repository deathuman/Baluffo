#!/usr/bin/env python3
"""Retire dead boards from a registry, with evidence and a control.

The R classification split the refused cross-site redirects into four kinds; only
``site_gone_or_moved`` (closure or acquisition) is terminal. Retirement is a visibility
change with real consequences -- the plan's standing rule is not to retire a board without
saying so -- so this tool demands three things before it writes anything:

1. a ``site_gone_or_moved`` classification for the row's source URL,
2. a board-root probe that comes back terminal **now** (the fetch report is a snapshot;
   the probe decides), and
3. a known-good control board passing through the same probe in the same run, so "the
   probe is broken" and "the board is gone" cannot be confused.

The transition itself (``transition_registry_to_retired``) refuses without all three, so a
bug here cannot land a silent retirement.

Usage:
  python tools/coverage_retire.py --report <fetch-report.json[.gz]> \\
      --registry data/defaults/source-registry-active.seed.json --out _out/coverage/retire-plan.json
  python tools/coverage_retire.py ... --apply --expect-rows N
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import shutil
import ssl
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)
_ROOT_DIR = str(Path(__file__).resolve().parents[1])
if _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)

from coverage_board_identity import (  # noqa: E402, I001
    build_candidate,
    candidate_identity,
    registry_identity,
)
from src.source_discovery.redirect_classification import (  # noqa: E402
    SITE_GONE,
    classify_cross_site_redirect,
)
from src.source_registry_state import (  # noqa: E402
    transition_registry_to_retired,
)

try:  # pragma: no cover - certifi is a declared dependency; the fallback is for slims
    import certifi

    _TLS_CONTEXT: ssl.SSLContext | None = ssl.create_default_context(cafile=certifi.where())
except Exception:  # noqa: BLE001
    _TLS_CONTEXT = None

_UA = "baluffo-coverage-retire"
_REDIRECT_RE = re.compile(r"Unsafe static redirect from (\S+) to ([^;\s]+)", re.IGNORECASE)

# A board root that serves today, so a probe failure on a candidate is evidence about the
# candidate rather than about the probe. Verified 200 through this exact path.
CONTROL_ROOT = "https://job-boards.greenhouse.io/2k"
_TERMINAL_STATUS = frozenset({404, 410})


def _load_json(path: Path) -> Any:
    raw = path.read_bytes()
    if path.suffix == ".gz":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def _host(url: str) -> str:
    return (urlparse(str(url or "")).hostname or "").strip().lower().removeprefix("www.")


def extract_redirect_pairs(report: Mapping[str, Any]) -> list[tuple[str, str]]:
    """Every ``Unsafe static redirect from S to T`` pair in the report's source errors."""
    pairs: list[tuple[str, str]] = []
    for row in report.get("sources") or []:
        if not isinstance(row, Mapping):
            continue
        pairs.extend(_REDIRECT_RE.findall(str(row.get("error") or "")))
    return pairs


def classified_terminal_sources(pairs: Sequence[tuple[str, str]]) -> dict[str, str]:
    """Source URL -> target URL for pairs classified ``site_gone_or_moved``."""
    terminal: dict[str, str] = {}
    for source_url, target_url in pairs:
        classification = classify_cross_site_redirect(source_url, target_url)
        if classification.kind == SITE_GONE and source_url not in terminal:
            terminal[source_url] = target_url
    return terminal


def probe_root(url: str, *, timeout: float) -> dict[str, Any]:
    request = Request(url, headers={"User-Agent": _UA})
    try:
        with urlopen(request, timeout=timeout, context=_TLS_CONTEXT) as response:
            return {
                "ok": True,
                "status": int(getattr(response, "status", 200) or 200),
                "final_url": str(response.geturl()),
            }
    except HTTPError as exc:
        return {"ok": False, "status": int(exc.code), "final_url": url, "error": f"HTTP {exc.code}"}
    except (URLError, OSError, ValueError) as exc:
        return {"ok": False, "status": 0, "final_url": url, "error": type(exc).__name__}


def _root_of(url: str) -> str:
    parsed = urlparse(str(url or ""))
    return f"https://{parsed.hostname or ''}/"


def root_is_terminal(source_url: str, *, timeout: float) -> tuple[bool, dict[str, Any]]:
    """Whether the source's board root is gone, judged now rather than from the snapshot.

    Terminal means: the root cannot be fetched on either scheme, answers 404/410, or
    redirects to a different site that classifies ``site_gone_or_moved``. A challenge or
    server error (403/429/5xx) is *not* terminal -- unproven is not gone.
    """
    source_root = _root_of(source_url)
    probes: list[dict[str, Any]] = []
    for candidate in (source_root, source_root.replace("https://", "http://", 1)):
        probe = probe_root(candidate, timeout=timeout)
        probes.append(probe)
        if probe["ok"]:
            final_host = _host(str(probe["final_url"]))
            if final_host == _host(candidate):
                return False, {**probe, "checked": candidate}
            classification = classify_cross_site_redirect(candidate, str(probe["final_url"]))
            terminal = classification.kind == SITE_GONE
            return terminal, {
                **probe,
                "checked": candidate,
                "classification": classification.kind,
            }
        if probe["status"] in _TERMINAL_STATUS:
            return True, {**probe, "checked": candidate}
        if probe["status"]:
            # An HTTP answer that is neither a page nor gone (403/429/5xx): unproven.
            return False, {**probe, "checked": candidate}
        # status 0: a transport failure on this scheme; the other scheme may still answer.
    return True, {"terminal_by": "unreachable", "checked": source_root, "probes": probes}


def match_registry_rows(
    rows: Sequence[Mapping[str, Any]], source_url: str
) -> list[Mapping[str, Any]]:
    """Rows on the source's host, preferring an exact listing-URL match.

    Host plus tenant, never a label: the registry carries one board as both "Lost Boys
    Interactive" and "Lost Boys Interactive (Embracer Group)", and a label-keyed join
    retires the wrong row.
    """
    _adapter, host, tenant = candidate_identity(build_candidate(source_url))
    if not host:
        return []
    exact: list[Mapping[str, Any]] = []
    same_host: list[Mapping[str, Any]] = []
    for row in rows:
        parsed = registry_identity(str(row.get("id") or ""))
        if parsed is None:
            continue
        _row_adapter, row_host, row_tenant = parsed
        if row_host != host:
            continue
        if tenant and row_tenant and tenant != row_tenant:
            continue
        same_host.append(row)
        if str(source_url).strip().lower() in str(row.get("id") or "").lower():
            exact.append(row)
    return exact or same_host


def _control_probe(*, timeout: float) -> dict[str, Any]:
    probe = probe_root(CONTROL_ROOT, timeout=timeout)
    final_host = _host(str(probe.get("final_url"))) if probe["ok"] else ""
    control_ok = bool(probe["ok"]) and final_host == _host(CONTROL_ROOT)
    return {"ok": control_ok, "root": CONTROL_ROOT, "probe": probe}


def build_plan(
    report_path: Path, registry_path: Path, *, timeout: float, limit: int | None = None
) -> dict[str, Any]:
    report = _load_json(report_path)
    registry = _load_json(registry_path)
    rows = registry if isinstance(registry, list) else list(registry.get("rows") or [])
    pairs = extract_redirect_pairs(report)
    terminal_sources = classified_terminal_sources(pairs)
    control = _control_probe(timeout=timeout)

    retired: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for source_url, target_url in sorted(terminal_sources.items()):
        if limit is not None and len(retired) >= limit:
            break
        matched = match_registry_rows(rows, source_url)
        if not matched:
            skipped.append({"source_url": source_url, "why": "no registry row on that host"})
            continue
        terminal, probe = root_is_terminal(source_url, timeout=timeout)
        if not terminal:
            skipped.append(
                {"source_url": source_url, "why": "board root still answers", "probe": probe}
            )
            continue
        for row in matched:
            retired.append(
                {
                    "row": dict(row),
                    "source_url": source_url,
                    "target_url": target_url,
                    "classification": SITE_GONE,
                    "probe": {"terminal": terminal, **probe},
                    "control": control,
                }
            )
    return {
        "report": str(report_path),
        "registry": str(registry_path),
        "pairs": len(pairs),
        "terminal_sources": len(terminal_sources),
        "control": control,
        "retired": retired,
        "skipped": skipped,
    }


def render(plan: Mapping[str, Any]) -> str:
    lines = [
        f"report: {plan['report']}",
        f"registry: {plan['registry']}",
        f"  redirect pairs in report : {plan['pairs']}",
        f"  site_gone_or_moved       : {plan['terminal_sources']}",
        f"  control ({plan['control']['root']}) : {'ok' if plan['control']['ok'] else 'FAILED'}",
        f"  rows to retire           : {len(plan['retired'])}",
        f"  skipped                  : {len(plan['skipped'])}",
        "",
        f"{'row id':<78} source",
        "-" * 130,
    ]
    for item in plan["retired"][:40]:
        lines.append(f"{str(item['row'].get('id'))[:78]:<78} {item['source_url']}")
    for item in plan["skipped"][:10]:
        lines.append(f"  skip: {item['source_url']} -- {item['why']}")
    return "\n".join(lines)


def prepare_retirements(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Retired rows from a plan, through the gated transition.

    Raises (via the transition) if any item lacks its classification, its terminal probe or
    a passing control -- so a control that failed mid-run cannot half-apply a plan.
    """
    prepared: list[dict[str, Any]] = []
    for item in plan["retired"]:
        prepared.append(
            transition_registry_to_retired(
                item["row"],
                classification=item["classification"],
                probe={**item["probe"], "control_ok": bool(plan["control"]["ok"])},
                actor="coverage_retire_evidence_gate",
            )
        )
    return prepared


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--out", type=Path, help="where to write the plan (dry run)")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expect-rows", type=int)
    parser.add_argument("--backup-dir", type=Path, default=Path("_out/coverage/retire-backups"))
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args(argv)

    plan = build_plan(args.report, args.registry, timeout=args.timeout, limit=args.limit)
    print(render(plan))

    prepared = prepare_retirements(plan)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            json.dumps(
                {"retired": prepared, "skipped": plan["skipped"]}, indent=2, ensure_ascii=False
            ),
            encoding="utf-8",
        )
        print(f"\nplan written to {args.out} (not applied)")

    if not args.apply:
        print("dry run: nothing written. Re-run with --apply to write.")
        return 0

    if args.expect_rows is not None and len(prepared) != args.expect_rows:
        raise SystemExit(
            f"expected {args.expect_rows} retirement rows, found {len(prepared)}; refusing to write"
        )

    registry = _load_json(args.registry)
    rows = registry if isinstance(registry, list) else list(registry.get("rows") or [])
    by_id = {str(row.get("id")): index for index, row in enumerate(rows)}
    missing = [row["id"] for row in prepared if str(row.get("id")) not in by_id]
    if missing:
        raise SystemExit(f"rows absent from the registry: {sorted(missing)[:5]}")

    args.backup_dir.mkdir(parents=True, exist_ok=True)
    backup = args.backup_dir / args.registry.name
    shutil.copy2(args.registry, backup)

    for row in prepared:
        rows[by_id[str(row["id"])]] = row
    args.registry.write_text(
        json.dumps(rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    readback = _load_json(args.registry)
    read_rows = readback if isinstance(readback, list) else list(readback.get("rows") or [])
    states = {str(row.get("id")): str(row.get("registryState")) for row in read_rows}
    not_retired = [row["id"] for row in prepared if states.get(str(row.get("id"))) != "rejected"]
    if not_retired:
        raise SystemExit(f"read-back found {len(not_retired)} rows not rejected: {not_retired[:5]}")
    print(f"backup: {backup}")
    print(f"retired {len(prepared)} rows in {args.registry}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
