#!/usr/bin/env python3
"""Refresh or retire failed static sources, and route ATS boards natively.

Two failed-source shapes share the same registry machinery but need opposite
outcomes:

* **ATS signature** -- the page is an ATS board registered as static.  Reclassify
  the row onto the existing provider adapter so its native loader collects it.
* **site_changed** -- the listing moved, died, or was acquired.  Follow the
  current URL; reclassify an ATS landing, repoint a live same-site move, and
  retire only with a terminal board-root probe plus a passing known-good control.

The tool is dry-run by default.  ``--apply`` requires ``--expect-rows``, backs up
the registry first, and reads every changed row back.  It never guesses a tenant
from a studio label: board identity is host + tenant, and a route is skipped when
the tenant cannot be read from the page or the target URL.
"""

from __future__ import annotations

import argparse
import gzip
import json
import shutil
import ssl
import sys
from collections.abc import Callable, Mapping, Sequence
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

import coverage_retire  # noqa: E402
from coverage_board_identity import (  # noqa: E402
    candidate_identity,
    describe_board,
    registry_identity,
)

from src.jobs.adapters.ats_signature_routing import (  # noqa: E402
    ATS_SIGNATURES,
    ROUTABLE_ADAPTERS,
    adapter_from_warning,
    extract_ats_route,
    reclassified_registry_row,
)
from src.source_discovery.redirect_classification import (  # noqa: E402
    MIGRATION,
    SITE_GONE,
    classify_cross_site_redirect,
)
from src.source_registry_state import transition_registry_to_retired  # noqa: E402

_UA = "baluffo-coverage-source-refresh"
_SOURCE_PREFIX = "static_source::"
_STATIC_ID_PREFIX = "static:listing_url:"
_TERMINAL_STATUS = frozenset({404, 410})
_PROBE_BODY_LIMIT = 1_500_000
_CONTROL_ROOT = coverage_retire.CONTROL_ROOT

try:  # pragma: no cover - certifi is a declared dependency; the fallback is for slims
    import certifi

    _TLS_CONTEXT: ssl.SSLContext | None = ssl.create_default_context(cafile=certifi.where())
except Exception:  # noqa: BLE001
    _TLS_CONTEXT = None


def _load_json(path: Path) -> Any:
    raw = path.read_bytes()
    if path.suffix == ".gz":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def _registry_payload(path: Path) -> Any:
    payload = _load_json(path)
    rows = payload if isinstance(payload, list) else list(payload.get("rows") or [])
    return payload, rows


def _write_registry(path: Path, payload: Any, rows: Sequence[Mapping[str, Any]]) -> None:
    if isinstance(payload, list):
        output: Any = list(rows)
    else:
        output = dict(payload)
        output["rows"] = list(rows)
    path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _host(url: str) -> str:
    return (urlparse(str(url or "")).hostname or "").strip().lower().removeprefix("www.")


def _clean_url(url: Any) -> str:
    text = str(url or "").strip()
    return text.split("#", 1)[0].rstrip("/") if text else ""


def _same_url(left: Any, right: Any) -> bool:
    return bool(_clean_url(left)) and _clean_url(left) == _clean_url(right)


def _source_id(row: Mapping[str, Any]) -> str:
    name = str(row.get("name") or "").strip()
    if name.startswith(_SOURCE_PREFIX):
        return name[len(_SOURCE_PREFIX) :]
    source_id = str(row.get("sourceId") or "").strip()
    return source_id if source_id.startswith("static:") else ""


def _source_url(row: Mapping[str, Any], source_id: str) -> str:
    for key in ("sourceUrl", "source_url", "listingUrl", "url", "careersUrl"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    if source_id.startswith(_STATIC_ID_PREFIX):
        return source_id[len(_STATIC_ID_PREFIX) :]
    return ""


def _report_static_rows(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in report.get("sources") or []:
        if not isinstance(raw, Mapping):
            continue
        source_id = _source_id(raw)
        if not source_id or source_id in seen:
            continue
        source_url = _source_url(raw, source_id)
        if not source_url:
            continue
        seen.add(source_id)
        rows.append(
            {
                **dict(raw),
                "sourceId": source_id,
                "sourceUrl": source_url,
                "keptCount": int(raw.get("keptCount") or 0),
                "failureBucket": str(raw.get("failureBucket") or ""),
            }
        )
    return rows


def _report_ats_route(row: Mapping[str, Any]) -> dict[str, Any] | None:
    direct = row.get("atsRoute")
    if isinstance(direct, Mapping) and str(direct.get("adapter") or "") in ROUTABLE_ADAPTERS:
        return dict(direct)
    details = row.get("details")
    if isinstance(details, list):
        for detail in details:
            if not isinstance(detail, Mapping):
                continue
            route = detail.get("atsRoute")
            if isinstance(route, Mapping) and str(route.get("adapter") or "") in ROUTABLE_ADAPTERS:
                return dict(route)
    return None


def _is_ats_failure(row: Mapping[str, Any]) -> bool:
    if row.get("keptCount") != 0:
        return False
    text = f"{row.get('error') or ''} {row.get('classification') or ''}"
    lowered = text.lower()
    if adapter_from_warning(text) or "atsroute" in lowered:
        return True
    return any(
        adapter in ROUTABLE_ADAPTERS and token in lowered for adapter, token in ATS_SIGNATURES
    )


def _find_registry_row(
    rows: Sequence[Mapping[str, Any]], source_id: str, source_url: str
) -> Mapping[str, Any] | None:
    for row in rows:
        if str(row.get("id") or "") == source_id:
            return row
    exact = [
        row
        for row in rows
        if any(_same_url(row.get(key), source_url) for key in ("listing_url", "careersUrl"))
    ]
    if len(exact) == 1:
        return exact[0]
    return None


def _row_url(row: Mapping[str, Any]) -> str:
    for key in ("listing_url", "careersUrl", "sourceUrl", "url"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    pages = row.get("pages")
    if isinstance(pages, list):
        for page in pages:
            if str(page or "").strip():
                return str(page)
    return ""


def _provider_collision(
    rows: Sequence[Mapping[str, Any]],
    route: Mapping[str, Any],
    *,
    exclude_id: str,
) -> Mapping[str, Any] | None:
    adapter = str(route.get("adapter") or "")
    host = str(route.get("host") or "")
    tenant = str(route.get("tenant") or "")
    if not adapter or not host or not tenant:
        return None
    wanted = candidate_identity(
        describe_board(adapter=adapter, host=host, tenant=tenant, company=tenant)
    )
    for row in rows:
        if str(row.get("id") or "") == exclude_id:
            continue
        if str(row.get("adapter") or "") != adapter:
            continue
        identity = registry_identity(str(row.get("id") or ""))
        if identity == wanted:
            return row
    return None


def _probe_html(url: str, *, timeout: float) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "User-Agent": _UA,
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    try:
        with urlopen(request, timeout=timeout, context=_TLS_CONTEXT) as response:
            body = response.read(_PROBE_BODY_LIMIT).decode("utf-8", "replace")
            return {
                "ok": True,
                "status": int(getattr(response, "status", 200) or 200),
                "finalUrl": str(response.geturl()),
                "body": body,
                "error": "",
            }
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read(_PROBE_BODY_LIMIT).decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            body = ""
        return {
            "ok": False,
            "status": int(exc.code),
            "finalUrl": str(exc.geturl() or url),
            "body": body,
            "error": f"HTTP {exc.code}",
        }
    except (URLError, OSError, ValueError) as exc:
        return {
            "ok": False,
            "status": 0,
            "finalUrl": url,
            "body": "",
            "error": type(exc).__name__,
        }


def _control_probe(*, timeout: float) -> dict[str, Any]:
    probe = coverage_retire.probe_root(_CONTROL_ROOT, timeout=timeout)
    control_ok = bool(probe.get("ok")) and _host(str(probe.get("final_url"))) == _host(
        _CONTROL_ROOT
    )
    return {"ok": control_ok, "root": _CONTROL_ROOT, "probe": probe}


def _root_is_terminal(url: str, *, timeout: float) -> tuple[bool, dict[str, Any]]:
    return coverage_retire.root_is_terminal(url, timeout=timeout, allow_unreachable=False)


def _route_from_target(
    target_url: str, *, adapter: str = "", body: str = ""
) -> dict[str, Any] | None:
    return extract_ats_route(
        body,
        target_url,
        expected_adapter=adapter,
        evidence="redirect_target" if not body else "redirect_target_html",
    )


def _plan_action(
    *,
    action: str,
    row: Mapping[str, Any],
    source_id: str,
    source_url: str,
    reason: str,
    route: Mapping[str, Any] | None = None,
    target_url: str = "",
    classification: str = "",
    probe: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "action": action,
        "sourceId": source_id,
        "sourceUrl": source_url,
        "reason": reason,
        "row": dict(row),
    }
    if route:
        item["route"] = dict(route)
    if target_url:
        item["targetUrl"] = target_url
    if classification:
        item["classification"] = classification
    if probe:
        item["probe"] = dict(probe)
    return item


def build_ats_plan(
    report: Mapping[str, Any],
    registry_rows: Sequence[Mapping[str, Any]],
    *,
    fetch_html: Callable[..., dict[str, Any]] = _probe_html,
    timeout: float = 30.0,
    limit: int | None = None,
) -> dict[str, Any]:
    """Plan native adapter routes for zero-kept static sources with ATS signatures."""
    actions: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    rows = [row for row in _report_static_rows(report) if _is_ats_failure(row)]
    if limit is not None:
        rows = rows[:limit]
    for row in rows:
        source_id = str(row["sourceId"])
        source_url = str(row["sourceUrl"])
        match = _find_registry_row(registry_rows, source_id, source_url)
        if match is None:
            skipped.append(
                {"sourceId": source_id, "sourceUrl": source_url, "why": "no registry row"}
            )
            continue
        route = _report_ats_route(row)
        probe: dict[str, Any] = {}
        if route is None:
            probe = dict(fetch_html(source_url, timeout=timeout))
            route = extract_ats_route(
                str(probe.get("body") or ""),
                str(probe.get("finalUrl") or source_url),
            )
        if route is None:
            skipped.append(
                {
                    "sourceId": source_id,
                    "sourceUrl": source_url,
                    "why": "ATS signature without a readable board tenant",
                    "probe": {k: probe.get(k) for k in ("status", "finalUrl", "error")}
                    if probe
                    else {},
                }
            )
            continue
        if str(route.get("adapter") or "") not in ROUTABLE_ADAPTERS:
            skipped.append(
                {"sourceId": source_id, "sourceUrl": source_url, "why": "unsupported ATS adapter"}
            )
            continue
        collision = _provider_collision(registry_rows, route, exclude_id=str(match.get("id") or ""))
        if collision is not None:
            skipped.append(
                {
                    "sourceId": source_id,
                    "sourceUrl": source_url,
                    "why": "provider row already covers this board",
                    "providerSourceId": collision.get("id"),
                }
            )
            continue
        actions.append(
            _plan_action(
                action="reclassify",
                row=match,
                source_id=source_id,
                source_url=source_url,
                reason="ATS signature matched a native provider board",
                route=route,
                probe={k: probe.get(k) for k in ("status", "finalUrl", "error")} if probe else {},
            )
        )
    return {
        "command": "ats",
        "signatureSources": len(rows),
        "actions": actions,
        "skipped": skipped,
    }


def build_site_changed_plan(
    report: Mapping[str, Any],
    registry_rows: Sequence[Mapping[str, Any]],
    *,
    fetch_html: Callable[..., dict[str, Any]] = _probe_html,
    root_is_terminal: Callable[..., tuple[bool, dict[str, Any]]] = _root_is_terminal,
    control_probe: Callable[..., dict[str, Any]] = _control_probe,
    timeout: float = 30.0,
    limit: int | None = None,
) -> dict[str, Any]:
    """Plan refresh, ATS migration, or evidence-gated retirement for ``site_changed``."""
    actions: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    rows = [
        row
        for row in _report_static_rows(report)
        if str(row.get("failureBucket") or "").strip().lower() == "site_changed"
    ]
    if limit is not None:
        rows = rows[:limit]
    control = control_probe(timeout=timeout)
    for row in rows:
        source_id = str(row["sourceId"])
        source_url = str(row["sourceUrl"])
        match = _find_registry_row(registry_rows, source_id, source_url)
        if match is None:
            skipped.append(
                {"sourceId": source_id, "sourceUrl": source_url, "why": "no registry row"}
            )
            continue
        probe = dict(fetch_html(source_url, timeout=timeout))
        final_url = str(probe.get("finalUrl") or source_url)
        status = int(probe.get("status") or 0)
        body = str(probe.get("body") or "")
        route = _route_from_target(final_url, body=body) if status == 200 else None
        if route is not None:
            collision = _provider_collision(
                registry_rows, route, exclude_id=str(match.get("id") or "")
            )
            if collision is None:
                actions.append(
                    _plan_action(
                        action="reclassify",
                        row=match,
                        source_id=source_id,
                        source_url=source_url,
                        reason="site changed onto a native ATS board",
                        route=route,
                        probe={k: probe.get(k) for k in ("status", "finalUrl", "error")},
                    )
                )
                continue
            skipped.append(
                {
                    "sourceId": source_id,
                    "sourceUrl": source_url,
                    "why": "provider row already covers this board",
                    "providerSourceId": collision.get("id"),
                }
            )
            continue

        classification = (
            classify_cross_site_redirect(source_url, final_url)
            if final_url and not _same_url(source_url, final_url)
            else None
        )
        if status in _TERMINAL_STATUS or (
            classification is not None and classification.kind == SITE_GONE
        ):
            terminal, root_probe = root_is_terminal(source_url, timeout=timeout)
            if terminal and control["ok"]:
                actions.append(
                    _plan_action(
                        action="retire",
                        row=match,
                        source_id=source_id,
                        source_url=source_url,
                        reason="listing and board root are terminal",
                        classification=SITE_GONE,
                        probe={**root_probe, "terminal": True, "control_ok": True},
                    )
                )
            elif bool(root_probe.get("ok")):
                target = str(root_probe.get("final_url") or _root_url(source_url))
                actions.append(
                    _plan_action(
                        action="repoint",
                        row=match,
                        source_id=source_id,
                        source_url=source_url,
                        reason="listing is gone but the board root answers",
                        target_url=target,
                        probe={k: root_probe.get(k) for k in ("status", "final_url", "error")},
                    )
                )
            else:
                skipped.append(
                    {
                        "sourceId": source_id,
                        "sourceUrl": source_url,
                        "why": "terminal listing without a terminal root or passing control",
                        "controlOk": control["ok"],
                        "probe": {k: root_probe.get(k) for k in ("status", "final_url", "error")},
                    }
                )
            continue

        if status == 200:
            if classification is not None and classification.kind == MIGRATION:
                target_route = _route_from_target(
                    final_url, adapter=classification.adapter, body=body
                )
                if target_route is None:
                    skipped.append(
                        {
                            "sourceId": source_id,
                            "sourceUrl": source_url,
                            "why": "platform migration without a readable provider tenant",
                            "classification": classification.kind,
                            "targetAdapter": classification.adapter,
                        }
                    )
                    continue
                collision = _provider_collision(
                    registry_rows, target_route, exclude_id=str(match.get("id") or "")
                )
                if collision is not None:
                    skipped.append(
                        {
                            "sourceId": source_id,
                            "sourceUrl": source_url,
                            "why": "provider row already covers this migrated board",
                            "providerSourceId": collision.get("id"),
                        }
                    )
                    continue
                actions.append(
                    _plan_action(
                        action="reclassify",
                        row=match,
                        source_id=source_id,
                        source_url=source_url,
                        reason="redirect classified as a platform migration",
                        route=target_route,
                        probe={k: probe.get(k) for k in ("status", "finalUrl", "error")},
                    )
                )
                continue
            if not _same_url(source_url, final_url):
                actions.append(
                    _plan_action(
                        action="repoint",
                        row=match,
                        source_id=source_id,
                        source_url=source_url,
                        reason="listing moved to a live URL",
                        target_url=final_url,
                        probe={k: probe.get(k) for k in ("status", "finalUrl", "error")},
                    )
                )
            else:
                skipped.append(
                    {
                        "sourceId": source_id,
                        "sourceUrl": source_url,
                        "why": "listing still answers; zero jobs is not a retirement signal",
                    }
                )
            continue

        skipped.append(
            {
                "sourceId": source_id,
                "sourceUrl": source_url,
                "why": "unproven status; not enough evidence to move or retire",
                "probe": {k: probe.get(k) for k in ("status", "finalUrl", "error")},
            }
        )
    return {
        "command": "site-changed",
        "siteChangedSources": len(rows),
        "control": control,
        "actions": actions,
        "skipped": skipped,
    }


def _root_url(url: str) -> str:
    parsed = urlparse(str(url or ""))
    return f"{parsed.scheme or 'https'}://{parsed.hostname or ''}/"


def _repointed_row(row: Mapping[str, Any], target_url: str, *, at: str) -> dict[str, Any]:
    updated = dict(row)
    updated["listing_url"] = target_url
    updated["careersUrl"] = target_url
    updated["pages"] = [target_url]
    updated["urlPatchApplied"] = True
    updated["urlRepair"] = {
        "previousUrl": _row_url(row),
        "targetUrl": target_url,
        "repairedAt": at,
    }
    updated["stateChangedAt"] = at
    updated["stateChangedBy"] = "site_changed_refresh"
    return updated


def prepare_actions(
    plan: Mapping[str, Any],
    registry_rows: Sequence[Mapping[str, Any]],
    *,
    at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Prepare registry rows for every action in a plan, without writing."""
    by_id = {str(row.get("id") or ""): row for row in registry_rows}
    prepared: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for action in plan.get("actions") or []:
        source_id = str(action.get("sourceId") or "")
        row = by_id.get(source_id)
        if row is None:
            skipped.append({"sourceId": source_id, "why": "row disappeared before apply"})
            continue
        kind = str(action.get("action") or "")
        if kind == "reclassify":
            route = action.get("route") if isinstance(action.get("route"), Mapping) else {}
            prepared.append(reclassified_registry_row(row, dict(route), at=at))
        elif kind == "repoint":
            target = str(action.get("targetUrl") or "").strip()
            if not target:
                skipped.append({"sourceId": source_id, "why": "repoint without a target URL"})
                continue
            prepared.append(_repointed_row(row, target, at=at))
        elif kind == "retire":
            probe = action.get("probe") if isinstance(action.get("probe"), Mapping) else {}
            prepared.append(
                transition_registry_to_retired(
                    dict(row),
                    classification=str(action.get("classification") or SITE_GONE),
                    probe=dict(probe),
                    actor="coverage_source_refresh",
                    at=at,
                )
            )
        else:
            skipped.append({"sourceId": source_id, "why": f"unknown action {kind!r}"})
    return prepared, skipped


def _readback_ok(action: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
    kind = str(action.get("action") or "")
    if kind == "reclassify":
        route = action.get("route") if isinstance(action.get("route"), Mapping) else {}
        return str(row.get("adapter") or "") == str(route.get("adapter") or "")
    if kind == "repoint":
        return _same_url(row.get("listing_url"), action.get("targetUrl"))
    if kind == "retire":
        return str(row.get("registryState") or "") == "rejected"
    return False


def apply_plan(
    plan: Mapping[str, Any],
    registry_path: Path,
    *,
    expect_rows: int,
    backup_dir: Path,
    at: str,
) -> dict[str, Any]:
    """Apply a plan with a backup and a per-row read-back."""
    if expect_rows < 0:
        raise ValueError("expect_rows must be non-negative")
    payload, rows = _registry_payload(registry_path)
    prepared, skipped = prepare_actions(plan, rows, at=at)
    if len(prepared) != expect_rows:
        raise SystemExit(
            f"expected {expect_rows} changed rows, prepared {len(prepared)}; refusing to write"
        )
    by_id = {str(row.get("id") or ""): index for index, row in enumerate(rows)}
    missing = [row.get("id") for row in prepared if str(row.get("id") or "") not in by_id]
    if missing:
        raise SystemExit(f"rows absent from the registry: {sorted(missing)[:5]}")

    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / registry_path.name
    shutil.copy2(registry_path, backup)

    updated_rows = list(rows)
    for row in prepared:
        updated_rows[by_id[str(row["id"])]] = row
    _write_registry(registry_path, payload, updated_rows)

    readback_payload, readback_rows = _registry_payload(registry_path)
    del readback_payload
    read_by_id = {str(row.get("id") or ""): row for row in readback_rows}
    failed = [
        action.get("sourceId")
        for action in plan.get("actions") or []
        if str(action.get("sourceId") or "") in read_by_id
        and not _readback_ok(action, read_by_id[str(action.get("sourceId") or "")])
    ]
    if failed:
        raise SystemExit(f"read-back found {len(failed)} unapplied rows: {failed[:5]}")
    return {
        "backup": str(backup),
        "changed": len(prepared),
        "skipped": skipped,
    }


def render(plan: Mapping[str, Any]) -> str:
    lines = [f"command: {plan.get('command')}"]
    count_key = "signatureSources" if plan.get("command") == "ats" else "siteChangedSources"
    lines.append(f"sources considered: {plan.get(count_key, 0)}")
    if plan.get("control"):
        control = plan["control"]
        lines.append(f"control ({control.get('root')}): {'ok' if control.get('ok') else 'FAILED'}")
    lines.append(f"actions: {len(plan.get('actions') or [])}")
    lines.append(f"skipped: {len(plan.get('skipped') or [])}")
    lines.append("")
    lines.append(f"{'action':<12} {'source':<72} target / reason")
    lines.append("-" * 150)
    for action in plan.get("actions") or []:
        target = (
            action.get("targetUrl")
            or (action.get("route") or {}).get("boardUrl")
            or action.get("reason", "")
        )
        lines.append(
            f"{action.get('action', ''):<12} {str(action.get('sourceId'))[:72]:<72} {target}"
        )
    for item in (plan.get("skipped") or [])[:20]:
        lines.append(f"{'skip':<12} {str(item.get('sourceId'))[:72]:<72} {item.get('why', '')}")
    return "\n".join(lines)


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--out", type=Path, help="where to write the dry-run plan")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expect-rows", type=int)
    parser.add_argument(
        "--backup-dir", type=Path, default=Path("_out/coverage/source-refresh-backups")
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--limit", type=int)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_common(subparsers.add_parser("ats", help="route ATS-signature static rows"))
    _add_common(subparsers.add_parser("site-changed", help="refresh or retire site_changed rows"))
    args = parser.parse_args(argv)
    if args.apply and args.expect_rows is None:
        parser.error("--apply requires --expect-rows")

    report = _load_json(args.report)
    _payload, registry_rows = _registry_payload(args.registry)
    if not isinstance(report, Mapping):
        raise SystemExit("report must be a JSON object")
    if args.command == "ats":
        plan = build_ats_plan(
            report,
            registry_rows,
            timeout=args.timeout,
            limit=args.limit,
        )
    else:
        plan = build_site_changed_plan(
            report,
            registry_rows,
            timeout=args.timeout,
            limit=args.limit,
        )
    print(render(plan))

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(plan, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"\nplan written to {args.out} (not applied)")

    if not args.apply:
        print("dry run: nothing written. Re-run with --apply --expect-rows N to write.")
        return 0

    from src.shared.utils import now_iso

    result = apply_plan(
        plan,
        args.registry,
        expect_rows=int(args.expect_rows),
        backup_dir=args.backup_dir,
        at=now_iso(),
    )
    print(f"backup: {result['backup']}")
    print(f"changed {result['changed']} rows in {args.registry}")
    if result["skipped"]:
        print(f"apply skipped {len(result['skipped'])} actions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
