"""Plan - and only with explicit flags apply - the registry cleanup a 404 probe implies.

Input is the verdict artifact from a board probe (``_out/reclassify/notfound_verdicts.json``
by default): one row per static source URL with the URL's status, the host root's status,
whether the source had ever succeeded, and a verdict. This tool turns those verdicts into
registry actions, matched **by host** (never by studio label), and prints the plan.

Three actions, because "the board is gone" and "the registered URL is wrong" are not the
same claim:

``retire_board``
    The host root and the registered URL both answer 404/410. Retired through
    ``transition_registry_to_retired``, which refuses without terminal evidence *and* a
    known-good control recorded in the verdict artifact (``control.ok``, stamped by the
    probe that produced the row). A root that only timed out or failed DNS, or an
    artifact with no passing control, is planned as ``[review]`` and never applied.
``retire_seed``
    The host is alive but the registered URL 404s and the source never delivered. The
    *row* is rejected (reason ``registry_cleanup_stale_seed``); the studio is not
    declared dead.
``repoint``
    The host is alive and the source succeeded in the past: the URL drifted. Needs a
    target from ``--repoint-map``; without one it is printed as ``needs-target`` and
    never applied.

Dry run is the default: nothing is created, moved or written. ``--apply`` additionally
requires ``--expect-retire`` / ``--expect-repoint`` to match the computed plan, writes a
backup under ``_out/registry-cleanup/``, saves through the canonical registry writer, and
reads the result back before reporting success.

Every plan also runs a read-only last-source guard. Active rows are grouped by their
canonical studio family (the same ``source_family_key`` the duplicate policy uses), with
board tenant/host as the fallback when that label is missing or generic. A planned retire
is marked ``LAST-SOURCE`` when it is the only active row left in its group, including the
case where several siblings retire together. The guard is advisory and printed in the
plan; the apply gates remain the expected-size assertions.

Usage::

    python tools/registry_cleanup_plan.py --data-dir "$APPDATA/Baluffo"
    python tools/registry_cleanup_plan.py --data-dir "$APPDATA/Baluffo" \\
        --repoint-map _out/reclassify/repoint_map.json
    python tools/registry_cleanup_plan.py --data-dir "$APPDATA/Baluffo" --apply \\
        --expect-retire 40 --expect-repoint 41
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
TOOLS_DIR = Path(__file__).resolve().parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from coverage_board_identity import registry_identity  # noqa: E402, I001
from src.source_registry_identity import normalize_source_url, source_family_key  # noqa: E402
from src.source_registry_io import load_json_array  # noqa: E402
from src.source_registry_io_save import save_registry_state_atomic  # noqa: E402
from src.source_registry_state import (  # noqa: E402
    RETIRE_TERMINAL_CLASSIFICATION,
    transition_registry_to_rejected,
    transition_registry_to_retired,
)

DEFAULT_VERDICTS = REPO_ROOT / "_out" / "reclassify" / "notfound_verdicts.json"
STALE_SEED_REASON = "registry_cleanup_stale_seed"
ACTOR = "registry_cleanup_plan"
BUCKETS = {
    "active": "source-registry-active",
    "pending": "source-registry-pending",
    "rejected": "source-registry-rejected",
}
TERMINAL_ROOT_STATUSES = {"404", "410"}
PRIORITY = {"repoint": 1, "retire_seed": 2, "retire_board": 3}
# Labels that identify a page or a field, not a studio. They must not merge unrelated
# rows into one "last source" group; the board tenant/host takes over instead.
GENERIC_STUDIO_KEYS = frozenset(
    {"", "career", "careers", "job", "jobs", "n a", "na", "none", "not applicable", "unknown"}
)


# -- inputs -----------------------------------------------------------------


def registry_paths(data_dir: Path) -> dict[str, Path]:
    """The entrypoint the install actually uses: lean ``.json.gz`` when present."""
    return {
        bucket: (
            compressed
            if (compressed := data_dir / f"{stem}.json.gz").exists()
            else data_dir / f"{stem}.json"
        )
        for bucket, stem in BUCKETS.items()
    }


def load_state(paths: dict[str, Path]) -> dict[str, list[dict[str, Any]]]:
    if not paths["active"].exists():
        raise SystemExit(
            f"no registry at {paths['active']} - pass --data-dir pointing at the Baluffo data directory"
        )
    return {bucket: load_json_array(path, []) for bucket, path in paths.items()}


def _dedupe(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    kept: list[str] = []
    for url in urls:
        key = normalize_source_url(url)
        if key and key not in seen:
            seen.add(key)
            kept.append(url)
    return kept


def row_urls(row: dict[str, Any]) -> list[str]:
    pages = row.get("pages")
    urls = [str(url) for url in pages if str(url).strip()] if isinstance(pages, list) else []
    for key in ("listingUrl", "board_url", "url"):
        value = str(row.get(key) or "").strip()
        if value:
            urls.append(value)
    return _dedupe(urls)


def _host_key(url: str) -> str:
    host = urlparse(normalize_source_url(url)).netloc.lower()
    return host.removeprefix("www.")


def _row_id(row: dict[str, Any]) -> str:
    return str(row.get("id") or "").strip()


def _canonical_studio_tenant(row: dict[str, Any]) -> tuple[str, str, str]:
    """Return ``(group key, basis, display label)`` for the last-source guard.

    The studio label is canonicalized with the repository's shared family key (case,
    punctuation and parentheticals are folded), so "Twin Studio" and "twin studio
    (parent)" are one studio. A missing or generic label falls back to the board's
    tenant, then host, then row id. That fallback is what keeps thirteen rows labelled
    ``jobs`` from becoming one false family; host/tenant is the only identity left when
    the label says nothing about the employer.
    """
    family = source_family_key(row)
    if family and family not in GENERIC_STUDIO_KEYS:
        label = str(row.get("studio") or row.get("name") or family).strip()
        return f"studio:{family}", "studio", label

    parsed = registry_identity(_row_id(row))
    if parsed is not None:
        _adapter, host, tenant = parsed
        if tenant and tenant.lower() not in GENERIC_STUDIO_KEYS:
            scope = f"{host}/{tenant}" if host else tenant
            return f"tenant:{scope}", "tenant", tenant
        if host:
            return f"host:{host}", "host", host

    row_id = _row_id(row)
    return f"row:{row_id}", "row", row_id


def last_source_guard(active_rows: list[dict[str, Any]], retire_ids: list[str]) -> dict[str, Any]:
    """Flag planned retires that empty every active row in a studio/tenant group.

    Read-only: the caller passes the pre-apply active rows and the rows the plan will
    retire. A group is flagged when at least one of its rows retires and no active row
    remains, so two siblings retiring together are caught even though neither is the
    last row in the unmodified registry.
    """
    retiring = {str(row_id).strip() for row_id in retire_ids if str(row_id).strip()}
    active_by_id = {_row_id(row): row for row in active_rows if _row_id(row)}
    groups: dict[str, dict[str, Any]] = {}
    for row_id, row in active_by_id.items():
        key, basis, label = _canonical_studio_tenant(row)
        group = groups.setdefault(key, {"key": key, "basis": basis, "label": label, "rowIds": []})
        group["rowIds"].append(row_id)

    flags: list[dict[str, Any]] = []
    matched: set[str] = set()
    for group in groups.values():
        row_ids = sorted(set(group["rowIds"]))
        retirees = [row_id for row_id in row_ids if row_id in retiring]
        if not retirees:
            continue
        matched.update(retirees)
        if any(row_id not in retiring for row_id in row_ids):
            continue
        flags.append(
            {
                "groupKey": group["key"],
                "groupBasis": group["basis"],
                "label": group["label"],
                "activeCount": len(row_ids),
                "retireCount": len(retirees),
                "remainingCount": 0,
                "retireRowIds": retirees,
                "activeRowIds": row_ids,
            }
        )

    flags.sort(key=lambda item: (item["label"].lower(), item["groupKey"]))
    return {
        "groups": len(groups),
        "retireIds": len(retiring),
        "matchedRetireIds": len(matched),
        "unmatchedRetireIds": sorted(retiring - matched),
        "flags": flags,
    }


def _name_url(name: str) -> str:
    """The registered source URL carried in ``static_source::static:listing_url:<url>``.

    The failing URL is often a different host (a redirect target, an ATS subdomain), so
    this is the identity that pairs a verdict with its registry row.
    """
    marker = "listing_url:"
    index = name.find(marker)
    return name[index + len(marker) :].strip() if index >= 0 else ""


def _verdict_groups(verdicts: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for verdict in verdicts:
        name = str(verdict.get("name") or "").strip()
        if not name:
            continue
        group = groups.setdefault(
            name,
            {
                "name": name,
                "urls": [],
                "kinds": [],
                "evidence": [],
                "registeredUrls": [],
            },
        )
        registered = _name_url(name)
        if registered and registered not in group["registeredUrls"]:
            group["registeredUrls"].append(registered)
        for url in [*(verdict.get("urls") or []), verdict.get("url")]:
            text = str(url or "").strip()
            if text and text not in group["urls"]:
                group["urls"].append(text)
        group["kinds"].append(str(verdict.get("verdict") or ""))
        group["evidence"].append(
            {
                "url": str(verdict.get("url") or ""),
                "urlStatus": verdict.get("url_status"),
                "root": str(verdict.get("root") or ""),
                "rootStatus": verdict.get("root_status"),
                "control": verdict.get("control") or {},
            }
        )
    return groups


def load_repoint_map(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"{path} must contain a JSON object of old URL/host -> new URL")
    targets: dict[str, str] = {}
    for key, value in payload.items():
        target = str(value or "").strip()
        if not target.startswith(("http://", "https://")):
            raise SystemExit(f"repoint target for {key!r} is not an http(s) URL: {target!r}")
        raw_key = str(key).strip()
        targets[raw_key.lower()] = target
        targets[normalize_source_url(raw_key)] = target
        targets[_host_key(raw_key)] = target
    return targets


# -- planning ---------------------------------------------------------------


def _action_kind(kinds: list[str]) -> str:
    if "retire_dead_host" in kinds:
        return "retire_board"
    if any(kind in kinds for kind in ("retire_stale_seed", "retire_deep_seed")):
        return "retire_seed"
    if "repoint" in kinds:
        return "repoint"
    return "skip"


def _terminal(evidence: list[dict[str, Any]]) -> bool:
    return any(str(item.get("rootStatus")) in TERMINAL_ROOT_STATUSES for item in evidence)


def _control_ok(evidence: list[dict[str, Any]]) -> bool:
    """Every row behind the action must carry the probe's passing known-good control."""
    return bool(evidence) and all(bool((item.get("control") or {}).get("ok")) for item in evidence)


def _readiness(kind: str, *, terminal: bool, control: bool, target: str) -> str:
    if kind == "repoint":
        return "ready" if target else "needs-target"
    if kind == "retire_board":
        return "terminal-evidence" if terminal and control else "review"
    return "ready"


def _target_for(group: dict[str, Any], targets: dict[str, str]) -> str:
    urls = [*group["urls"], *group["registeredUrls"]]
    for url in urls:
        for key in (normalize_source_url(url), url.lower()):
            if key in targets:
                return targets[key]
    for url in urls:
        if (host := _host_key(url)) in targets:
            return targets[host]
    return ""


def _index_rows(active: list[dict[str, Any]]) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    by_url: dict[str, list[int]] = defaultdict(list)
    by_host: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(active):
        for url in row_urls(row):
            by_url[normalize_source_url(url)].append(index)
            host = _host_key(url)
            if host and index not in by_host[host]:
                by_host[host].append(index)
    return by_url, by_host


def _exact(urls: list[str], by_url: dict[str, list[int]]) -> set[int]:
    return {index for url in urls for index in by_url.get(normalize_source_url(url), [])}


def _on_host(urls: list[str], by_host: dict[str, list[int]]) -> set[int]:
    return {index for url in urls for index in by_host.get(_host_key(url), [])}


def build_plan(
    state: dict[str, list[dict[str, Any]]],
    verdicts: list[dict[str, Any]],
    repoint_targets: dict[str, str],
) -> dict[str, Any]:
    active = state.get("active") or []
    by_url, by_host = _index_rows(active)
    groups = _verdict_groups(verdicts)
    actions: dict[int, dict[str, Any]] = {}
    unmatched: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    skipped = 0

    for name in sorted(groups, key=str.lower):
        group = groups[name]
        kind = _action_kind(group["kinds"])
        if kind == "skip":
            skipped += 1
            continue

        # Exact registered URL -> exact failing URL -> host. The registered URL comes
        # first because the failing URL can be a shared ATS host (ats.rippling.com).
        candidates = (
            _exact(group["registeredUrls"], by_url)
            or _exact(group["urls"], by_url)
            or _on_host([*group["registeredUrls"], *group["urls"]], by_host)
        )
        if not candidates:
            unmatched.append({"name": name, "urls": group["urls"], "kind": kind})
            continue
        if len(candidates) > 1:
            ambiguous.append(
                {
                    "name": name,
                    "urls": group["urls"],
                    "ids": sorted(_row_id(active[index]) for index in candidates),
                }
            )
            continue
        index = next(iter(candidates))
        row_id = _row_id(active[index])
        if not row_id:
            ambiguous.append({"name": name, "urls": group["urls"], "ids": ["<row has no id>"]})
            continue

        terminal = _terminal(group["evidence"])
        control = _control_ok(group["evidence"])
        existing = actions.get(index)
        if existing:
            existing["verdicts"].append(name)
            existing["failingUrls"] = sorted({*existing["failingUrls"], *group["urls"]})
            existing["terminal"] = existing["terminal"] or terminal
            existing["controlOk"] = existing["controlOk"] and control
            if PRIORITY[kind] > PRIORITY[existing["kind"]]:
                existing["kind"] = kind
                existing["target"] = ""
                existing["readiness"] = _readiness(
                    kind,
                    terminal=existing["terminal"],
                    control=existing["controlOk"],
                    target="",
                )
            continue

        target = _target_for(group, repoint_targets) if kind == "repoint" else ""
        hosts = {
            host for url in [*group["registeredUrls"], *group["urls"]] if (host := _host_key(url))
        }
        actions[index] = {
            "kind": kind,
            "verdicts": [name],
            "rowId": row_id,
            "studio": str(active[index].get("studio") or active[index].get("name") or ""),
            "host": min(hosts, default=""),
            "currentUrls": row_urls(active[index]),
            "failingUrls": group["urls"],
            "evidence": group["evidence"],
            "terminal": terminal,
            "controlOk": control,
            "target": target,
            "readiness": _readiness(kind, terminal=terminal, control=control, target=target),
        }

    ordered = sorted(actions.values(), key=lambda action: (action["kind"], action["host"]))
    retiring_ids = [
        action["rowId"]
        for action in ordered
        if action["kind"] in {"retire_board", "retire_seed"}
        and action["readiness"] in {"ready", "terminal-evidence"}
    ]
    last_source = last_source_guard(active, retiring_ids)
    last_source_ids = {row_id for flag in last_source["flags"] for row_id in flag["retireRowIds"]}
    for action in ordered:
        action["lastSource"] = action["rowId"] in last_source_ids

    counts = {
        "retire_board": sum(action["kind"] == "retire_board" for action in ordered),
        "retire_seed": sum(action["kind"] == "retire_seed" for action in ordered),
        "repoint": sum(action["kind"] == "repoint" for action in ordered),
        "repoint_ready": sum(
            action["kind"] == "repoint" and bool(action["target"]) for action in ordered
        ),
        "repoint_needs_target": sum(
            action["kind"] == "repoint" and not action["target"] for action in ordered
        ),
        "unmatched": len(unmatched),
        "ambiguous": len(ambiguous),
        "skipped": skipped,
    }
    counts["retire_total"] = counts["retire_board"] + counts["retire_seed"]
    # A seed rejection needs no extra evidence; a board retirement needs a terminal root
    # probe plus the control, so a merely unreachable host stays in review.
    counts["retire_applicable"] = sum(
        action["kind"] == "retire_seed"
        or (action["kind"] == "retire_board" and action["readiness"] == "terminal-evidence")
        for action in ordered
    )
    counts["last_source_groups"] = len(last_source["flags"])
    counts["last_source_retires"] = len(last_source_ids)
    return {
        "actions": ordered,
        "counts": counts,
        "lastSource": last_source,
        "unmatched": unmatched,
        "ambiguous": ambiguous,
        "verdictRows": len(verdicts),
        "verdictSources": len(groups),
    }


# -- output -----------------------------------------------------------------


def render(plan: dict[str, Any], *, data_dir: Path, authority: str) -> str:
    counts = plan["counts"]
    lines = [
        "registry cleanup plan (dry run)"
        if not plan.get("apply")
        else "registry cleanup plan (apply)",
        f"  data dir : {data_dir}",
        f"  authority: {authority}",
        f"  registry : {plan.get('registryCounts', {})}",
        f"  verdicts : {plan['verdictRows']} URLs / {plan['verdictSources']} source rows",
        (
            f"  actions  : retire {counts['retire_total']} "
            f"(board {counts['retire_board']}, seed {counts['retire_seed']}, "
            f"applicable {counts['retire_applicable']}), "
            f"repoint {counts['repoint']} "
            f"(ready {counts['repoint_ready']}, needs target {counts['repoint_needs_target']})"
        ),
        f"  unmatched: {counts['unmatched']}  ambiguous: {counts['ambiguous']}  skipped: {counts['skipped']}",
        (
            f"  last source: {counts['last_source_retires']} retire(s) in "
            f"{counts['last_source_groups']} group(s)"
        ),
        "",
    ]
    titles = {
        "retire_board": "RETIRE BOARD (host root and URL both gone)",
        "retire_seed": "RETIRE SEED (host alive, registered URL never worked)",
        "repoint": "REPOINT (host alive, URL drifted)",
    }
    for kind, title in titles.items():
        rows = [action for action in plan["actions"] if action["kind"] == kind]
        if not rows:
            continue
        lines.append(f"{title} - {len(rows)}")
        for action in rows:
            evidence = action["evidence"][0] if action["evidence"] else {}
            control = ""
            if kind == "retire_board":
                control = f" control={'ok' if action['controlOk'] else 'missing'}"
            marker = " LAST-SOURCE" if action.get("lastSource") else ""
            lines.append(
                f"  {action['host']:<38} {action['rowId'][:44]:<44} {action['studio'][:24]:<24} "
                f"[{action['readiness']}] root={evidence.get('rootStatus')} "
                f"url={evidence.get('urlStatus')}{control}{marker}"
            )
            lines.extend(f"      - {url}" for url in action["failingUrls"])
            if kind == "repoint":
                lines.append(f"      -> {action['target'] or '<needs --repoint-map target>'}")
        lines.append("")

    guard = plan.get("lastSource") or {}
    if guard.get("flags"):
        lines.append(
            "LAST SOURCE (studio/tenant loses its last active source) - "
            f"{len(guard['flags'])} groups / {counts['last_source_retires']} retires"
        )
        for flag in guard["flags"]:
            lines.append(
                f"  {flag['label'][:36]:<36} {flag['groupBasis']:<7} "
                f"active={flag['activeCount']} retire={flag['retireCount']} remaining=0"
            )
            lines.extend(f"      - {row_id}" for row_id in flag["retireRowIds"])
        lines.append("")

    for label, key in (("UNMATCHED", "unmatched"), ("AMBIGUOUS", "ambiguous")):
        if plan[key]:
            lines.append(f"{label} - {len(plan[key])}")
            lines.extend(
                f"  {row['name'][:70]}  {row.get('ids') or row.get('urls') or row.get('reason')}"
                for row in plan[key]
            )
            lines.append("")

    lines.append(
        "apply with: --apply --expect-retire "
        f"{counts['retire_applicable']} --expect-repoint {counts['repoint_ready']}"
    )
    return "\n".join(lines)


# -- apply ------------------------------------------------------------------


def write_backup(
    state: dict[str, list[dict[str, Any]]], plan: dict[str, Any], out_dir: Path
) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    directory = out_dir / f"backup-{stamp}"
    directory.mkdir(parents=True, exist_ok=False)
    for bucket, rows in state.items():
        (directory / f"{bucket}.json").write_text(
            json.dumps(rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    (directory / "plan.json").write_text(
        json.dumps(plan, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return directory


def _repoint_row(row: dict[str, Any], failing_urls: list[str], target: str) -> None:
    """Point the row at ``target``, replacing every page on the failing hosts.

    The failing URL can differ by ``www.`` or a scraped suffix, so pages match by host.
    When it names another host (an ATS posting), no page matches; the row still points
    at the old careers page, so it collapses to the target.
    """
    hosts = {host for url in failing_urls if (host := _host_key(url))}

    def stale(url: str) -> bool:
        return _host_key(url) in hosts

    pages: list[str] = []
    matched = False
    for url in row_urls(row):
        if stale(url):
            if not matched:
                pages.append(target)
                matched = True
            continue
        pages.append(url)
    row["pages"] = _dedupe(pages) if matched else [target]
    for key in ("listingUrl", "board_url", "url", "listing_url", "careersUrl"):
        value = str(row.get(key) or "")
        if value and (not matched or stale(value)):
            row[key] = target


def apply_plan(
    state: dict[str, list[dict[str, Any]]], plan: dict[str, Any]
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    active = [dict(row) for row in state.get("active") or []]
    rejected = [dict(row) for row in state.get("rejected") or []]
    by_id = {_row_id(row): index for index, row in enumerate(active)}
    stamp = datetime.now(UTC).isoformat()
    retiring: set[int] = set()
    applied: dict[str, Any] = {"repointed": [], "retired": []}

    for action in plan["actions"]:
        index = by_id[action["rowId"]]
        if action["kind"] == "repoint":
            if not action["target"]:
                continue
            _repoint_row(active[index], action["failingUrls"], action["target"])
            applied["repointed"].append((action["rowId"], action["target"]))
            continue
        if action["kind"] == "retire_board" and action["readiness"] != "terminal-evidence":
            continue
        if action["kind"] == "retire_board":
            rejected.append(
                transition_registry_to_retired(
                    active[index],
                    classification=RETIRE_TERMINAL_CLASSIFICATION,
                    probe={"terminal": action["terminal"], "control_ok": action["controlOk"]},
                    actor=ACTOR,
                    at=stamp,
                )
            )
        else:
            rejected.append(
                transition_registry_to_rejected(
                    active[index], reason=STALE_SEED_REASON, actor=ACTOR, at=stamp
                )
            )
        retiring.add(index)
        applied["retired"].append(action["rowId"])

    kept = [row for index, row in enumerate(active) if index not in retiring]
    return {"active": kept, "pending": state.get("pending") or [], "rejected": rejected}, applied


def verify(paths: dict[str, Path], applied: dict[str, Any]) -> list[str]:
    active = load_json_array(paths["active"], [])
    rejected = load_json_array(paths["rejected"], [])
    active_by_id = {_row_id(row): row for row in active}
    rejected_ids = {_row_id(row) for row in rejected}
    problems: list[str] = []
    for row_id, target in applied["repointed"]:
        row = active_by_id.get(row_id)
        if row is None:
            problems.append(f"{row_id}: repointed row missing from active")
        elif normalize_source_url(target) not in {
            normalize_source_url(url) for url in row_urls(row)
        }:
            problems.append(f"{row_id}: repoint target not present after write")
    for row_id in applied["retired"]:
        if row_id in active_by_id:
            problems.append(f"{row_id}: still active after retire")
        if row_id not in rejected_ids:
            problems.append(f"{row_id}: missing from rejected after retire")
    return problems


# -- cli --------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", required=True, type=Path, help="Baluffo data directory")
    parser.add_argument("--verdicts", type=Path, default=DEFAULT_VERDICTS)
    parser.add_argument("--repoint-map", type=Path, default=None)
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--apply", action="store_true", help="write the plan (default: dry run)")
    parser.add_argument("--expect-retire", type=int, default=None)
    parser.add_argument("--expect-repoint", type=int, default=None)
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=REPO_ROOT / "_out" / "registry-cleanup",
        help="where --apply writes the pre-change backup",
    )
    args = parser.parse_args(argv)

    paths = registry_paths(args.data_dir.expanduser().resolve())
    state = load_state(paths)
    if not args.verdicts.exists():
        raise SystemExit(f"no verdict artifact at {args.verdicts} - pass --verdicts")
    verdicts = json.loads(args.verdicts.read_text(encoding="utf-8"))
    if not isinstance(verdicts, list):
        raise SystemExit(f"{args.verdicts} must contain a JSON array of verdicts")

    plan = build_plan(state, verdicts, load_repoint_map(args.repoint_map))
    plan["registryCounts"] = {bucket: len(rows) for bucket, rows in state.items()}
    plan["apply"] = bool(args.apply)
    if args.json_out:
        args.json_out.write_text(
            json.dumps(plan, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    authority = "json (.json.gz)" if paths["active"].name.endswith(".gz") else "json (.json)"
    print(render(plan, data_dir=args.data_dir, authority=authority))

    if not args.apply:
        return 0

    counts = plan["counts"]
    if (
        args.expect_retire != counts["retire_applicable"]
        or args.expect_repoint != counts["repoint_ready"]
    ):
        print(
            f"refusing to apply: the plan has retire-applicable={counts['retire_applicable']} "
            f"(planned {counts['retire_total']}) repoint-ready={counts['repoint_ready']}; pass "
            f"--expect-retire {counts['retire_applicable']} --expect-repoint {counts['repoint_ready']}",
            file=sys.stderr,
        )
        return 2
    backup = write_backup(state, plan, args.backup_dir)
    result, applied = apply_plan(state, plan)
    save_registry_state_atomic(paths["active"], paths["pending"], paths["rejected"], result)
    print(f"backup: {backup}")
    problems = verify(paths, applied)
    if problems:
        print("verification FAILED:", *problems, sep="\n  ", file=sys.stderr)
        return 1
    print(
        f"applied: {len(applied['repointed'])} repoints, {len(applied['retired'])} retires "
        "- verification ok"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
