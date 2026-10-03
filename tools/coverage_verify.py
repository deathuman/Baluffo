#!/usr/bin/env python3
"""Decide whether a candidate board would actually collect anything.

`tools/coverage_boards.py` answers *which* board a missed opening belongs to. It
cannot answer whether registering that board would collect it, and the two answers
differ sharply: a board can resolve correctly and still yield zero rows because the
listing is a JS shell, the tenant was renamed, or the adapter needs a browser.

This runs the real per-adapter fetch for each candidate and reports what came back,
so registration is driven by evidence rather than by URL shape. It reuses the probe's
retry, control and tenant-keying rules rather than reimplementing them.

Verdicts are three-way on purpose. ``collects`` and ``empty`` are answers;
``unknown`` is the absence of one, and is never folded into either -- writing it as
``empty`` would file genuinely working boards as dead and quietly reintroduce the
gap this whole effort exists to close.

Usage:
  python tools/coverage_verify.py --boards _out/coverage/boards.json \\\
      --out _out/coverage/verified.json --limit 40
  python tools/coverage_verify.py --boards ... --adapter workday --out ...
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _load(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if not spec or not spec.loader:  # pragma: no cover - import guard
        raise SystemExit(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


probe = _load("coverage_probe", Path(__file__).resolve().parent / "coverage_probe.py")

VERDICT_COLLECTS = "collects"
VERDICT_EMPTY = "empty"
VERDICT_UNKNOWN = "unknown"

# A control board per adapter, so "the fetcher works" is proven per vendor rather
# than once. A single control on one vendor cannot tell a dead board from a broken
# fetcher, which is how a whole vendor's backlog gets filed as boards with no work.
#
# Every one of these was picked by fetching it and confirming the row count, not by
# looking plausible. The first draft of this table had three dead URLs in it, and a
# dead control silently marks every board on that adapter `unknown`.
ADAPTER_CONTROL: dict[str, str] = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/grover/jobs?content=true",
    "lever": "https://api.lever.co/v0/postings/animocabrands?mode=json",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/ramp",
    "smartrecruiters": (
        "https://api.smartrecruiters.com/v1/companies/SmartRecruiters/postings?limit=100"
    ),
}

# Workday is not probed over HTTP here. Its CXS endpoint is a POST to
# /wday/cxs/<tenant>/<site>/jobs and needs a certifi-anchored TLS context, because
# the OS cert store poisons chain building for *.myworkdayjobs.com. The runtime
# already implements both; asking a GET probe to judge it produces the worst
# possible answer, which it did: the GET hits a
# {"widget":"redirect","externalSpa":true} body, which is one JSON object, and the
# naive row count then reported every Workday board as `collects` with 1 row.
#
# These are therefore verified by the real runner, not reimplemented here. Reusing
# production code is also what keeps this tool honest about what the pipeline can
# actually fetch.
STRUCTURED_ADAPTERS = ("workday",)

# Where each adapter's board data lives, keyed by the id fields coverage_boards emits.
LIST_URL_FIELD = {
    "greenhouse": "slug",
    "lever": "account",
    "ashby": "board_url",
    "smartrecruiters": "api_url",
    "recruitee": "api_url",
    "personio": "feed_url",
    "pinpoint": "api_url",
    "bamboohr": "listing_url",
    "breezy": "board_url",
    "jazzhr": "board_url",
    "teamtailor": "listing_url",
    "workable": "account",
    "workday": "listing_url",
    "static": "listing_url",
}


def list_url_for(candidate: Mapping[str, Any]) -> str:
    """The endpoint the runtime would read for this board, built from the emitted row.

    Deliberately derived from the candidate's own fields rather than the original
    job URL: if the row the registry would hold cannot be turned into a fetch
    target, that row would collect nothing and must not be proposed.
    """
    adapter = str(candidate.get("adapter") or "")
    field = LIST_URL_FIELD.get(adapter)
    if not field:
        return ""
    if adapter == "greenhouse":
        slug = candidate.get("slug") or ""
        return (
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true" if slug else ""
        )
    if adapter == "lever":
        account = candidate.get("account") or ""
        return f"https://api.lever.co/v0/postings/{account}?mode=json" if account else ""
    if adapter == "ashby":
        url = str(candidate.get("board_url") or "")
        tenant = url.rstrip("/").rsplit("/", 1)[-1] if url else ""
        return f"https://api.ashbyhq.com/posting-api/job-board/{tenant}" if tenant else ""
    if adapter == "workable":
        account = candidate.get("account") or ""
        return f"https://apply.workable.com/api/v1/accounts/{account}/jobs" if account else ""
    return str(candidate.get(field) or "")


def count_rows(payload: str) -> int | None:
    """Rows in a vendor payload, or None when the body is not JSON we recognise."""
    import json as _json

    try:
        data = _json.loads(payload)
    except Exception:
        return None
    return _row_count(data)


# Payload keys that actually hold listing rows. Anything outside this set is
# envelope, not data.
_ROW_KEYS = ("jobs", "postings", "results", "offers", "items", "content", "jobPostings")


def _row_count(data: Any) -> int:
    """Rows in a vendor payload, or None when the shape is not recognised.

    Returning None rather than a guess is the whole point. The first version fell
    back to "any non-empty dict is one row", which turned Workday's SPA redirect
    stub -- {"widget":"redirect","externalSpa":true} -- into `collects, 1 row` for
    every Workday board on the list. An unrecognised shape is unknown, not one row.
    """
    if isinstance(data, list):
        return len(data)
    if not isinstance(data, dict):
        return None
    for key in _ROW_KEYS:
        value = data.get(key)
        if isinstance(value, list):
            return len(value)
    # Workday's CXS returns {"total": N, "jobPostings": [...]}, which the loop above
    # catches. A dict with a numeric total and no list is still a listing envelope.
    if isinstance(data.get("total"), int):
        return int(data["total"])
    # An empty envelope is a real answer: the endpoint replied with a valid, empty
    # listing. A *non-empty* dict with no recognised keys is not -- that is the
    # redirect stub case, and guessing there is what invented coverage.
    return 0 if not data else None


def verify_structured_candidate(
    candidate: Mapping[str, Any], *, timeout: float = 40.0
) -> dict[str, Any]:
    """Verify a board through the real production runner.

    Workday's fetch is a POST to a CXS endpoint behind a certifi-anchored TLS
    context, because the OS cert store poisons chain building for
    *.myworkdayjobs.com. A GET probe cannot see it, and when it guessed it reported
    every Workday board as collecting.

    Rather than reimplement that -- which would be a second, silently divergent
    copy of the rule -- this calls the runtime's own helper for the page payload and
    only falls back to `unknown` if it is unavailable. The measurement question is
    "would registering this board collect anything", and the production path is the
    only thing that answers it.
    """
    adapter = str(candidate.get("adapter") or "")
    board_id = str(candidate.get("id") or "")
    base = {
        "id": board_id,
        "adapter": adapter,
        "status": 0,
        "rows": 0,
        "url": str(candidate.get("listing_url") or ""),
    }
    try:
        from src.jobs.adapters import provider_structured_listing as psl
    except Exception as exc:  # pragma: no cover - import guard
        return {
            **base,
            "verdict": VERDICT_UNKNOWN,
            "reason": f"production structured adapter unavailable: {type(exc).__name__}",
        }

    fetch = getattr(psl, "_fetch_workday_cxs_page", None)
    config = getattr(psl, "_workday_cxs_config", None)
    if not callable(fetch) or not callable(config):  # pragma: no cover - shape guard
        return {**base, "verdict": VERDICT_UNKNOWN, "reason": "structured adapter shape changed"}

    endpoint, _site, _query = config(base["url"])
    if not endpoint:
        return {**base, "verdict": VERDICT_UNKNOWN, "reason": "no CXS endpoint for this board"}
    try:
        payload = fetch(
            endpoint=endpoint,
            payload={"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""},
            timeout_s=int(timeout),
            retries=2,
            backoff_s=1.0,
        )
    except Exception as exc:
        # An unreachable board is unknown, never empty: "empty" is a claim that the
        # board has no openings, and a failed fetch is not evidence of that.
        return {
            **base,
            "verdict": VERDICT_UNKNOWN,
            "reason": f"CXS fetch failed: {type(exc).__name__}",
        }

    rows = _row_count(payload)
    if not rows:
        return {**base, "verdict": VERDICT_UNKNOWN, "reason": "CXS returned no listing envelope"}
    verdict = VERDICT_COLLECTS if rows else VERDICT_EMPTY
    return {**base, "verdict": verdict, "reason": f"CXS reports {rows} openings", "rows": rows}


def verify_candidate(candidate: Mapping[str, Any], *, timeout: float = 40.0) -> dict[str, Any]:
    """Fetch one candidate board and record what came back."""
    adapter = str(candidate.get("adapter") or "")
    if adapter in STRUCTURED_ADAPTERS:
        return verify_structured_candidate(candidate, timeout=timeout)
    url = list_url_for(candidate)
    if not url:
        return {
            "id": candidate.get("id"),
            "adapter": adapter,
            "verdict": VERDICT_UNKNOWN,
            "reason": "no list endpoint could be derived from the registry row",
            "status": 0,
            "rows": 0,
        }
    status, payload = probe.http_get(url, timeout=timeout)
    if status != 200:
        return {
            "id": candidate.get("id"),
            "adapter": adapter,
            "verdict": VERDICT_UNKNOWN,
            "reason": f"list endpoint returned HTTP {status}",
            "status": status,
            "rows": 0,
        }
    rows = count_rows(payload)
    if rows is None:
        verdict, reason = (
            VERDICT_UNKNOWN,
            f"list endpoint returned {len(payload)} bytes in an unrecognised shape",
        )
    elif rows:
        verdict, reason = VERDICT_COLLECTS, f"list endpoint returned {rows} rows"
    elif len(payload.strip()) < 200:
        verdict, reason = VERDICT_EMPTY, "list endpoint answered with an empty body"
    else:
        # A substantial body that yields no rows is a shape we failed to parse, not
        # an empty board. Filing it as empty would hide a working board.
        verdict, reason = (
            VERDICT_UNKNOWN,
            f"list endpoint returned {len(payload)} bytes but no recognisable rows",
        )
    return {
        "id": candidate.get("id"),
        "adapter": adapter,
        "verdict": verdict,
        "reason": reason,
        "status": status,
        "rows": rows,
        "url": url,
    }


def run_adapter_controls(*, timeout: float = 40.0) -> dict[str, dict[str, Any]]:
    """Prove each adapter's fetcher works before trusting its empty verdicts.

    Without this, a vendor whose API changed shape is indistinguishable from a
    vendor of dead boards, and the fix (writing an adapter) never gets made.
    """
    out: dict[str, dict[str, Any]] = {}
    for adapter, url in ADAPTER_CONTROL.items():
        status, payload = probe.http_get(url, timeout=timeout)
        rows = count_rows(payload) if status == 200 else None
        out[adapter] = {
            "ok": status == 200 and bool(rows),
            "status": status,
            "rows": rows or 0,
            "url": url,
            "detail": f"control {url} -> HTTP {status}, {rows if rows is not None else 'unparsed'} rows",
        }
    return out


def verify_candidates(
    candidates: Sequence[Mapping[str, Any]],
    *,
    timeout: float = 40.0,
    controls: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Verify each candidate, downgrading verdicts when that adapter's control failed.

    A board whose adapter control is broken gets ``unknown``, not ``empty``: the
    fetch never proved anything about the board.
    """
    results: list[dict[str, Any]] = []
    for candidate in candidates:
        adapter = str(candidate.get("adapter") or "")
        control = (controls or {}).get(adapter)
        if control is not None and not control.get("ok"):
            results.append(
                {
                    "id": candidate.get("id"),
                    "adapter": adapter,
                    "verdict": VERDICT_UNKNOWN,
                    "reason": f"adapter control failed ({control.get('detail')}), so this proves nothing",
                    "status": 0,
                    "rows": 0,
                }
            )
            continue
        results.append(verify_candidate(candidate, timeout=timeout))
    return results


def summarise(
    results: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Verdict counts, and the openings each verdict stands to recover.

    Openings, not boards, is the number that matters: one board with 176 openings and
    one with a single opening are not comparable units of value.
    """
    missing_by_id = {str(c.get("id")): int(c.get("missingCount") or 0) for c in (candidates or [])}
    by_verdict: dict[str, int] = {}
    rows_by_verdict: dict[str, int] = {}
    openings_by_verdict: dict[str, int] = {}
    for row in results:
        verdict = str(row.get("verdict"))
        by_verdict[verdict] = by_verdict.get(verdict, 0) + 1
        rows_by_verdict[verdict] = rows_by_verdict.get(verdict, 0) + int(row.get("rows") or 0)
        openings_by_verdict[verdict] = openings_by_verdict.get(verdict, 0) + missing_by_id.get(
            str(row.get("id")), 0
        )
    return {
        "boards": len(results),
        "byVerdict": by_verdict,
        "rowsByVerdict": rows_by_verdict,
        "openingsByVerdict": openings_by_verdict,
    }


def render(results: Sequence[Mapping[str, Any]], *, limit: int = 40) -> str:
    lines = [
        f"boards verified: {len(results)}",
        f"  collects : {sum(1 for r in results if r.get('verdict') == VERDICT_COLLECTS)}",
        f"  empty    : {sum(1 for r in results if r.get('verdict') == VERDICT_EMPTY)}",
        f"  unknown  : {sum(1 for r in results if r.get('verdict') == VERDICT_UNKNOWN)}",
        "",
        f"{'verdict':<10} {'rows':>7} {'adapter':<16} id",
        "-" * 108,
    ]
    for row in results[:limit]:
        lines.append(
            f"{str(row.get('verdict')):<10} {row.get('rows', 0):>7} "
            f"{str(row.get('adapter')):<16} {str(row.get('id'))[:58]}"
        )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--boards", required=True, type=Path, help="boards.json from coverage_boards"
    )
    parser.add_argument("--out", type=Path, help="where to write verified.json")
    parser.add_argument("--adapter", help="only verify this adapter")
    parser.add_argument("--verdict", help="only keep this verdict")
    parser.add_argument("--limit", type=int, help="verify at most this many boards")
    parser.add_argument("--timeout", type=float, default=40.0)
    parser.add_argument("--show", type=int, default=40)
    args = parser.parse_args(argv)

    payload = json.loads(args.boards.read_text(encoding="utf-8"))
    candidates = [
        c for c in (payload.get("candidates") or []) if c.get("status") == "new_candidate"
    ]
    if args.adapter:
        candidates = [c for c in candidates if str(c.get("adapter")) == args.adapter]
    candidates.sort(key=lambda c: -int(c.get("missingCount") or 0))
    if args.limit:
        candidates = candidates[: args.limit]

    controls = run_adapter_controls(timeout=args.timeout)
    broken = sorted(name for name, control in controls.items() if not control["ok"])
    if broken:
        print(
            f"adapter controls failed: {', '.join(broken)} -- their boards report 'unknown', not 'empty'"
        )

    results = verify_candidates(candidates, timeout=args.timeout, controls=controls)
    if args.verdict:
        results = [r for r in results if r.get("verdict") == args.verdict]

    out = {"controls": controls, "results": results, "summary": summarise(results, candidates)}
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(render(results, limit=args.show))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
