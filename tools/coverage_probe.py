#!/usr/bin/env python3
"""Decide whether a coverage-audit miss is a live gap or a delisted listing.

``tools/coverage_audit.py`` reports that Baluffo lacks a role. It cannot say
whether that role still exists, and the two demand opposite responses: a live gap
means register the board, a delisted listing means do nothing. Guessing wrong
either wastes an afternoon or, worse, registers boards for jobs that are gone.

The evidence rule is deliberately conservative, because the obvious check is
wrong. A job's detail page usually stays fetchable long after the posting closes,
so "the detail page still resolves" proves nothing. This probes the board's
*listing* and looks for the role there:

  live           the role is present in the board's current listing
  delisted       the board answers and lists roles, but not this one
  board_dead     the board itself does not answer
  inconclusive   the probe could not decide; needs a human

Per the repo guardrail, a dead verdict is never taken from one endpoint. Each
board is probed at its root *and* at its adapter's list API where one is known,
and a known-good control runs first so a probe that is silently broken (a 403, a
WAF, a certifi-less urllib) is visible as a failed control rather than as 21 dead
boards.

Output is written as {source_url: bool} for ``coverage_audit.py --live-probe``,
plus a richer per-job report.

Usage:
  python tools/coverage_probe.py --report _out/coverage/report.json \\
      --out _out/coverage/probe.json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "coverage_audit", _ROOT / "tools" / "coverage_audit.py"
)
if not _spec or not _spec.loader:  # pragma: no cover - import guard
    raise SystemExit("coverage_audit.py not found next to coverage_probe.py")
_audit = importlib.util.module_from_spec(_spec)
sys.modules["coverage_audit"] = _audit
_spec.loader.exec_module(_audit)
normalize_token = _audit.normalize_token

try:  # certifi-anchored TLS: bare urllib fails hosts the pipeline fetches fine.
    import certifi

    _SSL_CTX: ssl.SSLContext = ssl.create_default_context(cafile=certifi.where())
except Exception:  # pragma: no cover - fallback only
    _SSL_CTX = ssl.create_default_context()

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

VERDICT_LIVE = "live"
VERDICT_DELISTED = "delisted"
VERDICT_BOARD_DEAD = "board_dead"
VERDICT_INCONCLUSIVE = "inconclusive"

# Must be a board that is definitely alive. If the control fails, every other
# verdict is untrustworthy, so the run stops rather than reporting noise.
CONTROL_URL = "https://api.ashbyhq.com/posting-api/job-board/thatgamecompany"
CONTROL_EXPECTED_TOKEN = "3dcharacterartist"


def http_get(url: str, *, timeout: float = 40.0, attempts: int = 3) -> tuple[int, str]:
    """GET with retries.

    Without this the verdicts are not reproducible: a single flaky board root made
    ``listing_ok`` toggle between runs, and on a two-role board that flipped one
    role to ``delisted`` and the other to ``inconclusive``, then swapped on the
    next run. Six of 26 verdicts were unstable across three identical runs.
    """
    last: tuple[int, str] = (0, "")
    for attempt in range(attempts):
        request = urllib.request.Request(
            url, headers={"User-Agent": USER_AGENT, "Accept": "application/json,text/html,*/*"}
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout, context=_SSL_CTX) as response:
                return int(response.status), response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            last = (int(exc.code), "")
            # 4xx is an answer, not a glitch: retrying just wastes requests.
            if 400 <= int(exc.code) < 500:
                return last
        except Exception:
            last = (0, "")
        if attempt + 1 < attempts:
            time.sleep(1.5 * (attempt + 1))
    return last


def host_of(url: str) -> str:
    """Host of a URL, lowercased and without a leading ``www.``."""
    parsed = urlparse(url if "//" in str(url or "") else f"//{url}")
    return (parsed.netloc or "").lower().removeprefix("www.")


def board_root(url: str) -> str:
    """The board's own root, so a 404 on a job path is not read as a dead board."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/"


def list_api_candidates(url: str) -> list[str]:
    """Adapter list endpoints for a job URL, where the vendor exposes one."""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    segments = [s for s in parsed.path.split("/") if s]
    out: list[str] = []

    if host.endswith(".recruitee.com"):
        out.append(f"{parsed.scheme}://{host}/api/offers/")
    elif host == "job-boards.greenhouse.io" and segments:
        out.append(f"https://boards-api.greenhouse.io/v1/boards/{segments[0]}/jobs?content=true")
    elif host == "jobs.lever.co" and segments:
        out.append(f"https://api.lever.co/v0/postings/{segments[0]}?mode=json")
    elif host == "jobs.ashbyhq.com" and segments:
        out.append(f"https://api.ashbyhq.com/posting-api/job-board/{segments[0]}")
    elif host == "jobs.smartrecruiters.com" and segments:
        org = re.sub(r"[^A-Za-z0-9]", "", segments[0])
        if org:
            out.append(f"https://api.smartrecruiters.com/v1/companies/{org}/postings?limit=100")
    elif host.endswith(".personio.com") or host.endswith(".personio.de"):
        out.append(f"{parsed.scheme}://{host}/xml")
    return out


def run_control() -> tuple[bool, str]:
    """Prove the probe can see a known-live board before trusting any verdict."""
    status, body = http_get(CONTROL_URL)
    ok = status == 200 and CONTROL_EXPECTED_TOKEN in normalize_token(body)
    return ok, f"control {CONTROL_URL} -> HTTP {status}"


def title_variants(title: str) -> set[str]:
    """Match on the full title and on its distinctive head.

    Boards append noise to titles ("Technical Artist (m/f/d)", "Technical Artist -
    Shaders"), so a whole-title comparison misses live roles.
    """
    full = normalize_token(title)
    variants = {full}
    head = re.split(r"[-–—|/,]", title)[0]
    if len(normalize_token(head)) >= 8:
        variants.add(normalize_token(head))
    return {v for v in variants if len(v) >= 8}


def probe_board(url: str, *, timeout: float = 40.0) -> dict[str, Any]:
    """Collect every listing view of a board and whether the board answered."""
    views: list[dict[str, Any]] = []
    root = board_root(url)
    status, body = http_get(root, timeout=timeout)
    views.append({"kind": "root", "url": root, "status": status, "len": len(body)})
    listing_text = body if status == 200 else ""
    listing_ok = status == 200 and len(normalize_token(body)) > 200

    for api in list_api_candidates(url):
        api_status, api_body = http_get(api, timeout=timeout)
        views.append({"kind": "list_api", "url": api, "status": api_status, "len": len(api_body)})
        if api_status == 200 and api_body:
            listing_text += api_body
            listing_ok = True

    detail_status, detail_body = http_get(url, timeout=timeout)
    views.append({"kind": "detail", "url": url, "status": detail_status, "len": len(detail_body)})

    return {
        "root": root,
        "views": views,
        "listing_ok": listing_ok,
        "listing_text": normalize_token(listing_text),
        "detail_status": detail_status,
        "detail_text": normalize_token(detail_body),
    }


def probe_job(job: Mapping[str, Any], board: Mapping[str, Any]) -> dict[str, Any]:
    title = str(job.get("title") or "")
    variants = title_variants(title)
    url = str(job.get("sourceUrl") or "")

    on_listing = any(v in board["listing_text"] for v in variants)
    on_detail = any(v in board["detail_text"] for v in variants)

    if on_listing:
        verdict, why = VERDICT_LIVE, "role present in the board's current listing"
    elif not board["listing_ok"]:
        # The board did not give us a listing to search, so "absent" proves nothing.
        verdict, why = VERDICT_INCONCLUSIVE, "board did not return a readable listing"
    elif on_detail:
        # Exactly the trap: detail pages outlive postings.
        verdict, why = (
            VERDICT_INCONCLUSIVE,
            (
                "role is on the detail page but absent from the listing; "
                "fetchable detail pages outlive closed postings"
            ),
        )
    else:
        verdict, why = VERDICT_DELISTED, "board lists roles but not this one"

    return {
        "sourceUrl": url,
        "title": title,
        "company": job.get("company"),
        "ats": job.get("ats"),
        "bucket": job.get("bucket"),
        "verdict": verdict,
        "reason": why,
        "onListing": on_listing,
        "onDetail": on_detail,
    }


def board_key(url: str) -> str:
    """Identity of the *board*, not the host.

    Multi-tenant ATS hosts serve unrelated companies side by side, so grouping by
    host alone silently merges them: jobs.smartrecruiters.com carries both
    CDPROJEKTRED and YggdrasilSandbox, and probing the first tenant's list API for
    the second reported the role as absent from a listing that was never fetched.
    Where a vendor exposes a per-tenant list API, that API *is* the board id.
    Otherwise the host root is right, which keeps plain career pages to one fetch.
    """
    apis = list_api_candidates(url)
    if apis:
        return apis[0]
    return board_root(url)


def group_jobs(misses: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for job in misses:
        url = str(job.get("sourceUrl") or "")
        if not url:
            continue
        groups.setdefault(board_key(url), []).append(job)
    return groups


def probe_report(report: Mapping[str, Any], *, timeout: float = 40.0) -> dict[str, Any]:
    control_ok, control_detail = run_control()
    if not control_ok:
        return {
            "aborted": True,
            "control": {"ok": False, "detail": control_detail},
            "note": (
                "The known-good control failed, so every board verdict below would be "
                "untrustworthy. Fix connectivity or the TLS/UA setup and re-run."
            ),
            "liveBoards": {},
            "jobs": [],
        }

    results: list[dict[str, Any]] = []
    boards: dict[str, Any] = {}
    for key, jobs in group_jobs(report.get("misses") or []).items():
        first = jobs[0]
        board = probe_board(str(first.get("sourceUrl") or ""), timeout=timeout)
        boards[key] = {"listing_ok": board["listing_ok"], "views": board["views"]}
        for job in jobs:
            results.append(probe_job(job, board))

    # Only definitive verdicts become booleans. An inconclusive probe is omitted
    # rather than written as False, because coverage_audit reads False as
    # "role_not_on_board" and would then report an undecided role as delisted.
    live_boards = {
        row["sourceUrl"]: row["verdict"] == VERDICT_LIVE
        for row in results
        if row["verdict"] in {VERDICT_LIVE, VERDICT_DELISTED}
    }
    counts: dict[str, int] = {}
    for row in results:
        counts[row["verdict"]] = counts.get(row["verdict"], 0) + 1
    return {
        "aborted": False,
        "control": {"ok": True, "detail": control_detail},
        "verdictCounts": counts,
        "boards": boards,
        "jobs": sorted(results, key=lambda row: (row["verdict"], str(row["company"]))),
        "liveBoards": live_boards,
    }


def render_summary(result: Mapping[str, Any]) -> str:
    if result.get("aborted"):
        return f"ABORTED: {result['control']['detail']}\n{result.get('note', '')}"
    lines = [f"control: {result['control']['detail']} (ok)", "", "verdicts:"]
    for verdict, count in sorted(result["verdictCounts"].items()):
        lines.append(f"  {count:>4}  {verdict}")
    lines.append("")
    for row in result["jobs"]:
        lines.append(
            f"[{row['verdict']:<12}] {str(row['company'])[:24]:<26} {str(row['title'])[:40]:<42}"
        )
        lines.append(f"{'':<15}{row['reason']}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", required=True, type=Path, help="coverage_audit report.json")
    parser.add_argument("--out", required=True, type=Path, help="where to write probe.json")
    parser.add_argument("--timeout", type=float, default=40.0)
    args = parser.parse_args(argv)

    report = json.loads(args.report.read_text(encoding="utf-8"))
    result = probe_report(report, timeout=args.timeout)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    args.out.with_name("live-boards.json").write_text(
        json.dumps(result["liveBoards"], indent=2), encoding="utf-8"
    )
    print(render_summary(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
