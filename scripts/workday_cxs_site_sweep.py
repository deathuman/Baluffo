#!/usr/bin/env python3
"""Bounded Workday CXS site-id sweep (read-only evidence tool).

POSTs /wday/cxs/{tenant}/{site}/jobs for each candidate site id on one Workday
tenant and classifies the result, mirroring the endpoint shape and TLS behavior
of the workday_sources adapter (certifi-anchored verified context for
*.myworkdayjobs.com, no-redirect POST handling, maintenance-303 surfaced as a
classified status). Pure stdlib + certifi; no src/ imports.

Classifications:
  populated              JSON total > 0 (site id valid and board has postings)
  configured_empty       JSON total == 0 (site id valid, board has no postings)
  not_found              HTTP 404 (site id does not exist on this tenant)
  blocked_or_challenge   HTTP 403/429 (rate limit / WAF)
  redirect               3xx (maintenance redirect or canonical move)
  http_error_<status>    other HTTP error (500, 503, ...)
  error                  network / TLS / unexpected failure
  unexpected_json        HTTP 200 whose body is not a CXS JSON payload

Usage:
  python scripts/workday_cxs_site_sweep.py --host tencent.wd1.myworkdayjobs.com \
      --sites timi_careers timi --control-sites lightspeed \
      --out-dir _out/timi-workday-site-sweep-2026-09-05

Optionally add --probe-page-titles to GET each candidate's shell page and
extract the og:title for human-readable confirmation.
"""

from __future__ import annotations

import argparse
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    import certifi
except ImportError:  # pragma: no cover - certifi is a runtime dependency of the app
    certifi = None

WORKDAY_TLS_HOST_SUFFIX = ".myworkdayjobs.com"
CXS_LIST_SEGMENT = "jobs"
DEFAULT_LIMIT = 20
REQUEST_GAP_S = 1.0

CLASSIFICATION_ORDER = (
    "populated",
    "configured_empty",
    "not_found",
    "blocked_or_challenge",
    "redirect",
    "http_error",
    "unexpected_json",
    "error",
)

_OG_TITLE_RE = re.compile(
    r"<meta[^>]+?(?:"
    r"property=[\"']og:title[\"'][^>]*?content=[\"']([^\"']{0,200})[\"']"
    r"|content=[\"']([^\"']{0,200})[\"'][^>]*?property=[\"']og:title[\"']"
    r")[^>]*?>",
    re.IGNORECASE,
)


def is_workday_tls_host(endpoint: str) -> bool:
    host = (urlparse(str(endpoint or "")).hostname or "").lower()
    return host.endswith(WORKDAY_TLS_HOST_SUFFIX)


def build_tls_context() -> ssl.SSLContext | None:
    """Certifi-anchored verified context, mirroring the adapter's T5 fix.

    Returns None when certifi is unavailable or the context cannot be built; the
    caller then falls back to urllib's default verified context. Never downgrades
    verification.
    """
    if certifi is None:
        return None
    try:
        return ssl.create_default_context(cafile=certifi.where())
    except (OSError, ssl.SSLError):
        return None


def cxs_endpoint(host: str, site: str) -> str:
    tenant = host.split(".", 1)[0]
    return f"https://{host}/wday/cxs/{tenant}/{site}/{CXS_LIST_SEGMENT}"


def cxs_payload() -> dict[str, Any]:
    return {"limit": DEFAULT_LIMIT, "offset": 0}


def classify_http_status(status: int) -> str:
    if status == 200:
        return "json_response"
    if status in (301, 302, 303, 307, 308):
        return "redirect"
    if status in (403, 429):
        return "blocked_or_challenge"
    if status == 404:
        return "not_found"
    return "http_error"


def extract_site_title(text: str) -> str:
    match = _OG_TITLE_RE.search(str(text or ""))
    if match is None:
        return ""
    raw = next((group for group in match.groups() if group), "")
    return unescape(raw).strip()


def classify_cxs_json(payload: Any) -> str:
    total: Any = None
    if isinstance(payload, dict):
        total = payload.get("total")
        if not isinstance(total, (int, float)):
            count = payload.get("count")
            total = count if isinstance(count, (int, float)) else None
    if isinstance(total, (int, float)):
        return "populated" if total > 0 else "configured_empty"
    return "unexpected_json"


def extract_total(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None
    for key in ("total", "count"):
        value = payload.get(key)
        if isinstance(value, (int, float)):
            return int(value)
    return None


def extract_sample_titles(payload: Any, cap: int = 5) -> list[str]:
    """First N job titles from a CXS jobs payload, for games-facing judgment."""
    if not isinstance(payload, dict):
        return []
    jobs = payload.get("jobPostings")
    if not isinstance(jobs, list):
        return []
    titles: list[str] = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        title = str(job.get("title") or "").strip()
        if title:
            titles.append(title)
        if len(titles) >= cap:
            break
    return titles


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """CXS POSTs must never be silently followed (mirror of adapter behavior).

    A 303 carries no re-POSTable body; Workday's maintenance redirect would
    otherwise land on an HTML page and surface as a confusing JSON error.
    """

    def redirect_request(
        self, req: Any, fp: Any, code: Any, msg: Any, headers: Any, newurl: Any
    ) -> None:
        return None


def _build_post_opener(endpoint: str) -> urllib.request.OpenerDirector:
    handlers: list[Any] = [_NoRedirectHandler()]
    tls_context = build_tls_context() if is_workday_tls_host(endpoint) else None
    if tls_context is not None:
        handlers.append(urllib.request.HTTPSHandler(context=tls_context))
    return urllib.request.build_opener(*handlers)


def probe_site(host: str, site: str, *, timeout_s: int = 20) -> dict[str, Any]:
    """POST the CXS jobs endpoint for one candidate site id."""
    endpoint = cxs_endpoint(host, site)
    row: dict[str, Any] = {
        "site": site,
        "endpoint": endpoint,
        "classification": "error",
        "httpStatus": None,
        "total": None,
        "sampleTitles": [],
        "error": "",
    }
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(cxs_payload()).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        method="POST",
    )
    try:
        with _build_post_opener(endpoint).open(request, timeout=timeout_s) as response:
            status = int(response.status)
            text = response.read().decode("utf-8", errors="replace")
        row["httpStatus"] = status
        kind = classify_http_status(status)
        if kind != "json_response":
            row["classification"] = kind if kind != "http_error" else f"http_error_{status}"
            return row
        try:
            payload = json.loads(text) if text.strip() else {}
        except json.JSONDecodeError:
            row["classification"] = "unexpected_json"
            row["error"] = "200 response was not valid JSON"
            return row
        row["classification"] = classify_cxs_json(payload)
        row["total"] = extract_total(payload)
        row["sampleTitles"] = extract_sample_titles(payload)
        return row
    except urllib.error.HTTPError as exc:
        row["httpStatus"] = int(exc.code)
        kind = classify_http_status(exc.code)
        row["classification"] = kind if kind != "http_error" else f"http_error_{exc.code}"
        return row
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        row["classification"] = "error"
        row["error"] = str(exc)
        return row


def probe_site_page(host: str, site: str, *, timeout_s: int = 20) -> dict[str, Any]:
    """GET the site shell page for og:title confirmation (best-effort).

    Redirects are followed so canonical locale-prefixed moves are visible.
    """
    url = f"https://{host}/{site}"
    row: dict[str, Any] = {
        "site": site,
        "pageUrl": url,
        "httpStatus": None,
        "finalUrl": "",
        "ogTitle": "",
        "error": "",
    }
    handlers: list[Any] = []
    tls_context = build_tls_context() if is_workday_tls_host(url) else None
    if tls_context is not None:
        handlers.append(urllib.request.HTTPSHandler(context=tls_context))
    opener = urllib.request.build_opener(*handlers)
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with opener.open(request, timeout=timeout_s) as response:
            row["httpStatus"] = int(response.status)
            row["finalUrl"] = str(response.geturl())
            row["ogTitle"] = extract_site_title(response.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        row["httpStatus"] = int(exc.code)
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        row["error"] = str(exc)
    return row


def summarize(results: list[dict[str, Any]]) -> dict[str, int]:
    counts = {key: 0 for key in CLASSIFICATION_ORDER}
    for row in results:
        classification = str(row.get("classification") or "error")
        if classification.startswith("http_error_"):
            classification = "http_error"
        counts[classification] = counts.get(classification, 0) + 1
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bounded Workday CXS site-id sweep")
    parser.add_argument(
        "--host", required=True, help="Workday tenant host, e.g. tencent.wd1.myworkdayjobs.com"
    )
    parser.add_argument("--sites", nargs="+", required=True, help="candidate site ids")
    parser.add_argument(
        "--control-sites",
        nargs="*",
        default=[],
        help="known-classified site ids used to validate the probe method",
    )
    parser.add_argument("--out-dir", required=True, help="evidence output directory")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument(
        "--probe-page-titles",
        action="store_true",
        help="also GET each candidate's shell page for og:title confirmation",
    )
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    sites = [*args.control_sites, *args.sites]
    control_set = set(args.control_sites)
    for index, site in enumerate(sites):
        row = probe_site(args.host, site, timeout_s=args.timeout)
        row["isControl"] = site in control_set
        results.append(row)
        line = f"[sweep] {site}: {row['classification']}"
        if row.get("total") is not None:
            line += f" total={row['total']}"
        if row.get("sampleTitles"):
            line += f" titles={row['sampleTitles'][:3]}"
        if row.get("httpStatus") is not None:
            line += f" status={row['httpStatus']}"
        if row.get("error"):
            line += f" error={row['error']}"
        print(line, flush=True)
        if index < len(sites) - 1:
            time.sleep(REQUEST_GAP_S)

    page_results: list[dict[str, Any]] = []
    if args.probe_page_titles:
        for index, site in enumerate(args.sites):
            page_results.append(probe_site_page(args.host, site, timeout_s=args.timeout))
            if index < len(args.sites) - 1:
                time.sleep(REQUEST_GAP_S)

    summary: dict[str, Any] = {
        "host": args.host,
        "endpointShape": f"/wday/cxs/{{tenant}}/{{site}}/{CXS_LIST_SEGMENT} (POST)",
        "probedAtUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "candidateCount": len(args.sites),
        "controlSites": args.control_sites,
        "classificationCounts": summarize(results),
        "pageTitleProbes": page_results,
        "results": results,
    }
    evidence_path = out_dir / "sweep-evidence.json"
    evidence_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[sweep] evidence written: {evidence_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
