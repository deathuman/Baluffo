#!/usr/bin/env python3
"""Turn coverage-audit misses into registry-ready board candidates.

The catalogue sweep says 4,930 of 10,465 missing openings are unreachable only
because their board was never registered. That is mechanical, and this turns it into
a repeatable pipeline instead of a hand-registered list.

One missed opening justifies one row: there is no volume threshold, because the
point of the product is that no opening is left behind. What a row must not do is
duplicate an existing one, and identity here is the **registry id**, which encodes
host plus tenant. Never the studio label — the registry carries one board as both
"Lost Boys Interactive" and "Lost Boys Interactive (Embracer Group)", and a
label-keyed join calls a covered board missing.

Candidates are emitted, not applied. Writing to the registry is a separate,
deliberate step so the plan can be printed and asserted first.

Usage:
  python tools/coverage_boards.py --report _out/coverage/report.json \\
      --registry data/source-registry-active.json --out _out/coverage/boards.json
  python tools/coverage_boards.py ... --vendor workday --show
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

# The probe owns host/board resolution; importing it by path keeps tools/ flat with
# no package init, and keeps one implementation of the tenant rules.
_spec = importlib.util.spec_from_file_location(
    "coverage_probe", Path(__file__).resolve().parent / "coverage_probe.py"
)
if not _spec or not _spec.loader:  # pragma: no cover - import guard
    raise SystemExit("coverage_probe.py must sit beside coverage_boards.py")
_probe = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("coverage_probe", _probe)
_spec.loader.exec_module(_probe)
board_key = _probe.board_key
host_of = _probe.host_of

# Adapters the runtime can actually fetch. A vendor outside this set is reported as
# `unsupported_vendor` rather than being silently dropped, because the opening is
# still real and the adapter gap is then a visible, countable backlog.
ADAPTER_ID_FORMAT = {
    "ashby": "board_url",
    # Anything without a structured adapter is still reachable: the registry holds
    # 2,030 active static rows, and a scraped careers page is how Baluffo covers
    # most studios. Without this fallback the 697 unknown-host boards would be
    # discarded, which is exactly the "leaving openings behind" failure the sweep
    # exists to prevent.
    "static": "listing_url",
    "bamboohr": "listing_url",
    "breezy": "board_url",
    "greenhouse": "slug",
    "jazzhr": "board_url",
    "lever": "account",
    "oracle_hcm": "listing_url",
    "personio": "feed_url",
    "pinpoint": "api_url",
    "recruitee": "api_url",
    "smartrecruiters": "company_id",
    "teamtailor": "listing_url",
    "workable": "account",
    "workday": "listing_url",
}

# Host patterns that identify a vendor. Ordered: first match wins, so put the
# specific multi-tenant patterns above the generic ones.
HOST_RULES: tuple[tuple[str, str], ...] = (
    (r"(^|\.)job-boards\.greenhouse\.io$", "greenhouse"),
    (r"(^|\.)boards\.greenhouse\.io$", "greenhouse"),
    (r"^jobs\.lever\.co$", "lever"),
    (r"(^|\.)jobs\.ashbyhq\.com$", "ashby"),
    (r"(^|\.)jobs\.smartrecruiters\.com$", "smartrecruiters"),
    (r"(^|\.)recruitee\.com$", "recruitee"),
    (r"(^|\.)bamboohr\.com$", "bamboohr"),
    (r"(^|\.)breezy\.hr$", "breezy"),
    (r"(^|\.)teamtailor\.com$", "teamtailor"),
    (r"(^|\.)apply\.workable\.com$", "workable"),
    (r"(^|\.)pinpointhq\.com$", "pinpoint"),
    (r"(^|\.)myworkdayjobs\.com$", "workday"),
    (r"(^|\.)oraclecloud\.com$", "oracle_hcm"),
    (r"(^|\.)personio\.(com|de)$", "personio"),
    (r"(^|\.)applytojob\.com$", "jazzhr"),
    (r"(^|\.)jobs\.feishu\.cn$", "feishu"),
    (r"(^|\.)jobs\.hiretalent\.com$", "hirentalent"),
    (r"(^|\.)hrmos\.co$", "hrmos"),
    (r"(^|\.)csod\.com$", "csod"),
)

STATUS_NEW = "new_candidate"
STATUS_ALREADY = "already_registered"
STATUS_UNSUPPORTED = "unsupported_vendor"
STATUS_NO_TENANT = "no_tenant_resolved"

# Where a platform keeps its tenant. Getting this wrong is the single most damaging
# thing this tool can do: a host-root row for a multi-tenant platform is one row
# standing in for a whole tenant's board, so the openings stay missing while the
# registry looks served. Four shapes occur in the data:
#
#   subdomain  boomingtech.jobs.feishu.cn, nintendoeurope.csod.com
#   seg0       jobs.jobvite.com/asus/job/..., jobs.ashbyhq.com/arb-interactive/...
#   segN       herp.careers/v1/charabank/..., app.mokahr.com/social-recruitment/ourpalm/...
#   host       jobs.ea.com/en_US/careers/..., careers.roblox.com/jobs/...
TENANT_SOURCE = {
    r"(^|\.)recruitee\.com$": "host",
    r"(^|\.)bamboohr\.com$": "host",
    r"(^|\.)breezy\.hr$": "host",
    r"(^|\.)pinpointhq\.com$": "host",
    r"(^|\.)personio\.(com|de)$": "host",
    r"(^|\.)applytojob\.com$": "host",
    r"^jobs\.lever\.co$": "seg0",
    r"(^|\.)job-boards\.greenhouse\.io$": "seg0",
    r"(^|\.)boards\.greenhouse\.io$": "seg0",
    r"(^|\.)jobs\.smartrecruiters\.com$": "seg0",
    r"(^|\.)jobs\.ashbyhq\.com$": "seg0",
    r"(^|\.)teamtailor\.com$": "seg0",
    r"(^|\.)myworkdayjobs\.com$": "seg0",
    r"(^|\.)jobs\.jobvite\.com$": "seg0",
    r"(^|\.)herp\.careers$": "after",
    r"(^|\.)app\.mokahr\.com$": "after",
    r"(^|\.)jobs\.feishu\.cn$": "subdomain",
    r"(^|\.)jobs\.hiretalent\.com$": "subdomain",
    r"(^|\.)csod\.com$": "subdomain",
    r"(^|\.)apply\.workable\.com$": "seg0",
    # hrmos.co/pages/<tenant>/jobs/<id> -- the tenant is the segment after
    # "pages", not the host. Measured 31 tenants on the apex host covering 845
    # openings, including Capcom, Square Enix, Cygames, Nexon, Game Freak and
    # Spike Chunsoft; a host-root row would report all 31 as one served board
    # while leaving every one of those openings missing.
    r"(^|\.)hrmos\.co$": "after",
}

# Path segments that are platform chrome, never a tenant. Stored lowercased
# because lookup lowercases the candidate: "en_US" listed with its original case
# would never match, and the locale segment would be taken for a tenant.
_TENANT_SKIP = {
    "v1",
    "v2",
    "index",
    "ux",
    "ats",
    "careersite",
    "social-recruitment",
    "recruitment",
    "jobs",
    "career",
    "pages",
    "position",
    "requisition",
    "en_us",
    "en_gb",
}


def tenant_source_for(host: str) -> str:
    lowered = host.lower()
    for pattern, source in TENANT_SOURCE.items():
        if re.search(pattern, lowered):
            return source
    return "host"


def resolve_tenant(host: str, segments: Sequence[str]) -> str:
    source = tenant_source_for(host)
    if source == "host":
        return host
    if source == "subdomain":
        return host.split(".", 1)[0]
    if source == "seg0":
        return segments[0] if segments else ""
    # "after": first segment that is not platform chrome.
    return next((s for s in segments if s.lower() not in _TENANT_SKIP), "")


def adapter_for_host(host: str) -> str:
    lowered = host.lower()
    for pattern, adapter in HOST_RULES:
        if re.search(pattern, lowered):
            return adapter
    return "static"


def _segments(url: str) -> list[str]:
    return [segment for segment in urlparse(url).path.split("/") if segment]


def build_candidate(source_url: str, *, company: str = "") -> dict[str, Any]:
    """Derive one board descriptor from a missed opening's URL.

    Tenant resolution is the delicate part. Multi-tenant hosts encode the board in
    the first path segment (greenhouse ``/2kczech``, workday ``/timi_careers``), but
    on single-tenant hosts the first segment is a job id, so a board built from it
    would be a new row per opening. Those hosts take the host root instead.
    """
    host = host_of(source_url)
    segments = _segments(source_url)
    adapter = adapter_for_host(host)
    scheme = "https"
    tenant = resolve_tenant(host, segments)
    if adapter == "oracle_hcm":
        tenant = (
            next((s for s in segments if s not in {"hcmUI", "CandidateExperience"}), "") or host
        )

    row: dict[str, Any] = {"adapter": adapter, "host": host, "tenant": tenant, "company": company}
    if adapter not in ADAPTER_ID_FORMAT:
        row["status"] = STATUS_UNSUPPORTED
        return row
    if not tenant:
        row["status"] = STATUS_NO_TENANT
        return row

    base = f"{scheme}://{host}"
    if adapter == "greenhouse":
        row.update({"slug": tenant, "id": f"greenhouse:slug:{tenant.lower()}"})
    elif adapter == "lever":
        row.update({"account": tenant, "id": f"lever:account:{tenant.lower()}"})
    elif adapter == "ashby":
        board_url = f"{base}/{tenant}"
        row.update({"board_url": board_url, "id": f"ashby:board_url:{board_url}"})
    elif adapter == "smartrecruiters":
        org = re.sub(r"[^A-Za-z0-9]", "", tenant)
        row.update(
            {
                "company_id": org,
                "api_url": f"https://api.smartrecruiters.com/v1/companies/{org}/postings",
                "id": f"smartrecruiters:company_id:{org.lower()}",
            }
        )
    elif adapter == "recruitee":
        api = f"{base}/api/offers/"
        row.update({"api_url": api, "id": f"recruitee:api_url:{api}"})
    elif adapter == "personio":
        feed = f"{base}/xml"
        row.update({"feed_url": feed, "id": f"personio:feed_url:{feed}"})
    elif adapter == "pinpoint":
        api = f"{base}/postings.json"
        row.update({"api_url": api, "id": f"pinpoint:api_url:{api}"})
    elif adapter == "bamboohr":
        url = f"{base}/careers"
        row.update({"listing_url": url, "id": f"bamboohr:listing_url:{url}"})
    elif adapter == "breezy":
        url = f"{base}/"
        row.update({"board_url": url, "id": f"breezy:board_url:{url}"})
    elif adapter == "jazzhr":
        url = f"{base}/apply"
        row.update({"board_url": url, "id": f"jazzhr:board_url:{url}"})
    elif adapter == "teamtailor":
        url = f"{base}/jobs"
        row.update({"listing_url": url, "id": f"teamtailor:listing_url:{url}"})
    elif adapter == "workable":
        row.update({"account": tenant, "id": f"workable:account:{tenant}"})
    elif adapter == "workday":
        url = "/".join([base, *segments[:1]])
        row.update({"listing_url": url, "id": f"workday:listing_url:{url}"})
    elif adapter == "oracle_hcm":
        row.update({"id": f"oracle_hcm:listing_url:{base}", "listing_url": base})
    elif adapter == "static":
        # Scraper candidate. A single-site careers host is its own board; a
        # multi-tenant platform needs one row per tenant or the whole tenant's
        # openings stay missing behind a row that looks served.
        if tenant == host:
            url = f"{scheme}://{host}"
            row.update({"listing_url": url, "id": f"static:listing_url:{url}"})
        else:
            base = f"{scheme}://{host}/{tenant}"
            row.update(
                {"listing_url": base, "id": f"static:listing_url:{base}", "tenantScoped": True}
            )
    row["status"] = STATUS_NEW
    return row


def collect_candidates(
    misses: Sequence[Mapping[str, Any]],
    registry_ids: set[str],
) -> list[dict[str, Any]]:
    """Group misses by board identity, counting the openings each board would add."""
    grouped: dict[str, dict[str, Any]] = {}
    for miss in misses:
        source_url = str(miss.get("sourceUrl") or "")
        if not source_url:
            continue
        candidate = build_candidate(source_url, company=str(miss.get("company") or ""))
        key = candidate.get("id") or board_key(source_url)
        if candidate.get("status") == STATUS_UNSUPPORTED:
            # Group by tenant even with no adapter, so the backlog reads as
            # "826 openings on 40 feishu boards" rather than one opaque row. Board
            # count is meaningless for a vendor that cannot be read; openings is
            # the thing worth sizing.
            key = f"unsupported:{candidate.get('adapter')}:{candidate.get('tenant') or key}"
        entry = grouped.setdefault(
            key,
            {**candidate, "id": key, "missingCount": 0, "sampleTitles": [], "companies": set()},
        )
        entry["missingCount"] += 1
        title = str(miss.get("title") or "")
        if title and len(entry["sampleTitles"]) < 3:
            entry["sampleTitles"].append(title)
        company = str(miss.get("company") or "")
        if company:
            entry["companies"].add(company)

    out: list[dict[str, Any]] = []
    for entry in grouped.values():
        entry["companies"] = sorted(entry["companies"])[:3]
        if entry.get("id") in registry_ids:
            entry["status"] = STATUS_ALREADY
        out.append(entry)
    return sorted(out, key=lambda row: (-int(row["missingCount"]), str(row.get("id"))))


def summarise(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_status: dict[str, int] = {}
    by_adapter: dict[str, int] = {}
    openings = 0
    backlog: dict[str, dict[str, int]] = {}
    for row in candidates:
        status = str(row.get("status"))
        adapter = str(row.get("adapter"))
        by_status[status] = by_status.get(status, 0) + 1
        if status == STATUS_NEW:
            openings += int(row.get("missingCount") or 0)
            by_adapter[adapter] = by_adapter.get(adapter, 0) + 1
        elif status == STATUS_UNSUPPORTED:
            entry = backlog.setdefault(adapter, {"boards": 0, "openings": 0})
            entry["boards"] += 1
            entry["openings"] += int(row.get("missingCount") or 0)
    return {
        "boards": len(candidates),
        "byStatus": by_status,
        "openingsRecoverable": openings,
        "newBoardsByAdapter": dict(sorted(by_adapter.items(), key=lambda kv: -kv[1])),
        "unsupportedVendorBacklog": dict(
            sorted(backlog.items(), key=lambda kv: -kv[1]["openings"])
        ),
    }


def render(
    candidates: Sequence[Mapping[str, Any]],
    *,
    limit: int = 40,
    summary: Mapping[str, Any] | None = None,
) -> str:
    lines = [
        f"boards seen: {len(candidates)}",
        f"  new candidates      : {sum(1 for c in candidates if c.get('status') == STATUS_NEW)}",
        f"  already registered  : {sum(1 for c in candidates if c.get('status') == STATUS_ALREADY)}",
        f"  unsupported vendor  : {sum(1 for c in candidates if c.get('status') == STATUS_UNSUPPORTED)}",
        f"  no tenant resolved  : {sum(1 for c in candidates if c.get('status') == STATUS_NO_TENANT)}",
        "",
        f"{'openings':>8}  {'adapter':<16} {'id':<62} company",
        "-" * 120,
    ]
    for row in candidates[:limit]:
        if row.get("status") != STATUS_NEW:
            continue
        company = ", ".join(row.get("companies") or [])[:40]
        lines.append(
            f"{row.get('missingCount', 0):>8}  {str(row.get('adapter')):<16} "
            f"{str(row.get('id'))[:60]:<62} {company}"
        )
    backlog = summary.get("unsupportedVendorBacklog") or {}
    if backlog:
        lines += ["", "unreadable vendors (adapter support backlog):"]
        for vendor, stats in backlog.items():
            lines.append(
                f"{stats['openings']:>8}  {vendor:<16} across {stats['boards']} boards -- needs an adapter"
            )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", required=True, type=Path, help="coverage_audit report.json")
    parser.add_argument("--registry", type=Path, help="source-registry-active.json")
    parser.add_argument("--out", type=Path, help="where to write boards.json")
    parser.add_argument("--vendor", help="only show this adapter")
    parser.add_argument("--show", type=int, default=40, help="rows to print")
    args = parser.parse_args(argv)

    report = json.loads(args.report.read_text(encoding="utf-8"))
    registry_ids: set[str] = set()
    if args.registry and args.registry.exists():
        raw = json.loads(args.registry.read_text(encoding="utf-8"))
        rows = raw if isinstance(raw, list) else raw.get("rows") or []
        registry_ids = {
            str(row.get("id") or "").lower()
            for row in rows
            if isinstance(row, Mapping) and row.get("id")
        }

    candidates = collect_candidates(report.get("misses") or [], registry_ids)
    if args.vendor:
        candidates = [c for c in candidates if str(c.get("adapter")) == args.vendor]

    summary = summarise(candidates)
    payload = {"summary": summary, "candidates": candidates}
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(render(candidates, limit=args.show, summary=summary))
    print()
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
