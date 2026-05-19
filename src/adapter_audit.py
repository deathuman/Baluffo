from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jobs.adapters import community, provider_api
from src.jobs.common.config import DEFAULT_TIMEOUT_S, SOURCE_DIAGNOSTICS
from src.jobs.common.http import default_fetch_text
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text
from src.shared.utils import now_iso

REPORT_JSON_PATH = Path("data/adapter-audit-report.json")
REPORT_MD_PATH = Path("data/adapter-audit-report.md")
ASHBY_REFRESH_REPORT_PATH = Path("data/ashby-registry-refresh-report.json")


AUDIT_CASES: list[dict[str, Any]] = [
    {
        "name": "gamejobs",
        "family": "community",
        "runner": community.run_gamejobs_source,
        "diagnostic": "gamejobs",
        "source_type": "built_in",
    },
    {
        "name": "workwithindies",
        "family": "community",
        "runner": community.run_workwithindies_source,
        "diagnostic": "workwithindies",
        "source_type": "built_in",
    },
    {
        "name": "gracklehq",
        "family": "community",
        "runner": community.run_gracklehq_source,
        "diagnostic": "gracklehq",
        "source_type": "built_in",
    },
    {
        "name": "8bitplay",
        "family": "community",
        "runner": community.run_8bitplay_source,
        "diagnostic": "8bitplay",
        "source_type": "built_in",
    },
    {
        "name": "gamesindustry",
        "family": "community",
        "runner": community.run_gamesindustry_source,
        "diagnostic": "gamesindustry",
        "source_type": "built_in",
    },
    {
        "name": "wellfound",
        "family": "community",
        "runner": community.run_wellfound_source,
        "diagnostic": "wellfound",
        "source_type": "built_in",
    },
    {
        "name": "epic_games_careers",
        "family": "community",
        "runner": community.run_epic_games_careers_source,
        "diagnostic": "epic_games_careers",
        "source_type": "built_in",
    },
    {
        "name": "greenhouse",
        "family": "provider",
        "runner": provider_api.run_greenhouse_boards_source,
        "diagnostic": "greenhouse_boards",
        "registry_adapter": "greenhouse",
    },
    {
        "name": "teamtailor",
        "family": "provider",
        "runner": provider_api.run_teamtailor_sources_source,
        "diagnostic": "teamtailor_sources",
        "registry_adapter": "teamtailor",
    },
    {
        "name": "lever",
        "family": "provider",
        "runner": provider_api.run_lever_sources_source,
        "diagnostic": "lever_sources",
        "registry_adapter": "lever",
    },
    {
        "name": "workable",
        "family": "provider",
        "runner": provider_api.run_workable_sources_source,
        "diagnostic": "workable_sources",
        "registry_adapter": "workable",
    },
    {
        "name": "smartrecruiters",
        "family": "provider",
        "runner": provider_api.run_smartrecruiters_sources_source,
        "diagnostic": "smartrecruiters_sources",
        "registry_adapter": "smartrecruiters",
    },
    {
        "name": "recruitee",
        "family": "provider",
        "runner": provider_api.run_recruitee_sources_source,
        "diagnostic": "recruitee_sources",
        "registry_adapter": "recruitee",
    },
    {
        "name": "pinpoint",
        "family": "provider",
        "runner": provider_api.run_pinpoint_sources_source,
        "diagnostic": "pinpoint_sources",
        "registry_adapter": "pinpoint",
    },
    {
        "name": "breezy",
        "family": "provider",
        "runner": provider_api.run_breezy_sources_source,
        "diagnostic": "breezy_sources",
        "registry_adapter": "breezy",
    },
    {
        "name": "jazzhr",
        "family": "provider",
        "runner": provider_api.run_jazzhr_sources_source,
        "diagnostic": "jazzhr_sources",
        "registry_adapter": "jazzhr",
    },
    {
        "name": "oracle_hcm",
        "family": "provider",
        "runner": provider_api.run_oracle_hcm_sources_source,
        "diagnostic": "oracle_hcm_sources",
        "registry_adapter": "oracle_hcm",
    },
    {
        "name": "ashby",
        "family": "provider",
        "runner": provider_api.run_ashby_sources_source,
        "diagnostic": "ashby_sources",
        "registry_adapter": "ashby",
    },
    {
        "name": "personio",
        "family": "provider",
        "runner": provider_api.run_personio_sources_source,
        "diagnostic": "personio_sources",
        "registry_adapter": "personio",
    },
]


def _registry_examples(adapter: str) -> list[str]:
    rows = registry_entries(adapter, enabled_only=True)
    return [
        clean_text(row.get("name")) or clean_text(row.get("studio")) or adapter for row in rows[:3]
    ]


def _detail_examples(details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for detail in details[:3]:
        rows.append(
            {
                "name": clean_text(detail.get("name"))
                or clean_text(detail.get("studio"))
                or "unknown",
                "status": clean_text(detail.get("status")) or "unknown",
                "classification": clean_text(detail.get("classification")),
                "keptCount": int(detail.get("keptCount") or 0),
                "error": clean_text(detail.get("error")),
            }
        )
    return rows


def _bucket_for_result(
    *, jobs_count: int, error_text: str, details: list[dict[str, Any]], partial_errors: list[str]
) -> str:
    detail_classifications = {
        clean_text(row.get("classification")).lower()
        for row in details
        if clean_text(row.get("classification"))
    }
    detail_errors = " | ".join(
        clean_text(row.get("error")) for row in details if clean_text(row.get("error"))
    )
    combined_error = " | ".join(
        partial_errors
        + ([error_text] if error_text else [])
        + ([detail_errors] if detail_errors else [])
    ).lower()
    if jobs_count > 0 and not combined_error:
        return "working"
    if jobs_count > 0:
        return "mixed-success"
    if "parser_stale" in detail_classifications or "no jobs extracted" in combined_error:
        return "parser-stale"
    if any(
        token in detail_classifications for token in {"rate_limited", "dead_listing_page"}
    ) or any(
        token in combined_error
        for token in ("429", "rate limit", "page not found", "not found", "forbidden")
    ):
        return "source-limited"
    if (
        "no adapter plugin matched" in combined_error
        or "missing registry_adapter" in combined_error
    ):
        return "adapter-broken"
    if jobs_count > 0:
        return "working"
    return "follow-up-needed"


def _ashby_summary(details: list[dict[str, Any]]) -> dict[str, int]:
    removed_count = 0
    try:
        payload = json.loads(ASHBY_REFRESH_REPORT_PATH.read_text(encoding="utf-8"))
        removed_count = int(payload.get("removedCount") or 0) if isinstance(payload, dict) else 0
    except (OSError, ValueError, json.JSONDecodeError):
        removed_count = 0
    return {
        "configuredCompanyCount": len(details),
        "liveNonEmptyBoardCount": sum(1 for row in details if int(row.get("keptCount") or 0) > 0),
        "rawPostingsCount": sum(int(row.get("fetchedCount") or 0) for row in details),
        "keptJobsCount": sum(int(row.get("keptCount") or 0) for row in details),
        "removedStaleOrEmptyCount": removed_count,
    }


def _run_case(case: dict[str, Any]) -> dict[str, Any]:
    SOURCE_DIAGNOSTICS.clear()
    started = time.perf_counter()
    jobs_count = 0
    error_text = ""
    try:
        rows = case["runner"](
            fetch_text=default_fetch_text,
            timeout_s=DEFAULT_TIMEOUT_S,
            retries=1,
            backoff_s=0.5,
        )
        jobs_count = len(rows)
    except Exception as exc:  # noqa: BLE001
        error_text = str(exc)
    duration_ms = int((time.perf_counter() - started) * 1000)
    diagnostics = dict(SOURCE_DIAGNOSTICS.get(case["diagnostic"], {}))
    details = list(diagnostics.get("details") or [])
    partial_errors = [
        clean_text(item) for item in (diagnostics.get("partialErrors") or []) if clean_text(item)
    ]
    bucket = _bucket_for_result(
        jobs_count=jobs_count, error_text=error_text, details=details, partial_errors=partial_errors
    )
    result = {
        "adapter": case["name"],
        "family": case["family"],
        "bucket": bucket,
        "jobsCount": jobs_count,
        "durationMs": duration_ms,
        "error": error_text,
        "sourceExamples": _registry_examples(case["registry_adapter"])
        if case.get("registry_adapter")
        else [],
        "detailExamples": _detail_examples(details),
        "partialErrors": partial_errors[:5],
    }
    if case["name"] == "ashby":
        result["ashbySummary"] = _ashby_summary(details)
    return result


def _build_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Adapter Audit Report",
        "",
        f"- Generated: `{report['generatedAt']}`",
        f"- Working: `{len(report['buckets']['working'])}`",
        f"- Source-limited: `{len(report['buckets']['source-limited'])}`",
        f"- Adapter-broken: `{len(report['buckets']['adapter-broken'])}`",
        f"- Mixed-success: `{len(report['buckets']['mixed-success'])}`",
        f"- Parser-stale: `{len(report['buckets']['parser-stale'])}`",
        f"- Follow-up-needed: `{len(report['buckets']['follow-up-needed'])}`",
        "",
    ]
    for bucket in (
        "working",
        "mixed-success",
        "source-limited",
        "adapter-broken",
        "parser-stale",
        "follow-up-needed",
    ):
        lines.append(f"## {bucket}")
        items = report["buckets"][bucket]
        if not items:
            lines.append("")
            lines.append("- none")
            lines.append("")
            continue
        lines.append("")
        for row in items:
            sources = ", ".join(row.get("sourceExamples") or []) or "built-in source list"
            detail = ""
            examples = row.get("detailExamples") or []
            if examples:
                sample = examples[0]
                detail = f"; sample={sample['name']} status={sample['status']} kept={sample['keptCount']}"
                if sample.get("classification"):
                    detail += f" classification={sample['classification']}"
            error = clean_text(row.get("error"))
            if error:
                detail += f"; error={error}"
            ashby_summary = (
                row.get("ashbySummary") if isinstance(row.get("ashbySummary"), dict) else None
            )
            if ashby_summary:
                detail += (
                    f"; ashby configured={ashby_summary['configuredCompanyCount']}"
                    f" live_nonempty={ashby_summary['liveNonEmptyBoardCount']}"
                    f" raw={ashby_summary['rawPostingsCount']}"
                    f" kept={ashby_summary['keptJobsCount']}"
                    f" removed={ashby_summary['removedStaleOrEmptyCount']}"
                )
            lines.append(
                f"- `{row['adapter']}` jobs=`{row['jobsCount']}` durationMs=`{row['durationMs']}` sources=`{sources}`{detail}"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_report() -> dict[str, Any]:
    items = [_run_case(case) for case in AUDIT_CASES]
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in items:
        buckets[row["bucket"]].append(row)
    for key in (
        "working",
        "mixed-success",
        "source-limited",
        "adapter-broken",
        "parser-stale",
        "follow-up-needed",
    ):
        buckets.setdefault(key, [])
    return {
        "generatedAt": now_iso(),
        "results": items,
        "buckets": dict(buckets),
    }


def main() -> int:
    report = build_report()
    REPORT_JSON_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    REPORT_MD_PATH.write_text(_build_markdown(report), encoding="utf-8")
    print(f"Wrote {REPORT_JSON_PATH}")
    print(f"Wrote {REPORT_MD_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
