"""Scrapy-only path for static adapter: runner invocation, envelope handling, result parsing."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.jobs import common
from src.jobs.adapters import _runtime
from src.jobs.models import RawJob
from src.shared.utils import coerce_int


def _clean_errors(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    cleaned = []
    for item in values:
        text = common.clean_text(item)
        if text:
            cleaned.append(text)
    return cleaned


def _base_detail(source_row: Dict[str, Any], *, status: str = "error", error: str = "") -> Dict[str, Any]:
    source_name = common.clean_text(source_row.get("name")) or "unknown"
    studio_name = common.clean_text(source_row.get("studio")) or source_name
    pages = source_row.get("pages") if isinstance(source_row.get("pages"), list) else []
    source_id = common.clean_text(source_row.get("id"))
    if not source_id:
        seed = "|".join([source_name, studio_name, *[common.clean_text(page) for page in pages]])
        source_id = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return {
        "adapter": "scrapy_static",
        "studio": studio_name,
        "name": source_name,
        "status": status,
        "fetchedCount": 0,
        "keptCount": 0,
        "error": common.clean_text(error),
        "classification": "parse_error" if common.norm_text(status) == "error" else "ok_no_jobs",
        "top_reject_reasons": [],
        "browserFallbackRecommended": False,
        "sourceId": source_id,
        "pages": [common.clean_text(page) for page in pages if common.clean_text(page)],
        "loss": {
            "scrapyRunnerRejectedValidation": 0,
            "scrapyParentInvalidPayload": 0,
        },
    }


def _coerce_int(value: Any) -> int:
    return coerce_int(value, 0, minimum=0, maximum=2**31 - 1)


def _normalize_job(raw: Any, source_row: Dict[str, Any]) -> Optional[RawJob]:
    if not isinstance(raw, dict):
        return None
    strict_validation = common.env_flag("BALUFFO_SCRAPY_VALIDATION_STRICT", common.DEFAULT_SCRAPY_VALIDATION_STRICT)
    source_name = common.clean_text(raw.get("source")) or (common.clean_text(source_row.get("name")) or "scrapy_static")
    studio_name = common.clean_text(raw.get("studio")) or (common.clean_text(source_row.get("studio")) or common.clean_text(source_row.get("name")) or "unknown")
    title = common.clean_text(raw.get("title"))
    company = common.clean_text(raw.get("company"))
    job_link = common.normalize_url(raw.get("jobLink"))
    source_job_id = common.clean_text(raw.get("sourceJobId"))
    if not title or not company:
        return None
    if not job_link and not strict_validation:
        source_bundle_raw = raw.get("sourceBundle")
        if isinstance(source_bundle_raw, list):
            for item in source_bundle_raw:
                if not isinstance(item, dict):
                    continue
                candidate = common.normalize_url(item.get("jobLink"))
                if candidate:
                    job_link = candidate
                    break
    if not job_link:
        return None
    if not source_job_id:
        source_job_id = hashlib.sha1(f"{title}|{company}|{job_link}".encode("utf-8")).hexdigest()[:12]
    posted_at = common.to_iso(raw.get("postedAt"))
    source_bundle = raw.get("sourceBundle")
    if not isinstance(source_bundle, list) or not source_bundle:
        source_bundle = [
            {
                "source": source_name,
                "sourceJobId": source_job_id,
                "jobLink": job_link,
                "postedAt": posted_at,
                "adapter": "scrapy_static",
                "studio": studio_name,
            }
        ]

    return {
        "sourceJobId": source_job_id,
        "title": title,
        "company": company,
        "city": common.clean_text(raw.get("city")),
        "country": common.clean_text(raw.get("country")) or "Unknown",
        "workType": common.clean_text(raw.get("workType")),
        "contractType": common.clean_text(raw.get("contractType")),
        "jobLink": job_link,
        "sector": common.clean_text(raw.get("sector")) or "Game",
        "postedAt": posted_at,
        "source": source_name,
        "studio": studio_name,
        "adapter": common.clean_text(raw.get("adapter")) or "scrapy_static",
        "sourceBundle": source_bundle,
    }


def run_scrapy_static_source(
    *,
    fetch_text: Any,
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> List[RawJob]:
    deps = _runtime.facade()
    subprocess_module = getattr(deps, "subprocess", subprocess)
    del fetch_text

    results_list: List[RawJob] = []
    errors_list: List[str] = []
    details: List[Dict[str, Any]] = []

    sources = deps.registry_entries("scrapy_static")
    if not sources:
        deps.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[],
            partial_errors=["No enabled scrapy_static sources"],
        )
        return []

    runner_path = Path(__file__).resolve().parents[2] / "scrapers" / "runner.py"
    if not runner_path.exists():
        msg = f"scrapy_static runner missing: {runner_path}"
        deps.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[_base_detail({"name": "scrapy_static"}, error=msg)],
            partial_errors=[msg],
        )
        return []

    for source in sources:
        source_name = common.clean_text(source.get("name")) or "unknown"
        studio_name = common.clean_text(source.get("studio")) or source_name
        pages = source.get("pages") if isinstance(source.get("pages"), list) else []
        config = {
            "source": {
                "name": source_name,
                "studio": studio_name,
                "pages": pages,
                "nlPriority": bool(source.get("nlPriority", False)),
            },
            "runtime": {
                "timeout_s": int(timeout_s),
                "retries": int(retries),
                "backoff_s": float(backoff_s),
                "download_delay": 1.0,
            },
        }

        source_detail = _base_detail(source)
        try:
            timeout_window = min(300, max(1, int(timeout_s)) * max(1, len(pages)) * 4)
            result = subprocess_module.run(
                [sys.executable, str(runner_path)],
                input=json.dumps(config).encode("utf-8"),
                capture_output=True,
                timeout=timeout_window,
                check=False,
            )
            stderr_text = common.clean_text(result.stderr.decode("utf-8", errors="replace"))
            if result.returncode != 0:
                errors_list.append(f"{source_name}: subprocess exit {result.returncode}")
            if stderr_text and result.returncode != 0:
                errors_list.append(f"{source_name}: stderr: {stderr_text[:500]}")

            stdout_text = result.stdout.decode("utf-8", errors="replace")
            try:
                envelope = json.loads(stdout_text)
            except json.JSONDecodeError as exc:
                envelope = {}
                errors_list.append(f"{source_name}: JSON parse error: {exc}")
                if stderr_text:
                    errors_list.append(f"{source_name}: stderr: {stderr_text[:500]}")

            if not isinstance(envelope, dict) or "ok" not in envelope:
                source_detail.update(
                    {
                        "status": "error",
                        "error": "Invalid envelope from scraper runner",
                        "classification": "parse_error",
                        "browserFallbackRecommended": False,
                    }
                )
                if not isinstance(envelope, dict):
                    errors_list.append(f"{source_name}: invalid envelope type")
                else:
                    errors_list.append(f"{source_name}: invalid envelope missing 'ok'")
                details.append(source_detail)
                continue

            envelope_details = envelope.get("details")
            if isinstance(envelope_details, list) and envelope_details:
                detail_0 = envelope_details[0]
                if isinstance(detail_0, dict):
                    source_detail.update(
                        {
                            "status": "ok" if common.clean_text(detail_0.get("status")).lower() == "ok" else "error",
                            "fetchedCount": _coerce_int(detail_0.get("fetchedCount")),
                            "keptCount": _coerce_int(detail_0.get("keptCount")),
                            "error": common.clean_text(detail_0.get("error")),
                            "classification": common.clean_text(detail_0.get("classification")) or source_detail.get("classification"),
                            "browserFallbackRecommended": bool(detail_0.get("browserFallbackRecommended")),
                            "top_reject_reasons": detail_0.get("top_reject_reasons") if isinstance(detail_0.get("top_reject_reasons"), list) else [],
                            "sourceId": common.clean_text(detail_0.get("sourceId")) or source_detail.get("sourceId"),
                            "pages": detail_0.get("pages") if isinstance(detail_0.get("pages"), list) else source_detail.get("pages"),
                        }
                    )

            partial_errors = _clean_errors(envelope.get("partialErrors"))
            for item in partial_errors:
                errors_list.append(f"{source_name}: {item}")

            jobs = envelope.get("jobs")
            if bool(envelope.get("ok")) and isinstance(jobs, list):
                kept = 0
                parent_invalid_payload = 0
                for item in jobs:
                    normalized = _normalize_job(item, source)
                    if normalized:
                        kept += 1
                        results_list.append(normalized)
                    else:
                        parent_invalid_payload += 1
                        errors_list.append(f"{source_name}: dropped invalid job payload from runner")
                source_detail_loss = source_detail.get("loss") if isinstance(source_detail.get("loss"), dict) else {}
                source_detail_loss["scrapyParentInvalidPayload"] = int(parent_invalid_payload)
                source_detail["loss"] = source_detail_loss
                source_detail["keptCount"] = max(int(source_detail.get("keptCount") or 0), kept)
                source_detail["status"] = "ok"
                if not common.clean_text(source_detail.get("classification")):
                    source_detail["classification"] = "ok_with_jobs" if kept > 0 else "ok_no_jobs"
                if source_detail.get("classification") == "ok_no_jobs" and int(source_detail.get("fetchedCount") or 0) > 0:
                    source_detail["classification"] = "fetch_ok_extract_zero"
                source_detail["browserFallbackRecommended"] = bool(
                    source_detail.get("browserFallbackRecommended")
                    or source_detail.get("classification") in {"fetch_ok_extract_zero", "blocked_or_challenge"}
                )
            else:
                source_detail["status"] = "error"
                if not common.clean_text(source_detail.get("error")):
                    source_detail["error"] = "crawl failed"
                source_detail["classification"] = "parse_error"
                errors_list.append(f"{source_name}: crawl failed")

            stats = envelope.get("stats")
            if isinstance(stats, dict):
                source_detail["stats"] = {
                    "downloader/request_count": _coerce_int(stats.get("downloader/request_count")),
                    "downloader/response_count": _coerce_int(stats.get("downloader/response_count")),
                    "downloader/response_status_count/200": _coerce_int(stats.get("downloader/response_status_count/200")),
                    "retry/count": _coerce_int(stats.get("retry/count")),
                    "item_scraped_count": _coerce_int(stats.get("item_scraped_count")),
                    "candidate_links_found": _coerce_int(stats.get("candidate_links_found")),
                    "detail_pages_visited": _coerce_int(stats.get("detail_pages_visited")),
                    "jobs_emitted": _coerce_int(stats.get("jobs_emitted")),
                    "jobs_rejected_validation": _coerce_int(stats.get("jobs_rejected_validation")),
                    "finish_reason": common.clean_text(stats.get("finish_reason")),
                }
                source_detail_loss = source_detail.get("loss") if isinstance(source_detail.get("loss"), dict) else {}
                source_detail_loss["scrapyRunnerRejectedValidation"] = _coerce_int(stats.get("jobs_rejected_validation"))
                source_detail["loss"] = source_detail_loss
                if int(source_detail.get("fetchedCount") or 0) <= 0:
                    source_detail["fetchedCount"] = int(source_detail["stats"]["downloader/response_count"])

            details.append(source_detail)
        except subprocess_module.TimeoutExpired:
            source_detail.update(
                {
                    "status": "error",
                    "error": "subprocess timeout",
                    "classification": "timeout",
                    "browserFallbackRecommended": True,
                }
            )
            errors_list.append(f"{source_name}: subprocess timeout")
            details.append(source_detail)
        except Exception as exc:  # noqa: BLE001
            source_detail.update(
                {
                    "status": "error",
                    "error": common.clean_text(exc)[:500],
                    "classification": "parse_error",
                    "browserFallbackRecommended": False,
                }
            )
            errors_list.append(f"{source_name}: {type(exc).__name__}: {common.clean_text(exc)[:200]}")
            details.append(source_detail)

    deps.set_source_diagnostics(
        "scrapy_static_sources",
        adapter="scrapy_static",
        studio="multiple",
        details=details,
        partial_errors=errors_list,
    )
    return results_list
