"""Scrapy-only path for static adapter: runner invocation, envelope handling, result parsing."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from src.jobs.common.datetime_utils import to_iso
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.taxonomy import (
    ClassificationContext,
    assess_zero_extract,
    classification_context_from_source_detail,
    classify_zero_kept,
    map_error_to_failure_bucket,
)
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.shared.utils import coerce_int, env_flag

from ..common import config as common_config

TIMEOUT_BUCKET_SOURCE_NAMES = {
    "andarion games gmbh (gamesmap)",
    "kevuru games (manual website)",
    "tequilaworks (manual website)",
}


def _update_taxonomy_fields(source_detail: dict[str, Any]) -> dict[str, Any]:
    """Update failureBucket and zeroKeptClassification based on current state."""
    original_classification = norm_text(source_detail.get("classification"))
    context = classification_context_from_source_detail(source_detail)
    if int(source_detail.get("keptCount", 0)) == 0 and source_detail.get("status") != "excluded":
        assessment = assess_zero_extract(context)
        source_detail["zeroKeptClassification"] = classify_zero_kept(context).value
        should_migrate = (
            norm_text(source_detail.get("status")) == "ok"
            or "no jobs extracted" in norm_text(source_detail.get("error"))
            or norm_text(source_detail.get("classification"))
            in {
                "ok_no_jobs",
                "fetch_ok_extract_zero",
                "parser_stale",
                "needs_review",
                "empty_confirmed",
            }
            or assessment.diagnosis.value != "needs_review"
        ) and original_classification != "dead_listing_page"
        if should_migrate:
            source_detail["classification"] = assessment.diagnosis.value
            source_detail["browserFallbackRecommended"] = assessment.browser_fallback_recommended
            context = classification_context_from_source_detail(source_detail)
    source_detail["failureBucket"] = map_error_to_failure_bucket(context).value
    return source_detail


def _clean_errors(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    cleaned = []
    for item in values:
        text = clean_text(item)
        if text:
            cleaned.append(text)
    return cleaned


def _base_detail(
    source_row: dict[str, Any],
    *,
    status: str = "error",
    error: str = "",
    signal_quality: str = "weak",
) -> dict[str, Any]:
    source_name = clean_text(source_row.get("name")) or "unknown"
    studio_name = clean_text(source_row.get("studio")) or source_name
    pages = source_row.get("pages") if isinstance(source_row.get("pages"), list) else []
    source_id = clean_text(source_row.get("id"))
    if not source_id:
        seed = "|".join([source_name, studio_name, *[clean_text(page) for page in pages]])
        source_id = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    classification = "parse_error" if norm_text(status) == "error" else "ok_no_jobs"
    context = ClassificationContext(
        status=status,
        error=error or "",
        classification=classification,
        signal_quality=signal_quality,
    )
    failure_bucket = map_error_to_failure_bucket(context)
    zero_kept_classification = None
    if classification in ("ok_no_jobs", "parser_stale") or status == "ok":
        zero_kept_class = classify_zero_kept(context)
        zero_kept_classification = zero_kept_class.value
    return {
        "adapter": "scrapy_static",
        "studio": studio_name,
        "name": source_name,
        "status": status,
        "fetchedCount": 0,
        "keptCount": 0,
        "error": clean_text(error),
        "classification": classification,
        "failureBucket": failure_bucket.value,
        "zeroKeptClassification": zero_kept_classification,
        "top_reject_reasons": [],
        "deadListingPageCount": 0,
        "deadListingPageExamples": [],
        "browserFallbackRecommended": False,
        "signalQuality": clean_text(signal_quality) or "weak",
        "sourceId": source_id,
        "pages": [clean_text(page) for page in pages if clean_text(page)],
        "loss": {
            "scrapyRunnerRejectedValidation": 0,
            "scrapyParentInvalidPayload": 0,
            "scrapyDeadListingPageRejected": 0,
        },
    }


def _coerce_int(value: Any) -> int:
    return coerce_int(value, 0, minimum=0, maximum=2**31 - 1)


def _normalize_job(raw: Any, source_row: dict[str, Any]) -> RawJob | None:
    if not isinstance(raw, dict):
        return None
    strict_validation = env_flag(
        "BALUFFO_SCRAPY_VALIDATION_STRICT", common_config.DEFAULT_SCRAPY_VALIDATION_STRICT
    )
    source_name = clean_text(raw.get("source")) or (
        clean_text(source_row.get("name")) or "scrapy_static"
    )
    studio_name = clean_text(raw.get("studio")) or (
        clean_text(source_row.get("studio")) or clean_text(source_row.get("name")) or "unknown"
    )
    title = clean_text(raw.get("title"))
    company = clean_text(raw.get("company"))
    job_link = normalize_url(raw.get("jobLink"))
    source_job_id = clean_text(raw.get("sourceJobId"))
    if not title or not company:
        return None
    if not job_link and not strict_validation:
        source_bundle_raw = raw.get("sourceBundle")
        if isinstance(source_bundle_raw, list):
            for item in source_bundle_raw:
                if not isinstance(item, dict):
                    continue
                candidate = normalize_url(item.get("jobLink"))
                if candidate:
                    job_link = candidate
                    break
    if not job_link:
        return None
    if not source_job_id:
        source_job_id = hashlib.sha1(f"{title}|{company}|{job_link}".encode()).hexdigest()[:12]
    posted_at = to_iso(raw.get("postedAt"))
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
        "city": clean_text(raw.get("city")),
        "country": clean_text(raw.get("country")) or "Unknown",
        "workType": clean_text(raw.get("workType")),
        "contractType": clean_text(raw.get("contractType")),
        "jobLink": job_link,
        "sector": clean_text(raw.get("sector")) or "Game",
        "postedAt": posted_at,
        "source": source_name,
        "studio": studio_name,
        "adapter": clean_text(raw.get("adapter")) or "scrapy_static",
        "sourceBundle": source_bundle,
    }


def run_scrapy_static_source(
    *,
    fetch_text: Any,
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> list[RawJob]:
    del fetch_text

    results_list: list[RawJob] = []
    errors_list: list[str] = []
    details: list[dict[str, Any]] = []

    sources = registry_entries("scrapy_static")
    if not sources:
        set_source_diagnostics(
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
        set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[_base_detail({"name": "scrapy_static"}, error=msg)],
            partial_errors=[msg],
        )
        return []

    for source in sources:
        source_name = clean_text(source.get("name")) or "unknown"
        studio_name = clean_text(source.get("studio")) or source_name
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
                "use_browser": True,
            },
        }
        timeout_bucket = source_name.lower() in TIMEOUT_BUCKET_SOURCE_NAMES
        if timeout_bucket:
            config["runtime"]["timeout_s"] = min(int(timeout_s), 10)

        source_detail = _base_detail(source, signal_quality="weak")
        try:
            timeout_window = min(
                90 if timeout_bucket else 300,
                max(1, int(config["runtime"]["timeout_s"])) * max(1, len(pages)) * 4,
            )
            result = subprocess.run(
                [sys.executable, str(runner_path)],
                input=json.dumps(config).encode("utf-8"),
                capture_output=True,
                timeout=timeout_window,
                check=False,
            )
            stderr_text = clean_text(result.stderr.decode("utf-8", errors="replace"))
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
                _update_taxonomy_fields(source_detail)
                details.append(source_detail)
                continue

            envelope_details = envelope.get("details")
            if isinstance(envelope_details, list) and envelope_details:
                detail_0 = envelope_details[0]
                if isinstance(detail_0, dict):
                    source_detail.update(
                        {
                            "status": "ok"
                            if clean_text(detail_0.get("status")).lower() == "ok"
                            else "error",
                            "fetchedCount": _coerce_int(detail_0.get("fetchedCount")),
                            "keptCount": _coerce_int(detail_0.get("keptCount")),
                            "error": clean_text(detail_0.get("error")),
                            "classification": clean_text(detail_0.get("classification"))
                            or source_detail.get("classification"),
                            "browserFallbackRecommended": bool(
                                detail_0.get("browserFallbackRecommended")
                            ),
                            "top_reject_reasons": detail_0.get("top_reject_reasons")
                            if isinstance(detail_0.get("top_reject_reasons"), list)
                            else [],
                            "deadListingPageCount": _coerce_int(
                                detail_0.get("deadListingPageCount")
                            ),
                            "deadListingPageExamples": detail_0.get("deadListingPageExamples")
                            if isinstance(detail_0.get("deadListingPageExamples"), list)
                            else [],
                            "sourceId": clean_text(detail_0.get("sourceId"))
                            or source_detail.get("sourceId"),
                            "pages": detail_0.get("pages")
                            if isinstance(detail_0.get("pages"), list)
                            else source_detail.get("pages"),
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
                        errors_list.append(
                            f"{source_name}: dropped invalid job payload from runner"
                        )
                source_detail_loss = (
                    source_detail.get("loss") if isinstance(source_detail.get("loss"), dict) else {}
                )
                source_detail_loss["scrapyParentInvalidPayload"] = int(parent_invalid_payload)
                source_detail["loss"] = source_detail_loss
                source_detail["keptCount"] = max(int(source_detail.get("keptCount") or 0), kept)
                source_detail["status"] = "ok"
                if not clean_text(source_detail.get("classification")):
                    source_detail["classification"] = "ok_with_jobs" if kept > 0 else "ok_no_jobs"
                source_detail["browserFallbackRecommended"] = False
            else:
                source_detail["status"] = "error"
                if not clean_text(source_detail.get("error")):
                    source_detail["error"] = "crawl failed"
                source_detail["classification"] = "parse_error"
                errors_list.append(f"{source_name}: crawl failed")

            stats = envelope.get("stats")
            if isinstance(stats, dict):
                source_detail["stats"] = {
                    "downloader/request_count": _coerce_int(stats.get("downloader/request_count")),
                    "downloader/response_count": _coerce_int(
                        stats.get("downloader/response_count")
                    ),
                    "downloader/response_status_count/200": _coerce_int(
                        stats.get("downloader/response_status_count/200")
                    ),
                    "retry/count": _coerce_int(stats.get("retry/count")),
                    "item_scraped_count": _coerce_int(stats.get("item_scraped_count")),
                    "candidate_links_found": _coerce_int(stats.get("candidate_links_found")),
                    "detail_pages_visited": _coerce_int(stats.get("detail_pages_visited")),
                    "jobs_emitted": _coerce_int(stats.get("jobs_emitted")),
                    "jobs_rejected_validation": _coerce_int(stats.get("jobs_rejected_validation")),
                    "finish_reason": clean_text(stats.get("finish_reason")),
                }
                source_detail_loss = (
                    source_detail.get("loss") if isinstance(source_detail.get("loss"), dict) else {}
                )
                source_detail_loss["scrapyRunnerRejectedValidation"] = _coerce_int(
                    stats.get("jobs_rejected_validation")
                )
                source_detail_loss["scrapyDeadListingPageRejected"] = _coerce_int(
                    stats.get("dead_listing_pages_rejected")
                )
                source_detail["loss"] = source_detail_loss
                if int(source_detail.get("fetchedCount") or 0) <= 0:
                    source_detail["fetchedCount"] = int(
                        source_detail["stats"]["downloader/response_count"]
                    )

            _update_taxonomy_fields(source_detail)
            details.append(source_detail)
        except subprocess.TimeoutExpired:
            source_detail.update(
                {
                    "status": "error",
                    "error": "subprocess timeout",
                    "classification": "browser_timeout",
                    "browserFallbackRecommended": False,
                }
            )
            _update_taxonomy_fields(source_detail)
            errors_list.append(f"{source_name}: subprocess timeout")
            details.append(source_detail)
        except Exception as exc:  # noqa: BLE001
            source_detail.update(
                {
                    "status": "error",
                    "error": clean_text(exc)[:500],
                    "classification": "parse_error",
                    "browserFallbackRecommended": False,
                }
            )
            _update_taxonomy_fields(source_detail)
            errors_list.append(f"{source_name}: {type(exc).__name__}: {clean_text(exc)[:200]}")
            details.append(source_detail)

    set_source_diagnostics(
        "scrapy_static_sources",
        adapter="scrapy_static",
        studio="multiple",
        details=details,
        partial_errors=errors_list,
    )
    return results_list
