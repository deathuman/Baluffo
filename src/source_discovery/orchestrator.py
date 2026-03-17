from __future__ import annotations

"""End-to-end discovery orchestration (CLI + core flow).

Phases:
1. Candidate generation (curated seeds, sheet directory, provider patterns, Gamesmap, web search)
2. De-duplication across runs (IDs + endpoint fingerprints)
3. Probe (HTTP checks) with concurrency limits
4. Queue balancing (caps by adapter/domain, top-N)
5. Summary + report writing
"""

import argparse
import asyncio
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

import src.source_discovery as sd
from src.contracts import SCHEMA_VERSION
from src.source_registry import load_json_array, save_json_atomic, source_identity, unique_sources

from .core import (
    adapter_domain_fingerprint,
    apply_queue_balancing,
    classify_probe_failure_stage,
    compute_candidate_score,
    estimate_probe_priority,
    init_stage_counter,
    probe_bucket_for,
    probe_concurrency_defaults,
    should_queue_candidate,
    _evidence_threshold_for_probe,
)
from .gamesmap import discover_gamesmap_candidates
from .probe import async_probe_candidate, validate_candidate_for_probe
from .reporting import merge_candidate_streams, stage_curated_seed_candidates
from .scoring import resolve_discovery_thresholds
from .sheet_directory import discover_game_studio_sheet_candidates
from .static_candidates import build_static_candidate_from_page
from .web_search import (
    async_fetch_text_httpx,
    discover_web_search_candidates,
    fetch_text,
    infer_provider_candidates_from_html,
)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover new job source candidates.")
    parser.add_argument("--timeout", type=int, default=12)
    parser.add_argument("--top", type=int, default=0, help="Limit new candidates written this run; 0 = no limit.")
    parser.add_argument("--mode", choices=("dynamic", "static"), default="dynamic")
    parser.add_argument("--no-web-search", action="store_true", help="Disable lightweight web search phase.")
    parser.add_argument(
        "--gamesmap-website-only-fallback",
        action="store_true",
        help="Manual-only mode: include Gamesmap homepage-only candidates in this run.",
    )
    parser.add_argument(
        "--gamesmap-max-detail-pages",
        type=int,
        default=0,
        help="Optional Gamesmap crawl cap override for this run; 0 = config default.",
    )
    return parser.parse_args(argv)


def run_discovery(
    *,
    timeout_s: int,
    top_n: int,
    mode: str = "dynamic",
    include_web_search: bool = True,
    discovery_config: Optional[Dict[str, Any]] = None,
    fetcher=fetch_text,
) -> Dict[str, Any]:
    started_at = sd.now_iso()
    effective_config = discovery_config if isinstance(discovery_config, dict) else sd.load_discovery_config()
    thresholds = resolve_discovery_thresholds(effective_config)

    active = load_json_array(sd.ACTIVE_PATH, [])
    pending_existing = load_json_array(sd.PENDING_PATH, [])
    rejected = load_json_array(sd.REJECTED_PATH, [])

    sd.emit_log(
        f"Starting source discovery: mode={mode}, top_n={top_n}, web_search={'on' if include_web_search else 'off'}."
    )
    sd.emit_log(
        f"Loaded registries: active={len(active)}, pending={len(pending_existing)}, rejected={len(rejected)}."
    )

    existing_rows = [*active, *pending_existing, *rejected]
    seen_ids = {source_identity(row) for row in existing_rows if isinstance(row, dict)}
    seen_domains = {
        fp
        for fp in (
            adapter_domain_fingerprint(row) for row in existing_rows if isinstance(row, dict)
        )
        if fp
    }

    web_failures: List[Dict[str, Any]] = []
    streams: List[Tuple[str, List[Dict[str, Any]]]] = []

    sd.emit_log("Generating curated seed candidates from static discovery inputs.")
    curated_seed_candidates = stage_curated_seed_candidates()
    sd.emit_log(f"Curated seed generation complete: {len(curated_seed_candidates)} candidate(s).")
    streams.append(("curated_seed", curated_seed_candidates))

    sd.emit_log("Scanning game studios sheet directory for candidate sources.")
    provider_sheet_candidates, static_sheet_candidates, sheet_failures = discover_game_studio_sheet_candidates(
        timeout_s,
        sheet_id=str(getattr(sd, "GAME_STUDIOS_SHEET_ID", "")) or None,
        gid=str(getattr(sd, "GAME_STUDIOS_SHEET_GID", "")) or None,
        fetcher=fetcher,
    )
    sd.emit_log(
        "Game studios sheet scan complete: "
        f"provider={len(provider_sheet_candidates)}, static={len(static_sheet_candidates)}, failures={len(sheet_failures)}."
    )
    # In unit tests we often supply a fetcher that only knows about the URLs
    # relevant to that test. Treat an empty sheet result as a soft failure in
    # that case so tests don't have to stub out the sheet directory URLs.
    if sheet_failures:
        if fetcher is fetch_text or (provider_sheet_candidates or static_sheet_candidates):
            web_failures.extend(sheet_failures)
    streams.append(("sheet_directory", provider_sheet_candidates))
    streams.append(("sheet_directory", static_sheet_candidates))

    if mode == "dynamic":
        sd.emit_log("Generating provider-pattern candidates from the studio seed catalog.")
        provider_pattern_candidates = sd.build_pattern_candidates()
        sd.emit_log(f"Provider-pattern generation complete: {len(provider_pattern_candidates)} candidate(s).")
        streams.append(("provider_pattern", provider_pattern_candidates))

        sd.emit_log("Scanning known careers pages from the seed catalog.")
        provider_web_candidates, static_web_candidates, seed_failures = sd.discover_seed_careers_page_candidates(
            timeout_s, fetcher=fetcher
        )
        sd.emit_log(
            "Seed careers scan complete: "
            f"provider={len(provider_web_candidates)}, static={len(static_web_candidates)}, failures={len(seed_failures)}."
        )
        web_failures.extend(seed_failures)
        streams.append(("web_provider", provider_web_candidates))
        streams.append(("generic_static", static_web_candidates))

        sd.emit_log("Scanning Gamesmap directory for discoverable studios.")
        provider_gamesmap_candidates, static_gamesmap_candidates, gamesmap_failures = discover_gamesmap_candidates(
            timeout_s,
            config=effective_config,
            fetcher=fetcher,
        )
        sd.emit_log(
            "Gamesmap scan complete: "
            f"provider={len(provider_gamesmap_candidates)}, static={len(static_gamesmap_candidates)}, failures={len(gamesmap_failures)}."
        )
        web_failures.extend(gamesmap_failures)
        streams.append(("web_provider", provider_gamesmap_candidates))
        streams.append(("generic_static", static_gamesmap_candidates))

        if include_web_search:
            sd.emit_log("Running web-search discovery queries.")
            provider_search_candidates, static_search_candidates, search_failures = discover_web_search_candidates(
                timeout_s,
                studio_seeds=list(getattr(sd, "STUDIO_SEEDS", [])),
                fetcher=fetcher,
            )
            sd.emit_log(
                "Web-search discovery complete: "
                f"provider={len(provider_search_candidates)}, static={len(static_search_candidates)}, failures={len(search_failures)}."
            )
            web_failures.extend(search_failures)
            streams.append(("web_provider", provider_search_candidates))
            streams.append(("generic_static", static_search_candidates))

    generated_count_by_stage = init_stage_counter()
    survived_dedupe_count_by_stage = init_stage_counter()
    probed_count_by_stage = init_stage_counter()
    queued_count_by_stage = init_stage_counter()
    duplicate_reasons: Counter[str] = Counter()
    dedupe_drop_rows: List[Dict[str, Any]] = []

    discovered = merge_candidate_streams(streams)
    for row in discovered:
        generated_count_by_stage[str(row.get("discoveryStage") or "provider_pattern")] += 1
    found_endpoint_count = len(discovered)
    sd.emit_log(
        "Generated candidates by stage: "
        + ", ".join(f"{stage}={generated_count_by_stage.get(stage, 0)}" for stage in sd.DISCOVERY_STAGES)
        + f" (total={found_endpoint_count})."
    )

    filtered: List[Dict[str, Any]] = []
    skipped_duplicate_count = 0
    local_seen_ids = set(seen_ids)
    local_seen_domains = set(seen_domains)
    for row in discovered:
        stage = str(row.get("discoveryStage") or "provider_pattern")
        row_id = source_identity(row)
        row_domain = adapter_domain_fingerprint(row)
        if row_id in seen_ids:
            skipped_duplicate_count += 1
            duplicate_reasons["existing_id"] += 1
            dedupe_drop_rows.append(
                {
                    "name": row.get("name"),
                    "adapter": row.get("adapter"),
                    "stage": "dedupe_skipped",
                    "error": "existing_id",
                    "dropStage": "dedupe_skipped",
                    "dropReason": "existing_id",
                }
            )
            continue
        if row_domain and row_domain in seen_domains:
            skipped_duplicate_count += 1
            duplicate_reasons["existing_domain"] += 1
            dedupe_drop_rows.append(
                {
                    "name": row.get("name"),
                    "adapter": row.get("adapter"),
                    "stage": "dedupe_skipped",
                    "error": "existing_domain",
                    "dropStage": "dedupe_skipped",
                    "dropReason": "existing_domain",
                }
            )
            continue
        if row_id in local_seen_ids:
            skipped_duplicate_count += 1
            duplicate_reasons["run_id"] += 1
            dedupe_drop_rows.append(
                {
                    "name": row.get("name"),
                    "adapter": row.get("adapter"),
                    "stage": "dedupe_skipped",
                    "error": "run_id",
                    "dropStage": "dedupe_skipped",
                    "dropReason": "run_id",
                }
            )
            continue
        if row_domain and row_domain in local_seen_domains:
            skipped_duplicate_count += 1
            duplicate_reasons["run_domain"] += 1
            dedupe_drop_rows.append(
                {
                    "name": row.get("name"),
                    "adapter": row.get("adapter"),
                    "stage": "dedupe_skipped",
                    "error": "run_domain",
                    "dropStage": "dedupe_skipped",
                    "dropReason": "run_domain",
                }
            )
            continue
        local_seen_ids.add(row_id)
        if row_domain:
            local_seen_domains.add(row_domain)
        survived_dedupe_count_by_stage[stage] += 1
        filtered.append(row)

    filtered.sort(key=estimate_probe_priority, reverse=True)
    sd.emit_log(
        "After dedupe: "
        + ", ".join(f"{stage}={survived_dedupe_count_by_stage.get(stage, 0)}" for stage in sd.DISCOVERY_STAGES)
        + f"; skipped_duplicates={skipped_duplicate_count}."
    )

    queueable_candidates: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = [
        {**row, "dropStage": "page_fetch", "dropReason": "page_fetch"}
        for row in list(web_failures)
        if isinstance(row, dict)
    ]
    failures.extend(dedupe_drop_rows)

    healthy = 0
    probed = 0
    adapter_counter: Counter[str] = Counter()
    method_counter: Counter[str] = Counter()
    skipped_invalid = 0
    skipped_low_evidence_probe_count = 0
    low_evidence_probes_used = 0
    validation_skipped_count = 0
    queue_filtered_count = 0
    probe_failed_count = 0

    def build_summary(
        current_candidates: List[Dict[str, Any]],
        deferred_candidates: int = 0,
        deferred_counts: Optional[Dict[str, int]] = None,
        *,
        phase: str = "",
        phase_label: str = "",
    ) -> Dict[str, Any]:
        deferred_reason_rows = deferred_counts or {}
        deferred_by_cap = int(sum(int(value or 0) for value in deferred_reason_rows.values()))
        return {
            "phase": str(phase or ""),
            "phaseLabel": str(phase_label or ""),
            "probedCount": probed,
            "healthyCount": healthy,
            "newCandidateCount": len(current_candidates),
            "taEnvCandidateCount": sum(
                1 for row in current_candidates if "target_role_signal" in row.get("reasons", [])
            ),
            "nlCandidateCount": sum(1 for row in current_candidates if bool(row.get("nlPriority"))),
            "failedProbeCount": len([row for row in failures if str(row.get("stage")) == "probe"]),
            "probeMissCount": len([row for row in failures if str(row.get("stage")) == "probe_miss"]),
            "foundEndpointCount": found_endpoint_count,
            "probedCandidateCount": probed,
            "queuedCandidateCount": len([row for row in current_candidates if not bool(row.get("deferred"))]),
            "discoverableButDeferredCount": int(deferred_candidates),
            "skippedDuplicateCount": skipped_duplicate_count,
            "skippedInvalidCount": skipped_invalid,
            "skippedLowEvidenceProbeCount": skipped_low_evidence_probe_count,
            "adapterCounts": dict(adapter_counter),
            "methodCounts": dict(method_counter),
            "generatedCountByStage": dict(generated_count_by_stage),
            "survivedDedupeCountByStage": dict(survived_dedupe_count_by_stage),
            "probedCountByStage": dict(probed_count_by_stage),
            "queuedCountByStage": dict(queued_count_by_stage),
            "duplicateReasons": dict(duplicate_reasons),
            "deferredReasons": dict(deferred_counts or {}),
            "thresholds": dict(thresholds),
            "lossAccounting": {
                "generated": int(found_endpoint_count),
                "dedupSkipped": int(skipped_duplicate_count),
                "dedupSkippedReasons": dict(duplicate_reasons),
                "validationSkipped": int(validation_skipped_count),
                "lowEvidenceSkipped": int(skipped_low_evidence_probe_count),
                "probeFailed": int(probe_failed_count),
                "queueFiltered": int(queue_filtered_count),
                "deferredByCap": deferred_by_cap,
                "queued": int(len([row for row in current_candidates if not bool(row.get("deferred"))])),
            },
        }

    def write_progress_report(current_candidates: List[Dict[str, Any]], *, phase: str, phase_label: str) -> None:
        save_json_atomic(
            sd.DISCOVERY_REPORT_PATH,
            {
                "schemaVersion": SCHEMA_VERSION,
                "mode": mode,
                "startedAt": started_at,
                "finishedAt": "",
                "summary": build_summary(current_candidates, phase=phase, phase_label=phase_label),
                "candidates": current_candidates,
                "failures": failures,
                "topFailures": [],
                "outputs": {
                    "report": str(sd.DISCOVERY_REPORT_PATH),
                    "candidates": str(sd.DISCOVERY_CANDIDATES_PATH),
                    "pending": str(sd.PENDING_PATH),
                },
            },
        )

    write_progress_report([], phase="candidate_generation", phase_label="Generating initial discovery candidates")
    sd.emit_log(f"Starting probe phase for {len(filtered)} candidate(s).")
    write_progress_report(queueable_candidates, phase="probe", phase_label=f"Probing {len(filtered)} candidate(s)")

    probe_inputs: List[Dict[str, Any]] = []
    for raw in filtered:
        stage = str(raw.get("discoveryStage") or "provider_pattern")
        valid, invalid_reason = validate_candidate_for_probe(raw)
        if not valid:
            skipped_invalid += 1
            validation_skipped_count += 1
            failures.append(
                {
                    "name": raw.get("name"),
                    "adapter": raw.get("adapter"),
                    "domain": (urlparse(sd.endpoint_url(raw)).netloc or "").lower(),
                    "error": invalid_reason,
                    "stage": "validation",
                    "dropStage": "validation",
                    "dropReason": "validation",
                }
            )
            continue
        evidence_score = int(raw.get("evidenceScore") or 0)
        threshold = _evidence_threshold_for_probe(raw, thresholds)
        if evidence_score < threshold:
            if stage == "provider_pattern":
                skipped_low_evidence_probe_count += 1
                failures.append(
                    {
                        "name": raw.get("name"),
                        "adapter": raw.get("adapter"),
                        "domain": (urlparse(sd.endpoint_url(raw)).netloc or "").lower(),
                        "error": f"pattern evidence score {evidence_score} below probe threshold {threshold}",
                        "stage": "probe_skipped",
                        "dropStage": "low_evidence_skipped",
                        "dropReason": "probe_threshold",
                    }
                )
                continue
            if low_evidence_probes_used >= int(thresholds.get("lowEvidenceProbeLimit", sd.LOW_EVIDENCE_PROBE_LIMIT)):
                skipped_low_evidence_probe_count += 1
                failures.append(
                    {
                        "name": raw.get("name"),
                        "adapter": raw.get("adapter"),
                        "domain": (urlparse(sd.endpoint_url(raw)).netloc or "").lower(),
                        "error": f"evidence score {evidence_score} below probe threshold {threshold}",
                        "stage": "probe_skipped",
                        "dropStage": "low_evidence_skipped",
                        "dropReason": "low_evidence_probe_cap",
                    }
                )
                continue
            low_evidence_probes_used += 1
        probe_inputs.append(raw)

    async def _run_probe_batch(rows: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], bool, int, str]]:
        limits = probe_concurrency_defaults()
        total_sem = asyncio.Semaphore(int(limits["total"]))
        bucket_sems = {
            "static": asyncio.Semaphore(int(limits["static"])),
            "provider": asyncio.Semaphore(int(limits["provider"])),
            "teamtailor": asyncio.Semaphore(int(limits["teamtailor"])),
        }

        async def _call_fetch(url: str, t: int) -> str:
            if fetcher is not fetch_text:
                return await asyncio.to_thread(fetcher, url, t)
            return await async_fetch_text_httpx(client, url, t)

        async def _probe_one(row: Dict[str, Any]) -> Tuple[Dict[str, Any], bool, int, str]:
            bucket = probe_bucket_for(row)
            bucket_sem = bucket_sems.get(bucket, bucket_sems["provider"])
            async with total_sem:
                async with bucket_sem:
                    ok, jobs_found, error = await async_probe_candidate(row, timeout_s, fetcher=_call_fetch)
                    return row, ok, jobs_found, error

        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_s)) as client:
            tasks = [asyncio.create_task(_probe_one(row)) for row in rows]
            results: List[Tuple[Dict[str, Any], bool, int, str]] = []
            for fut in asyncio.as_completed(tasks):
                results.append(await fut)
            return results

    completed = 0
    for raw, ok, jobs_found, error in asyncio.run(_run_probe_batch(probe_inputs)):
        completed += 1
        stage = str(raw.get("discoveryStage") or "provider_pattern")
        evidence_score = int(raw.get("evidenceScore") or 0)
        probed += 1
        probed_count_by_stage[stage] += 1

        if not ok:
            probe_failed_count += 1
            probe_stage = classify_probe_failure_stage(error)
            failures.append(
                {
                    "name": raw.get("name"),
                    "adapter": raw.get("adapter"),
                    "domain": (urlparse(sd.endpoint_url(raw)).netloc or "").lower(),
                    "error": error,
                    "stage": probe_stage,
                    "dropStage": "probe_failed",
                    "dropReason": probe_stage,
                }
            )
        elif not should_queue_candidate(raw, jobs_found, thresholds):
            queue_filtered_count += 1
            failures.append(
                {
                    "name": raw.get("name"),
                    "adapter": raw.get("adapter"),
                    "domain": (urlparse(sd.endpoint_url(raw)).netloc or "").lower(),
                    "error": f"candidate passed probe but evidence {evidence_score} is below queue threshold",
                    "stage": "queue_filtered",
                    "dropStage": "queue_filtered",
                    "dropReason": "queue_threshold",
                }
            )
        else:
            healthy += 1
            score, reasons = compute_candidate_score(raw, jobs_found)
            normalized = sd.normalize_candidate(raw, score, reasons, jobs_found, probed_at=sd.now_iso())
            queueable_candidates.append(normalized)
            adapter_counter[str(normalized.get("adapter") or "unknown")] += 1
            method_counter[str(normalized.get("discoveryMethod") or "unknown")] += 1

        if completed % 10 == 0:
            sd.emit_log(
                f"Progress: completed={completed}/{len(probe_inputs)}, probed={probed}, queued={len(queueable_candidates)}, "
                f"probe_misses={len([row for row in failures if str(row.get('stage')) == 'probe_miss'])}, "
                f"skipped_low_evidence={skipped_low_evidence_probe_count}."
            )
            write_progress_report(queueable_candidates, phase="probe", phase_label=f"Probing {len(filtered)} candidate(s)")

    queued_candidates, report_candidates, deferred_reason_counts = apply_queue_balancing(queueable_candidates, top_n)
    for row in report_candidates:
        if not isinstance(row, dict):
            continue
        if bool(row.get("deferred")):
            row["dropStage"] = "deferred_by_cap"
            row["dropReason"] = str(row.get("deferReason") or "deferred")
    for row in queued_candidates:
        queued_count_by_stage[str(row.get("discoveryStage") or "provider_pattern")] += 1

    sd.emit_log(
        f"Probe phase finished: healthy={healthy}, queued={len(queued_candidates)}, "
        f"deferred={len([row for row in report_candidates if bool(row.get('deferred'))])}, probe_misses={len([row for row in failures if str(row.get('stage')) == 'probe_miss'])}."
    )

    save_json_atomic(sd.PENDING_PATH, unique_sources([*pending_existing, *queued_candidates]))
    save_json_atomic(sd.DISCOVERY_CANDIDATES_PATH, queued_candidates)

    summary = build_summary(
        report_candidates,
        deferred_candidates=len([row for row in report_candidates if bool(row.get("deferred"))]),
        deferred_counts=deferred_reason_counts,
        phase="completed",
        phase_label="Discovery completed",
    )
    failure_counter: Counter[str] = Counter()
    for row in failures:
        adapter = str(row.get("adapter") or "unknown")
        domain = str(row.get("domain") or "").strip()
        failure_counter[f"{adapter}:{domain}" if domain else adapter] += 1

    report = {
        "schemaVersion": SCHEMA_VERSION,
        "mode": mode,
        "startedAt": started_at,
        "finishedAt": sd.now_iso(),
        "summary": summary,
        "candidates": report_candidates,
        "failures": failures,
        "topFailures": [{"key": key, "count": count} for key, count in failure_counter.most_common(5)],
        "outputs": {
            "report": str(sd.DISCOVERY_REPORT_PATH),
            "candidates": str(sd.DISCOVERY_CANDIDATES_PATH),
            "pending": str(sd.PENDING_PATH),
        },
    }
    save_json_atomic(sd.DISCOVERY_REPORT_PATH, report)
    sd.emit_log(f"Discovery report written to {sd.DISCOVERY_REPORT_PATH}.")
    return report


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    discovery_config = sd.load_discovery_config()
    if bool(getattr(args, "gamesmap_website_only_fallback", False)):
        gamesmap_cfg = dict(discovery_config.get("gamesmap") or {})
        gamesmap_cfg["websiteOnlyFallback"] = True
        gamesmap_cfg["websiteOnlyManualOnly"] = True
        discovery_config["gamesmap"] = gamesmap_cfg
    if int(getattr(args, "gamesmap_max_detail_pages", 0) or 0) > 0:
        gamesmap_cfg = dict(discovery_config.get("gamesmap") or {})
        gamesmap_cfg["maxDetailPages"] = int(args.gamesmap_max_detail_pages)
        discovery_config["gamesmap"] = gamesmap_cfg
    report = run_discovery(
        timeout_s=int(args.timeout),
        top_n=int(args.top),
        mode=str(args.mode),
        include_web_search=not bool(args.no_web_search),
        discovery_config=discovery_config,
    )
    sd.emit_log(
        "Source discovery completed. "
        f"Found endpoints: {report['summary']['foundEndpointCount']}. "
        f"Queued candidates: {report['summary']['queuedCandidateCount']}. "
        f"Deferred candidates: {report['summary'].get('discoverableButDeferredCount', 0)}. "
        f"Failed probes: {report['summary'].get('failedProbeCount', 0)}. "
        f"Probe misses: {report['summary'].get('probeMissCount', 0)}. "
        f"Report: {report['outputs']['report']}"
    )
    return 0

