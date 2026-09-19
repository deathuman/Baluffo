#!/usr/bin/env python3
"""Artifact loading, source identity and registry lookup helpers.

Leaf of ``scripts/source_policy_soak_report.py``; every unit body is byte-identical to the pre-split
module. The coordinator imports and re-exports these names.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root

from scripts.source_policy_soak_report_spec import (
    ARTIFACT_PATHS,
    LEAN_REGISTRY_ARTIFACT_NAMES,
    REGISTRY_SEED_PATHS,
    STATIC_LIKE_ADAPTERS,
    _int_value,
    as_json_list,
    as_json_object,
    clean_text,
    json_object_rows,
    load_registry_json_array,
    norm_text,
    read_json,
    runtime_static_source_name_for_registry_row,
    source_identity,
)

__all__ = [
    "ARTIFACT_PATHS",
    "Any",
    "LEAN_REGISTRY_ARTIFACT_NAMES",
    "REGISTRY_SEED_PATHS",
    "STATIC_LIKE_ADAPTERS",
    "_artifact_inputs",
    "_discovery_rows",
    "_fetch_only_sources_mode",
    "_find_likely_registry_row",
    "_find_linked_static_registry_row",
    "_find_provider_registry_row",
    "_find_registry_row",
    "_first_clean_text",
    "_first_int_value",
    "_gate",
    "_int_value",
    "_list_rows",
    "_pair_key",
    "_policy_pairs",
    "_provider_counts",
    "_provider_coverage_status_by_token",
    "_read_json_artifact",
    "_source_evidence_rows",
    "_source_identity_tokens",
    "_source_name",
    "_source_row_excluded_by_cache",
    "_source_row_tokens",
    "_source_state_rows",
    "_source_state_token_index",
    "_source_token_index",
    "_static_loader_name_for_registry_row",
    "_static_loader_name_index",
    "_static_registry_identity_tokens",
    "_static_source_url",
    "_unique_text",
    "_warning_gate",
    "as_json_list",
    "as_json_object",
    "clean_text",
    "json",
    "json_object_rows",
    "load_registry_json_array",
    "norm_text",
    "read_json",
    "runtime_static_source_name_for_registry_row",
    "source_identity",
]


def _read_json_artifact(path: Path) -> tuple[Any, str, str]:
    source_path = path
    status = "ok"
    if not source_path.exists():
        gzip_path = path.with_name(path.name + ".gz")
        seed_rel_path = REGISTRY_SEED_PATHS.get(path.name)
        seed_path = path.parent / seed_rel_path if seed_rel_path else None
        if gzip_path.exists():
            source_path = gzip_path
        elif seed_path is not None and seed_path.exists():
            source_path = seed_path
            status = "seed"
        else:
            return {}, "missing", ""
    try:
        payload = read_json(source_path, None)
        if payload is None:
            return {}, "malformed", f"{source_path.name} is malformed"
        if path.name in LEAN_REGISTRY_ARTIFACT_NAMES:
            payload = load_registry_json_array(path, [])
    except (OSError, json.JSONDecodeError) as exc:
        return {}, "malformed", f"{source_path.name} is malformed: {exc}"
    return payload, status, ""


def _artifact_inputs(data_dir: Path) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    payloads: dict[str, Any] = {}
    inputs: dict[str, Any] = {}
    warnings: list[str] = []
    for key, filename in ARTIFACT_PATHS.items():
        path = data_dir / filename
        payload, status, warning = _read_json_artifact(path)
        payloads[key] = payload
        inputs[key] = {"path": str(path), "status": status}
        if warning:
            warnings.append(warning)
    if inputs["jobsFetchReport"]["status"] == "missing":
        warnings.append("jobs-fetch-report.json is missing; runtime fetch evidence is unavailable.")
    return payloads, inputs, warnings


def _gate(
    gate_id: str,
    status: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": gate_id,
        "status": status,
        "message": message,
        "details": details or {},
    }


def _warning_gate(
    gate_id: str, message: str, details: dict[str, Any] | None = None
) -> dict[str, Any]:
    return _gate(gate_id, "warning", message, details)


def _source_state_rows(payload: Any) -> dict[str, dict[str, Any]]:
    sources = as_json_object(payload).get("sources")
    if not isinstance(sources, dict):
        return {}
    return {clean_text(key): value for key, value in sources.items() if isinstance(value, dict)}


def _first_clean_text(mapping: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = clean_text(mapping.get(key))
        if value:
            return value
    return ""


def _first_int_value(mapping: dict[str, Any], *keys: str) -> int:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return _int_value(value)
    return 0


def _list_rows(payload: Any) -> list[dict[str, Any]]:
    return json_object_rows(payload)


def _discovery_rows(report_payload: Any, candidates_payload: Any) -> list[dict[str, Any]]:
    rows = []
    rows.extend(json_object_rows(as_json_object(report_payload).get("candidates")))
    rows.extend(_list_rows(candidates_payload))
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        key = (
            clean_text(row.get("sourceId"))
            or clean_text(row.get("name"))
            or json.dumps(row, sort_keys=True)
        )
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique


def _source_name(row: dict[str, Any]) -> str:
    return clean_text(row.get("name") or row.get("source") or row.get("sourceId"))


def _source_identity_tokens(row: dict[str, Any]) -> set[str]:
    tokens = {
        clean_text(row.get("id")),
        clean_text(row.get("sourceId")),
        clean_text(row.get("name")),
        clean_text(row.get("source")),
        clean_text(row.get("sourceIdentity")),
        clean_text(row.get("migrationSourceIdentity")),
    }
    return {token for token in tokens if token}


def _pair_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        norm_text(row.get("staticSourceId")) or norm_text(row.get("staticSourceName")),
        norm_text(row.get("providerSourceId")) or norm_text(row.get("providerSourceName")),
    )


def _policy_pairs(policy: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        *json_object_rows(policy.get("suppressedPairs")),
        *json_object_rows(policy.get("pausedPairs")),
        *json_object_rows(policy.get("warningPairs")),
    ]


def _provider_counts(provider_coverage: dict[str, Any]) -> dict[str, int]:
    status_counts = as_json_object(provider_coverage.get("statusCounts"))
    validated = int(status_counts.get("validated_provider") or 0)
    unstable = int(status_counts.get("unstable_provider") or 0)
    failed = int(status_counts.get("failed_provider") or 0)
    return {
        "validatedProviderCount": validated,
        "unstableFailedProviderCount": unstable + failed,
    }


def _source_token_index(rows: list[dict[str, Any]]) -> set[str]:
    tokens: set[str] = set()
    for row in rows:
        tokens.update(_source_identity_tokens(row))
    return tokens


def _source_state_token_index(source_state_rows: dict[str, dict[str, Any]]) -> set[str]:
    tokens: set[str] = set()
    for name, row in source_state_rows.items():
        tokens.add(clean_text(name))
        tokens.update(_source_identity_tokens(as_json_object(row)))
    return {token for token in tokens if token}


def _provider_coverage_status_by_token(
    provider_coverage: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]],
) -> dict[str, str]:
    status_by_token: dict[str, str] = {}
    for name, raw in source_state_rows.items():
        row = as_json_object(raw)
        status = clean_text(row.get("providerCoverageStatus"))
        if not status:
            continue
        for token in {clean_text(name), *_source_identity_tokens(row)}:
            if token:
                status_by_token[token] = status
    for key in (
        "probingProviders",
        "validatedProviders",
        "unstableOrFailedProviders",
        "needsReviewProviders",
        "readyLaterProviders",
    ):
        for row in json_object_rows(provider_coverage.get(key)):
            status = clean_text(row.get("providerCoverageStatus"))
            if not status:
                continue
            for token in _source_identity_tokens(row):
                status_by_token[token] = status
    return status_by_token


def _source_row_tokens(row: dict[str, Any]) -> set[str]:
    tokens = set(_source_identity_tokens(row))
    name = clean_text(row.get("name"))
    if name.startswith("static_source::"):
        tokens.add(name[len("static_source::") :])
    return {token for token in tokens if token}


def _source_evidence_rows(source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    evidence_rows: list[dict[str, Any]] = []
    for row in source_rows:
        evidence_rows.append(row)
        source_loader_name = clean_text(row.get("name"))
        adapter = clean_text(row.get("adapter"))
        for detail in json_object_rows(row.get("details")):
            detail_row = dict(detail)
            if source_loader_name and not clean_text(detail_row.get("sourceLoaderName")):
                detail_row["sourceLoaderName"] = source_loader_name
            if adapter and not clean_text(detail_row.get("adapter")):
                detail_row["adapter"] = adapter
            evidence_rows.append(detail_row)
    return evidence_rows


def _find_registry_row(
    *,
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]] | None = None,
    identity: str,
) -> tuple[str, dict[str, Any]] | None:
    target = clean_text(identity)
    if not target:
        return None
    for bucket, rows in (
        ("active", active_rows),
        ("pending", pending_rows),
        ("rejected", rejected_rows or []),
    ):
        for row in rows:
            if target in _source_identity_tokens(row):
                return bucket, row
    return None


def _static_registry_identity_tokens(row: dict[str, Any]) -> set[str]:
    url = _static_source_url(row)
    tokens = {
        clean_text(row.get("id")),
        clean_text(row.get("sourceId")),
        clean_text(row.get("sourceIdentity")),
        clean_text(row.get("staticSourceId")),
        clean_text(source_identity(row)),
        url,
        f"static:listing_url:{url}" if url else "",
    }
    return {token for token in tokens if token}


def _find_linked_static_registry_row(
    *,
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    identity: str,
) -> tuple[str, dict[str, Any]] | None:
    target = clean_text(identity)
    if not target:
        return None
    bucket_rows = (
        ("active", active_rows),
        ("pending", pending_rows),
        ("rejected", rejected_rows),
    )
    for bucket, rows in bucket_rows:
        for row in rows:
            if (
                target in _static_registry_identity_tokens(row)
                and clean_text(row.get("adapter")) in STATIC_LIKE_ADAPTERS
            ):
                return bucket, row
    for bucket, rows in bucket_rows:
        for row in rows:
            if target in _static_registry_identity_tokens(row):
                return bucket, row
    return None


def _static_loader_name_for_registry_row(row: dict[str, Any]) -> str:
    return runtime_static_source_name_for_registry_row(row)


def _unique_text(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def _static_loader_name_index(rows: list[dict[str, Any]]) -> dict[str, str]:
    index: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = _static_loader_name_for_registry_row(row)
        for token in {*_source_identity_tokens(row), source_identity(row)}:
            index.setdefault(token, name)
    return index


def _static_source_url(row: dict[str, Any]) -> str:
    url = clean_text(row.get("listing_url") or row.get("careersUrl") or row.get("url"))
    if url:
        return url
    pages = as_json_list(row.get("pages"))
    for item in pages:
        url = clean_text(item)
        if url:
            return url
    return ""


def _find_likely_registry_row(
    *,
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
    identity: str,
    static_name: str,
) -> tuple[str, dict[str, Any]] | None:
    target_name = norm_text(static_name)
    target_url = norm_text(identity.removeprefix("static:listing_url:"))
    for bucket, rows in (
        ("active", active_rows),
        ("pending", pending_rows),
        ("rejected", rejected_rows),
    ):
        for row in rows:
            if target_name and target_name == norm_text(row.get("name")):
                return bucket, row
            if target_url and target_url == norm_text(_static_source_url(row)):
                return bucket, row
    return None


def _fetch_only_sources_mode(fetch_report: dict[str, Any]) -> bool:
    runtime = as_json_object(fetch_report.get("runtime"))
    if as_json_list(runtime.get("onlySources")):
        return True
    if as_json_list(fetch_report.get("onlySources")):
        return True
    args = as_json_object(runtime.get("args"))
    return bool(as_json_list(args.get("onlySources")) or clean_text(args.get("onlySources")))


def _source_row_excluded_by_cache(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    cache_decision = clean_text(row.get("cacheDecision"))
    exclusion_reason = clean_text(row.get("exclusionReason"))
    return cache_decision in {"skip_fresh", "cooldown_skip"} or exclusion_reason.startswith(
        "cache_"
    )


def _find_provider_registry_row(
    *,
    active_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    source_name: str,
    source_state_row: dict[str, Any],
) -> tuple[str, dict[str, Any]] | None:
    target_name = clean_text(source_name)
    target_adapter = clean_text(
        source_state_row.get("lastAdapter") or source_state_row.get("adapter")
    )
    for bucket, rows in (("active", active_rows), ("pending", pending_rows)):
        for row in rows:
            if target_name and clean_text(row.get("name")) == target_name:
                return bucket, row
            if (
                target_name
                and target_adapter
                and clean_text(row.get("studio")) == target_name
                and clean_text(row.get("adapter")) == target_adapter
            ):
                return bucket, row
    return None
