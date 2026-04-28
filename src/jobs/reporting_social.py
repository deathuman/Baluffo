"""Social experiment reporting helpers for jobs pipeline output."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from src.jobs.models import CanonicalJob
from src.jobs.text_utils import clean_text
from src.shared.json_shapes import as_json_list, as_json_object, json_object_rows

SOCIAL_EXPERIMENT_REVIEW_FILENAME = "social-experiment-review.json"
SOCIAL_EXPERIMENT_SAMPLE_SIZE = 50
OFFICIAL_BOARD_SOURCE_ADAPTERS = {
    "greenhouse",
    "teamtailor",
    "lever",
    "smartrecruiters",
    "workable",
    "recruitee",
    "pinpoint",
    "ashby",
    "bamboohr",
    "breezy",
    "jazzhr",
    "workday",
    "personio",
    "static",
}
OFFICIAL_BOARD_SOURCE_NAMES = {"epic_games_careers"}


def _canonical_sort_key(row: dict[str, Any]) -> tuple[str, int, str, str, str]:
    dedup_key = clean_text(row.get("dedupKey"))
    row_id = int(row.get("id") or 0)
    return (
        dedup_key or f"id:{row_id:020d}",
        row_id,
        clean_text(row.get("title")),
        clean_text(row.get("company")),
        clean_text(row.get("jobLink")),
    )


def _social_channel_for_source(source_name: Any) -> str:
    name = clean_text(source_name)
    if name == "social_reddit":
        return "reddit"
    if name == "social_mastodon":
        return "mastodon"
    if name == "social_x":
        return "x"
    return ""


def _source_bundle_items(row: dict[str, Any]) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], json_object_rows(row.get("sourceBundle")))


def _row_origin_info(row: dict[str, Any]) -> tuple[list[str], bool]:
    channels: set[str] = set()
    official = False
    for item in _source_bundle_items(row):
        channel = _social_channel_for_source(item.get("source"))
        if channel in {"reddit", "mastodon"}:
            channels.add(channel)
        source_name = clean_text(item.get("source"))
        adapter = clean_text(item.get("adapter"))
        if source_name in OFFICIAL_BOARD_SOURCE_NAMES or adapter in OFFICIAL_BOARD_SOURCE_ADAPTERS:
            official = True
    return sorted(channels), official


def build_social_experiment_review_sample(
    deduped_rows: Sequence[CanonicalJob],
    *,
    sample_size: int = SOCIAL_EXPERIMENT_SAMPLE_SIZE,
) -> list[dict[str, Any]]:
    sample: list[dict[str, Any]] = []
    for row in deduped_rows:
        payload = row.to_dict() if isinstance(row, CanonicalJob) else dict(row)
        channels, official = _row_origin_info(payload)
        if not channels:
            continue
        sample.append(
            {
                "dedupKey": clean_text(payload.get("dedupKey")),
                "id": int(payload.get("id") or 0),
                "title": clean_text(payload.get("title")),
                "company": clean_text(payload.get("company")),
                "jobLink": clean_text(payload.get("jobLink")),
                "channels": channels,
                "officialBoardOrigin": bool(official),
                "sourceBundleCount": int(payload.get("sourceBundleCount") or 0),
                "reviewDecision": clean_text(payload.get("reviewDecision")),
                "reviewNotes": clean_text(payload.get("reviewNotes")),
            }
        )
    sample.sort(key=_canonical_sort_key)
    return sample[: max(0, int(sample_size or 0))]


def build_social_experiment_review_payload(
    review_rows: Sequence[dict[str, Any]],
    *,
    generated_at: str,
    pilot_window_start_at: str,
    pilot_window_end_at: str,
    review_artifact_path: str,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    reviewed_count = 0
    false_positive_count = 0
    for row in json_object_rows(list(review_rows)):
        decision = clean_text(row.get("reviewDecision"))
        reviewed = decision in {"true_positive", "false_positive"}
        if reviewed:
            reviewed_count += 1
        if decision == "false_positive":
            false_positive_count += 1
        rows.append(
            {
                "dedupKey": clean_text(row.get("dedupKey")),
                "id": int(row.get("id") or 0),
                "title": clean_text(row.get("title")),
                "company": clean_text(row.get("company")),
                "jobLink": clean_text(row.get("jobLink")),
                "channels": [
                    clean_text(item)
                    for item in as_json_list(row.get("channels"))
                    if clean_text(item) in {"reddit", "mastodon", "x"}
                ],
                "officialBoardOrigin": bool(row.get("officialBoardOrigin")),
                "sourceBundleCount": int(row.get("sourceBundleCount") or 0),
                "reviewDecision": decision,
                "reviewNotes": clean_text(row.get("reviewNotes")),
            }
        )
    rows.sort(key=_canonical_sort_key)
    sample_size = len(rows) if reviewed_count > 0 else 0
    false_positive_rate = (
        float(false_positive_count) / float(reviewed_count) if reviewed_count > 0 else 0.0
    )
    return {
        "schemaVersion": 1,
        "generatedAt": clean_text(generated_at),
        "pilotWindowStartAt": clean_text(pilot_window_start_at),
        "pilotWindowEndAt": clean_text(pilot_window_end_at),
        "candidateCount": len(rows),
        "sampleSize": sample_size,
        "reviewedCount": reviewed_count,
        "falsePositiveCount": false_positive_count,
        "falsePositiveRate": false_positive_rate,
        "reviewArtifactPath": clean_text(review_artifact_path),
        "rows": rows,
    }


def summarize_social_experiment(
    source_reports: Sequence[dict[str, Any]],
    deduped_rows: Sequence[CanonicalJob],
    *,
    pilot_window_start_at: str,
    pilot_window_end_at: str,
    review_payload: dict[str, Any] | None = None,
    review_artifact_path: str = "",
) -> dict[str, Any]:
    social_rows = [
        row
        for row in json_object_rows(list(source_reports))
        if clean_text(row.get("name")) in {"social_reddit", "social_mastodon"}
    ]
    by_channel: dict[str, dict[str, Any]] = {}
    social_unique_total = 0
    social_overlap_total = 0
    for channel, source_name in {"reddit": "social_reddit", "mastodon": "social_mastodon"}.items():
        channel_report: dict[str, Any] = next(
            (row for row in social_rows if clean_text(row.get("name")) == source_name),
            {},
        )
        channel_kept = int(channel_report.get("keptCount") or 0)
        channel_low_conf = int(channel_report.get("lowConfidenceDropped") or 0)
        unique_count = 0
        overlap_count = 0
        for row in deduped_rows:
            payload = row.to_dict() if isinstance(row, CanonicalJob) else dict(row)
            channels, official = _row_origin_info(payload)
            if channel not in channels:
                continue
            if official:
                overlap_count += 1
            else:
                unique_count += 1
        duplicate_count = max(0, channel_kept - unique_count - overlap_count)
        duplicate_rate = (duplicate_count / channel_kept) if channel_kept > 0 else 0.0
        by_channel[channel] = {
            "keptCount": channel_kept,
            "uniqueKeptCount": unique_count,
            "officialBoardOverlapCount": overlap_count,
            "duplicateCount": duplicate_count,
            "duplicateRate": duplicate_rate,
            "lowConfidenceDropped": channel_low_conf,
        }
        social_unique_total += unique_count
        social_overlap_total += overlap_count

    kept_total = sum(int(row.get("keptCount") or 0) for row in social_rows)
    low_conf_total = sum(int(row.get("lowConfidenceDropped") or 0) for row in social_rows)
    duplicate_total = max(0, kept_total - social_unique_total - social_overlap_total)
    duplicate_rate_total = (duplicate_total / kept_total) if kept_total > 0 else 0.0
    review_payload = as_json_object(review_payload)
    reviewed_count = int(review_payload.get("reviewedCount") or 0)
    false_positive_count = int(review_payload.get("falsePositiveCount") or 0)
    false_positive_rate = float(review_payload.get("falsePositiveRate") or 0.0)
    candidate_count = int(review_payload.get("candidateCount") or 0)
    sample_size = candidate_count if reviewed_count > 0 else 0
    return {
        "pilotWindowStartAt": clean_text(pilot_window_start_at),
        "pilotWindowEndAt": clean_text(pilot_window_end_at),
        "scheduledRunCount": 1 if social_rows else 0,
        "keptCount": kept_total,
        "uniqueKeptCount": social_unique_total,
        "officialBoardOverlapCount": social_overlap_total,
        "duplicateCount": duplicate_total,
        "duplicateRate": duplicate_rate_total,
        "lowConfidenceDropped": low_conf_total,
        "sampleSize": sample_size,
        "reviewedCount": reviewed_count,
        "falsePositiveCount": false_positive_count,
        "falsePositiveRate": false_positive_rate if reviewed_count > 0 else 0.0,
        "reviewArtifactPath": clean_text(review_artifact_path),
        "channels": by_channel,
    }
