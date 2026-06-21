"""Data-only models for the jobs pipeline package.

AI boundary owns: jobs pipeline dataclasses, typed dicts, and CanonicalJob wire-model fields.
AI boundary implement in: this file for data models only; normalization and validation belong in contracts/canonicalization leaves.
AI boundary search before contracts: DATA_CONTRACT.md, core schemas/contracts, jobs contracts, and frontend job consumers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused jobs contract tests.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, TypedDict

from src.jobs.text_utils import clean_text

RawJob = dict[str, Any]
RawJobLike = Mapping[str, Any]


class SourceConfig(TypedDict, total=False):
    name: str
    studio: str
    adapter: str
    enabledByDefault: bool
    fetchStrategy: str
    cadenceMinutes: int


@dataclass(frozen=True, slots=True)
class RequestConfig:
    timeout_s: int
    headers: dict[str, str] = field(default_factory=dict)
    user_agent: str = ""
    proxy_url: str = ""


@dataclass(frozen=True, slots=True)
class SourceDiagnostics:
    adapter: str
    studio: str
    details: list[dict[str, Any]] = field(default_factory=list)
    partial_errors: list[str] = field(default_factory=list)
    low_confidence_dropped: int = 0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["partialErrors"] = payload.pop("partial_errors")
        payload["lowConfidenceDropped"] = payload.pop("low_confidence_dropped")
        return payload


@dataclass(frozen=True, slots=True)
class FetchContext:
    source_name: str
    request: RequestConfig
    retries: int
    backoff_s: float
    fetched_at: str = ""


@dataclass(frozen=True, slots=True)
class FetchResult:
    jobs: list[RawJob] = field(default_factory=list)
    diagnostics: SourceDiagnostics | None = None
    duration_ms: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CanonicalJob:
    id: Any = ""
    title: str = ""
    company: str = ""
    city: str = ""
    country: str = ""
    workType: str = ""
    contractType: str = ""
    jobLink: str = ""
    sector: str = ""
    profession: str = ""
    companyType: str = ""
    description: str = ""
    source: str = ""
    sourceJobId: str = ""
    fetchedAt: str = ""
    postedAt: str = ""
    status: str = ""
    firstSeenAt: str = ""
    lastSeenAt: str = ""
    removedAt: str = ""
    lifecycleEvent: str = ""
    lifecycleReason: str = ""
    dedupKey: str = ""
    qualityScore: int = 0
    focusScore: int = 0
    sourceBundleCount: int = 0
    sourceBundle: list[dict[str, Any]] = field(default_factory=list)
    locations: list[dict[str, Any]] = field(default_factory=list)
    locationSummary: str = ""
    adapter: str = ""
    studio: str = ""

    @classmethod
    def from_mapping(cls, payload: RawJobLike) -> CanonicalJob:
        data = dict(payload)
        locations_raw = data.get("locations") or []
        if isinstance(locations_raw, str):
            try:
                locations_raw = json.loads(locations_raw)
            except json.JSONDecodeError:
                locations_raw = []
        return cls(
            id=data.get("id", ""),
            title=clean_text(data.get("title")),
            company=clean_text(data.get("company")),
            city=clean_text(data.get("city")),
            country=clean_text(data.get("country")),
            workType=clean_text(data.get("workType")),
            contractType=clean_text(data.get("contractType")),
            jobLink=clean_text(data.get("jobLink")),
            sector=clean_text(data.get("sector")),
            profession=clean_text(data.get("profession")),
            companyType=clean_text(data.get("companyType")),
            description=clean_text(data.get("description")),
            source=clean_text(data.get("source")),
            sourceJobId=clean_text(data.get("sourceJobId")),
            fetchedAt=clean_text(data.get("fetchedAt")),
            postedAt=clean_text(data.get("postedAt")),
            status=clean_text(data.get("status")),
            firstSeenAt=clean_text(data.get("firstSeenAt")),
            lastSeenAt=clean_text(data.get("lastSeenAt")),
            removedAt=clean_text(data.get("removedAt")),
            lifecycleEvent=clean_text(data.get("lifecycleEvent")),
            lifecycleReason=clean_text(data.get("lifecycleReason")),
            dedupKey=clean_text(data.get("dedupKey")),
            qualityScore=int(data.get("qualityScore") or 0),
            focusScore=int(data.get("focusScore") or 0),
            sourceBundleCount=int(data.get("sourceBundleCount") or 0),
            sourceBundle=[
                dict(item) for item in data.get("sourceBundle") or [] if isinstance(item, Mapping)
            ],
            locations=[dict(item) for item in locations_raw if isinstance(item, Mapping)],
            locationSummary=clean_text(data.get("locationSummary")),
            adapter=clean_text(data.get("adapter")),
            studio=clean_text(data.get("studio")),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["lifecycleEvent"] = self.lifecycleEvent
        payload["lifecycleReason"] = self.lifecycleReason
        return payload
