"""Data-only models for the jobs pipeline package.

AI boundary owns: jobs pipeline dataclasses, typed dicts, and CanonicalJob wire-model fields.
AI boundary implement in: this file for data models only; normalization and validation belong in contracts/canonicalization leaves.
AI boundary search before contracts: DATA_CONTRACT.md, core schemas/contracts, jobs contracts, and frontend job consumers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused jobs contract tests.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from typing import Any, TypedDict

from src.jobs.text_utils import clean_text

ROW_PAYLOAD_ENV = "BALUFFO_ROW_PAYLOAD"
# One measurement switch, resolved once at import: ``legacy`` restores the recursive
# ``dataclasses.asdict`` build that ``CanonicalJob.to_dict`` replaced, so the corpus A/B can
# price the change as a paired study on one tree, the way pass 1 priced normalisation. Unset
# -- production and every other caller -- takes the cheap build.
ROW_PAYLOAD_LEGACY = os.environ.get(ROW_PAYLOAD_ENV, "").strip().lower() == "legacy"

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
    # Optional sink: a fetch that fills it reports the response's status and validators.
    # The object is shared by reference, so a caller reads it after the call returns.
    capture_response_headers: dict[str, str] | None = None


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
    availabilityId: str = ""
    availabilityStatus: str = ""
    availabilityCheckedAt: str = ""
    availabilityVerifiedAt: str = ""
    availabilityUnavailableAt: str = ""
    availabilityEvidence: dict[str, Any] = field(default_factory=dict)
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
            availabilityId=clean_text(data.get("availabilityId")),
            availabilityStatus=clean_text(data.get("availabilityStatus")),
            availabilityCheckedAt=clean_text(data.get("availabilityCheckedAt")),
            availabilityVerifiedAt=clean_text(data.get("availabilityVerifiedAt")),
            availabilityUnavailableAt=clean_text(data.get("availabilityUnavailableAt")),
            availabilityEvidence=(
                dict(data.get("availabilityEvidence") or {})
                if isinstance(data.get("availabilityEvidence"), Mapping)
                else {}
            ),
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
        """This row's field->value payload, independent of every other payload.

        ``asdict`` re-derives the field list, filters it, and recurses a deep copy over all
        40 fields on every call, which is per-call machinery waste on a path a recorded run
        hits ~2 million times for 30,053 rows. The field names cannot change, so this build
        reads the same fields and copies the same containers as ``asdict`` did: equal in
        value, and the same contract of one fresh top-level dict and fresh nested containers
        per caller (pinned by ``tests/test_jobs_row_payload.py``).
        """
        if ROW_PAYLOAD_LEGACY:
            return asdict(self)
        payload: dict[str, Any] = {name: getattr(self, name) for name in CANONICAL_JOB_FIELDS}
        for name in CANONICAL_JOB_CONTAINER_FIELDS:
            payload[name] = deepcopy(payload[name])
        return payload


# Read once, from the class, so a field added above cannot drift out of the payload.
CANONICAL_JOB_FIELDS: tuple[str, ...] = tuple(CanonicalJob.__dataclass_fields__)
# The fields ``asdict`` would deep-copy for us: the only mutable values in the model. Every
# other field is a scalar that is shared by reference either way.
CANONICAL_JOB_CONTAINER_FIELDS: tuple[str, ...] = (
    "availabilityEvidence",
    "sourceBundle",
    "locations",
)
