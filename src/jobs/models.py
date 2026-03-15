"""Data-only models for the jobs pipeline package."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, TypedDict


RawJob = Dict[str, Any]
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
    headers: Dict[str, str] = field(default_factory=dict)
    user_agent: str = ""
    proxy_url: str = ""


@dataclass(frozen=True, slots=True)
class SourceDiagnostics:
    adapter: str
    studio: str
    details: List[Dict[str, Any]] = field(default_factory=list)
    partial_errors: List[str] = field(default_factory=list)
    low_confidence_dropped: int = 0

    def to_dict(self) -> Dict[str, Any]:
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
    jobs: List[RawJob] = field(default_factory=list)
    diagnostics: Optional[SourceDiagnostics] = None
    duration_ms: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


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
    dedupKey: str = ""
    qualityScore: int = 0
    focusScore: int = 0
    sourceBundleCount: int = 0
    sourceBundle: List[Dict[str, Any]] = field(default_factory=list)
    adapter: str = ""
    studio: str = ""

    @classmethod
    def from_mapping(cls, payload: RawJobLike) -> "CanonicalJob":
        data = dict(payload)
        return cls(
            id=data.get("id", ""),
            title=str(data.get("title") or ""),
            company=str(data.get("company") or ""),
            city=str(data.get("city") or ""),
            country=str(data.get("country") or ""),
            workType=str(data.get("workType") or ""),
            contractType=str(data.get("contractType") or ""),
            jobLink=str(data.get("jobLink") or ""),
            sector=str(data.get("sector") or ""),
            profession=str(data.get("profession") or ""),
            companyType=str(data.get("companyType") or ""),
            description=str(data.get("description") or ""),
            source=str(data.get("source") or ""),
            sourceJobId=str(data.get("sourceJobId") or ""),
            fetchedAt=str(data.get("fetchedAt") or ""),
            postedAt=str(data.get("postedAt") or ""),
            status=str(data.get("status") or ""),
            firstSeenAt=str(data.get("firstSeenAt") or ""),
            lastSeenAt=str(data.get("lastSeenAt") or ""),
            removedAt=str(data.get("removedAt") or ""),
            dedupKey=str(data.get("dedupKey") or ""),
            qualityScore=int(data.get("qualityScore") or 0),
            focusScore=int(data.get("focusScore") or 0),
            sourceBundleCount=int(data.get("sourceBundleCount") or 0),
            sourceBundle=[dict(item) for item in data.get("sourceBundle") or [] if isinstance(item, Mapping)],
            adapter=str(data.get("adapter") or ""),
            studio=str(data.get("studio") or ""),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def canonical_job_to_dict(job: CanonicalJob) -> Dict[str, Any]:
    return job.to_dict()


def canonical_jobs_to_dicts(rows: Sequence[CanonicalJob]) -> List[Dict[str, Any]]:
    return [row.to_dict() for row in rows]


def update_canonical_job(job: CanonicalJob, **changes: Any) -> CanonicalJob:
    payload = job.to_dict()
    payload.update(changes)
    return CanonicalJob.from_mapping(payload)


def ensure_mutable_mapping(payload: RawJobLike | MutableMapping[str, Any]) -> Dict[str, Any]:
    return dict(payload)
