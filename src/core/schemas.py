"""
Pydantic schemas for data contracts (CanonicalJob, SavedJob, Manifest).
Used at pipeline and bridge boundaries to validate shape; frontend contract remains camelCase.
See docs/DATA_CONTRACT.md for field definitions.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class CanonicalJobSchema(BaseModel):
    """Schema for a single job row (jobs-unified.json). All fields optional for lenient validation."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    id: Any | None = None
    title: str = Field(default="", description="Job title")
    company: str = Field(default="", description="Employer or studio name")
    city: str = Field(default="", description="City or empty if remote")
    country: str = Field(default="", description="Country name")
    workType: str = Field(default="", description="One of Remote, Hybrid, Onsite")
    contractType: str = Field(
        default="", description="One of Full-time, Internship, Temporary, Unknown"
    )
    jobLink: str = Field(default="", description="Canonical apply URL")
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
    sourceBundle: list[dict[str, Any]] = Field(default_factory=list)
    locations: list[dict[str, Any]] = Field(default_factory=list)
    locationSummary: str = ""
    adapter: str = ""
    studio: str = ""


class SavedJobSnapshotSchema(BaseModel):
    """Legacy subset of CanonicalJob for older SavedJob.snapshot payloads."""

    model_config = ConfigDict(extra="ignore")

    title: str = ""
    company: str = ""
    city: str = ""
    country: str = ""
    workType: str = ""


class SavedJobSchema(BaseModel):
    """Lenient schema for `/desktop-local-data/saved-jobs/save` input payloads."""

    model_config = ConfigDict(extra="ignore")

    jobKey: str = Field(default="", description="Primary key job_<hash>")
    snapshot: SavedJobSnapshotSchema | None = Field(
        default=None, description="Display subset: title, company, location, workType"
    )
    title: str = ""
    company: str = ""
    sector: str = ""
    companyType: str = ""
    city: str = ""
    country: str = ""
    workType: str = ""
    contractType: str = ""
    jobLink: str = ""
    profession: str = ""
    updatedAt: str = Field(default="", description="ISO 8601 last modified")
    savedAt: str = ""
    createdAt: str = Field(default="", description="ISO 8601 when bookmark/custom row was created")
    status: str = Field(default="", description="Legacy user stage mirror")
    applicationStatus: str = Field(default="", description="User stage e.g. bookmark, applied")
    pipelinePhase: str = Field(default="", description="Application pipeline phase")
    outcomeStatus: str = Field(default="", description="Application outcome status")
    phaseTimestamps: dict[str, str] = Field(default_factory=dict)
    outcomeTimestamps: dict[str, str] = Field(default_factory=dict)
    notes: str = ""
    isCustom: bool = False
    customSourceLabel: str = ""
    reminderAt: str = ""
    contactedAt: str = ""
    updatedBy: str = ""
    attachmentsCount: int = 0
    attachments: int = 0
    signature: str = ""
    keySalt: str = ""


class LocalSavedJobRowSchema(BaseModel):
    """Persisted local saved-job row stored under desktop local data."""

    model_config = ConfigDict(extra="ignore")

    profileId: str = ""
    jobKey: str = Field(default="", description="Primary key job_<hash>")
    title: str = ""
    company: str = ""
    sector: str = ""
    companyType: str = ""
    city: str = ""
    country: str = ""
    workType: str = ""
    contractType: str = ""
    jobLink: str = ""
    profession: str = ""
    isCustom: bool = False
    customSourceLabel: str = ""
    reminderAt: str = ""
    contactedAt: str = ""
    updatedBy: str = ""
    applicationStatus: str = ""
    pipelinePhase: str = ""
    outcomeStatus: str = ""
    phaseTimestamps: dict[str, str] = Field(default_factory=dict)
    outcomeTimestamps: dict[str, str] = Field(default_factory=dict)
    notes: str = ""
    attachmentsCount: int = 0
    savedAt: str = ""
    updatedAt: str = ""
    contentUpdatedAt: str = ""
    trackingUpdatedAt: str = ""
    notesUpdatedAt: str = ""
    lastActivityAt: str = ""


class LocalDataActivityRowSchema(BaseModel):
    """Persisted local activity row for a desktop profile."""

    model_config = ConfigDict(extra="ignore")

    id: str = ""
    profileId: str = ""
    type: str = ""
    jobKey: str = ""
    title: str = ""
    company: str = ""
    createdAt: str = ""
    details: dict[str, Any] = Field(default_factory=dict)


class LocalDataAttachmentRowSchema(BaseModel):
    """Persisted local attachment metadata row for a desktop profile."""

    model_config = ConfigDict(extra="ignore")

    id: str = ""
    profileId: str = ""
    jobKey: str = ""
    name: str = ""
    type: str = ""
    size: int = 0
    createdAt: str = ""
    path: str = ""


class LocalDataBackupAttachmentSchema(LocalDataAttachmentRowSchema):
    """Attachment row shape inside local-data backup payloads."""

    blobDataUrl: str = ""


class LocalDataBackupProfileSchema(BaseModel):
    """Profile metadata stored in backup export payloads."""

    model_config = ConfigDict(extra="ignore")

    id: str = ""
    name: str = ""
    email: str = ""


class LocalDataBackupCountsSchema(BaseModel):
    """Count summary for backup exports."""

    model_config = ConfigDict(extra="ignore")

    savedJobs: int = 0
    customJobs: int = 0
    historyEvents: int = 0
    attachments: int = 0
    sourcePolicyReviewPairs: int = 0
    sourcePolicyRecommendationPairs: int = 0


class LocalDataBackupSourcePolicySchema(BaseModel):
    """Source-policy artifacts stored in explicit desktop backup payloads."""

    model_config = ConfigDict(extra="ignore")

    reviewState: dict[str, Any] = Field(default_factory=dict)
    recommendations: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class LocalDataBackupPayloadSchema(BaseModel):
    """Backup export/import payload for desktop local data."""

    model_config = ConfigDict(extra="ignore")

    version: int = 3
    schemaVersion: int = 3
    exportedAt: str = ""
    includesFiles: bool = False
    counts: LocalDataBackupCountsSchema = Field(default_factory=LocalDataBackupCountsSchema)
    profile: LocalDataBackupProfileSchema = Field(default_factory=LocalDataBackupProfileSchema)
    savedJobs: list[LocalSavedJobRowSchema] = Field(default_factory=list)
    attachments: list[LocalDataBackupAttachmentSchema] = Field(default_factory=list)
    activityLog: list[LocalDataActivityRowSchema] = Field(default_factory=list)
    sourcePolicy: LocalDataBackupSourcePolicySchema = Field(
        default_factory=LocalDataBackupSourcePolicySchema
    )


class ManifestArtifactsSchema(BaseModel):
    """Artifacts section of LATEST_MANIFEST.json."""

    model_config = ConfigDict(extra="ignore")

    exe: str = ""
    ship: str = ""
    smoke_report: str = ""
    precommit_status: Literal["not_run", "passed", "failed"] = "not_run"
    py_tests_status: Literal["not_run", "passed", "failed"] = "not_run"
    node_tests_status: Literal["not_run", "passed", "failed"] = "not_run"
    precommit_ok: bool = False
    py_tests_ok: bool = False
    node_tests_ok: bool = False


class ManifestSchema(BaseModel):
    """Schema for _out/LATEST_MANIFEST.json (HUD)."""

    model_config = ConfigDict(extra="ignore")

    last_run_id: str = Field(default="", description="Run ID YYYYMMDD_HHMMSS")
    last_run_time: str = Field(default="", description="Human-readable timestamp")
    status: str = Field(default="", description="success or failure")
    summary: str = Field(default="", description="Human-readable status")
    src_hash: str = Field(default="", description="SHA256 of src/")
    artifacts_root: str = Field(default="", description="Path to run directory")
    artifacts: ManifestArtifactsSchema | None = Field(
        default=None, description="Paths and test flags"
    )
