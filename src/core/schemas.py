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
    contractType: str = Field(default="", description="One of Full-time, Internship, Temporary, Unknown")
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
    dedupKey: str = ""
    qualityScore: int = 0
    focusScore: int = 0
    sourceBundleCount: int = 0
    sourceBundle: list[dict[str, Any]] = Field(default_factory=list)
    adapter: str = ""
    studio: str = ""


class SavedJobSnapshotSchema(BaseModel):
    """Subset of CanonicalJob for SavedJob.snapshot (display data)."""

    model_config = ConfigDict(extra="ignore")

    title: str = ""
    company: str = ""
    city: str = ""
    country: str = ""
    workType: str = ""


class SavedJobSchema(BaseModel):
    """Schema for a saved job (user bookmark or custom row)."""

    model_config = ConfigDict(extra="ignore")

    jobKey: str = Field(default="", description="Primary key job_<hash>")
    snapshot: SavedJobSnapshotSchema | None = Field(default=None, description="Display subset: title, company, location, workType")
    createdAt: str = Field(default="", description="ISO 8601 when bookmark/custom row was created")
    updatedAt: str = Field(default="", description="ISO 8601 last modified")
    status: str = Field(default="", description="User stage e.g. saved, applied, interviewing_1")
    notes: str = ""
    isCustom: bool = False
    customSourceLabel: str = ""
    reminderAt: str = ""
    attachments: int = 0
    signature: str = ""


class ManifestArtifactsSchema(BaseModel):
    """Artifacts section of LATEST_MANIFEST.json."""

    model_config = ConfigDict(extra="ignore")

    exe: str = ""
    ship: str = ""
    smoke_report: str = ""
    py_tests_status: Literal["not_run", "passed", "failed"] = "not_run"
    node_tests_status: Literal["not_run", "passed", "failed"] = "not_run"
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
    artifacts: ManifestArtifactsSchema | None = Field(default=None, description="Paths and test flags")
