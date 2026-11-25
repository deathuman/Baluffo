"""A discovery run with auto-approval off still stages pending through the authority."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest import mock

from src.bridge.storage_health import close_storage_stores, get_storage_store
from src.source_registry_identity import source_identity
from src.storage.baluffo_store import BaluffoStore
from src.storage.source_registry_runtime import SourceRegistryRuntimeStore

from ._helpers import (
    discovery_config_without_generator_stages,
    discovery_orchestrator,
    override_discovery_runtime,
    sd,
    workspace_tmpdir,
)

_ACTIVE_ROW = {
    "id": "seeded-active",
    "adapter": "static",
    "name": "Seeded Active",
    "careersUrl": "https://seeded.example/careers",
    "jobsFound": 2,
}
_STATIC_CANDIDATE = {
    "name": "Authority Static",
    "studio": "Authority Static",
    "adapter": "static",
    "listing_url": "https://authority.example/jobs",
    "pages": ["https://authority.example/jobs"],
    "evidenceScore": 52,
    "evidenceTypes": ["seed_curated"],
}


def _authority_state(root: Path) -> dict[str, list[dict[str, Any]]]:
    with BaluffoStore(root, defer_maintenance=True) as store:
        return SourceRegistryRuntimeStore(store).current_state()


def test_run_discovery_with_auto_approval_off_stages_pending_through_sqlite() -> None:
    try:
        with workspace_tmpdir("source-discovery") as root:
            with override_discovery_runtime(
                root,
                studio_seeds=[],
                static_candidates=[_STATIC_CANDIDATE],
            ):
                SourceRegistryRuntimeStore(get_storage_store(root)).replace_state(
                    state={"active": [dict(_ACTIVE_ROW)], "pending": [], "rejected": []},
                    tombstones={},
                    reason="test_seed",
                )

                def fake_fetch(url: str, _timeout: int) -> str:
                    if url == "https://authority.example/jobs":
                        return '<a href="https://authority.example/jobs/role-1">Role</a>'
                    raise RuntimeError(f"unexpected URL: {url}")

                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config=discovery_config_without_generator_stages(
                        autoApproveHealthyPendingOnComplete=False
                    ),
                    fetcher=fake_fetch,
                )

                assert int(report["summary"]["queuedCandidateCount"]) == 1
                auto_approval = report["runtime"]["autoApproval"]
                assert auto_approval["enabled"] is False
                assert auto_approval["authorityMode"] == "sqlite"
                assert int(auto_approval["approvedCount"]) == 0

                current = _authority_state(root)
                # The seeded active row survives the publish and the candidate is
                # staged as pending rather than promoted.
                assert [source_identity(row) for row in current["active"]] == ["seeded-active"]
                assert [str(row.get("name")) for row in current["pending"]] == ["Authority Static"]
    finally:
        close_storage_stores()


def test_failed_authority_publish_carries_pending_rows_for_bridge_restage() -> None:
    try:
        with workspace_tmpdir("source-discovery") as root:
            with override_discovery_runtime(
                root,
                studio_seeds=[],
                static_candidates=[_STATIC_CANDIDATE],
            ) as paths:
                SourceRegistryRuntimeStore(get_storage_store(root)).replace_state(
                    state={"active": [dict(_ACTIVE_ROW)], "pending": [], "rejected": []},
                    tombstones={},
                    reason="test_seed",
                )

                def fake_fetch(url: str, _timeout: int) -> str:
                    if url == "https://authority.example/jobs":
                        return '<a href="https://authority.example/jobs/role-1">Role</a>'
                    raise RuntimeError(f"unexpected URL: {url}")

                def fail_after_staging(**_kwargs: Any) -> None:
                    staged = json.loads(paths.discovery_report_path.read_text(encoding="utf-8"))[
                        "runtime"
                    ]["registryFinalization"]
                    assert staged["status"] == "running"
                    assert [row["name"] for row in staged["pendingRows"]] == ["Authority Static"]
                    raise ValueError("authority unavailable")

                with mock.patch.object(
                    discovery_orchestrator,
                    "apply_discovery_approval_to_authority",
                    side_effect=fail_after_staging,
                ):
                    report = sd.run_discovery(
                        timeout_s=5,
                        top_n=0,
                        mode="dynamic",
                        include_web_search=False,
                        discovery_config=discovery_config_without_generator_stages(
                            autoApproveHealthyPendingOnComplete=False
                        ),
                        fetcher=fake_fetch,
                    )

                finalization = report["runtime"]["registryFinalization"]
                assert finalization["status"] == "failed"
                assert finalization["error"] == "authority unavailable"
                assert [row["name"] for row in finalization["pendingRows"]] == ["Authority Static"]
                # The failed publish leaves the authority untouched; the bridge
                # retries from the rows carried in the report.
                current = _authority_state(root)
                assert [row["name"] for row in current["pending"]] == []
    finally:
        close_storage_stores()


def test_normal_publish_reports_the_pending_merge_audit() -> None:
    """The terminal report carries the merge audit on the success path too."""
    try:
        with workspace_tmpdir("source-discovery") as root:
            with override_discovery_runtime(
                root,
                studio_seeds=[],
                static_candidates=[_STATIC_CANDIDATE],
            ):

                def fake_fetch(url: str, _timeout: int) -> str:
                    if url == "https://authority.example/jobs":
                        return '<a href="https://authority.example/jobs/role-1">Role</a>'
                    raise RuntimeError(f"unexpected URL: {url}")

                def fake_apply(**kwargs: Any) -> tuple[dict[str, list[dict[str, Any]]], int, str]:
                    kwargs["merge_audit"]["rowsDroppedPlaced"] = 1
                    return {"active": [], "pending": [], "rejected": []}, 0, "sqlite"

                with mock.patch.object(
                    discovery_orchestrator,
                    "apply_discovery_approval_to_authority",
                    side_effect=fake_apply,
                ):
                    report = sd.run_discovery(
                        timeout_s=5,
                        top_n=0,
                        mode="dynamic",
                        include_web_search=False,
                        discovery_config=discovery_config_without_generator_stages(
                            autoApproveHealthyPendingOnComplete=False
                        ),
                        fetcher=fake_fetch,
                    )

                finalization = report["runtime"]["registryFinalization"]
                assert finalization["status"] == "completed"
                assert finalization["pendingMergeAudit"] == {"rowsDroppedPlaced": 1}
                assert "pendingRows" not in finalization
    finally:
        close_storage_stores()
