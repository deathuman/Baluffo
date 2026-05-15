from __future__ import annotations

import copy
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest import mock

from src import source_registry as source_registry_module
from src.source_discovery import config as discovery_config_module
from src.source_discovery import orchestrator as discovery_orchestrator_module

_UNSET = object()


@dataclass(frozen=True)
class DiscoveryRuntimePaths:
    active_path: Path
    pending_path: Path
    rejected_path: Path
    discovery_candidates_path: Path
    approval_state_path: Path
    discovery_report_path: Path
    url_patch_manifest_path: Path
    m5_strategic_backlog_path: Path


@contextmanager
def override_discovery_config(
    *,
    studio_seeds: Any = _UNSET,
    static_candidates: Any = _UNSET,
    discovery_config_path: Any = _UNSET,
    extra_overrides: dict[str, Any] | None = None,
):
    with ExitStack() as stack:
        if studio_seeds is not _UNSET:
            stack.enter_context(
                mock.patch.object(
                    discovery_config_module,
                    "STUDIO_SEEDS",
                    list(studio_seeds),
                )
            )
        if static_candidates is not _UNSET:
            stack.enter_context(
                mock.patch.object(
                    discovery_config_module,
                    "STATIC_DISCOVERY_CANDIDATES",
                    list(static_candidates),
                )
            )
        if discovery_config_path is not _UNSET:
            stack.enter_context(
                mock.patch.object(
                    discovery_config_module,
                    "DISCOVERY_CONFIG_PATH",
                    Path(discovery_config_path),
                )
            )
        for name, value in (extra_overrides or {}).items():
            stack.enter_context(mock.patch.object(discovery_config_module, name, value))
        yield


@contextmanager
def override_discovery_runtime(
    root: Path,
    *,
    studio_seeds: Any = _UNSET,
    static_candidates: Any = _UNSET,
    discovery_config_path: Any = _UNSET,
    include_m5_backlog: bool = False,
    extra_config_overrides: dict[str, Any] | None = None,
):
    paths = DiscoveryRuntimePaths(
        active_path=root / "active.json",
        pending_path=root / "pending.json",
        rejected_path=root / "rejected.json",
        discovery_candidates_path=root / "candidates.json",
        approval_state_path=root / "source-approval-state.json",
        discovery_report_path=root / "report.json",
        url_patch_manifest_path=root / "url-patch-manifest.json",
        m5_strategic_backlog_path=root / "m5-strategic-backlog.json",
    )
    isolated_default_config = copy.deepcopy(discovery_config_module.DEFAULT_DISCOVERY_CONFIG)
    for section_name, artifact_name in (
        ("sheetDirectory", "sheet-directory-discovery-audit.json"),
        ("webSearch", "web-search-discovery-audit.json"),
    ):
        section = isolated_default_config.get(section_name)
        if isinstance(section, dict):
            section["activeAuditPath"] = str(root / artifact_name)
    with ExitStack() as stack:
        stack.enter_context(
            mock.patch.dict(
                discovery_config_module.DEFAULT_DISCOVERY_CONFIG,
                isolated_default_config,
                clear=True,
            )
        )
        stack.enter_context(
            mock.patch.object(source_registry_module, "ACTIVE_PATH", paths.active_path)
        )
        stack.enter_context(
            mock.patch.object(source_registry_module, "PENDING_PATH", paths.pending_path)
        )
        stack.enter_context(
            mock.patch.object(source_registry_module, "REJECTED_PATH", paths.rejected_path)
        )
        stack.enter_context(
            mock.patch.object(
                source_registry_module,
                "DISCOVERY_CANDIDATES_PATH",
                paths.discovery_candidates_path,
            )
        )
        stack.enter_context(
            mock.patch.object(
                source_registry_module,
                "APPROVAL_STATE_PATH",
                paths.approval_state_path,
            )
        )
        stack.enter_context(
            mock.patch.object(
                discovery_orchestrator_module,
                "DEFAULT_APPROVAL_STATE_PATH",
                paths.approval_state_path,
            )
        )
        stack.enter_context(
            mock.patch.object(
                source_registry_module,
                "DISCOVERY_REPORT_PATH",
                paths.discovery_report_path,
            )
        )
        stack.enter_context(
            mock.patch.object(
                source_registry_module,
                "URL_PATCH_MANIFEST_PATH",
                paths.url_patch_manifest_path,
            )
        )
        if include_m5_backlog:
            stack.enter_context(
                mock.patch.object(
                    source_registry_module,
                    "M5_STRATEGIC_BACKLOG_PATH",
                    paths.m5_strategic_backlog_path,
                )
            )
        stack.enter_context(
            override_discovery_config(
                studio_seeds=studio_seeds,
                static_candidates=static_candidates,
                discovery_config_path=discovery_config_path,
                extra_overrides=extra_config_overrides,
            )
        )
        yield paths


__all__ = [
    "DiscoveryRuntimePaths",
    "override_discovery_config",
    "override_discovery_runtime",
]
