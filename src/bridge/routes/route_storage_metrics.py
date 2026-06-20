"""Storage metric helpers shared by GET route modules.

AI boundary owns: storage read metric recording for route modules.
AI boundary implement in: storage metrics and route leaves that own the read operation.
AI boundary search before contracts: route callers, storage metrics tests, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused route helper tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from src.storage_metrics import duration_ms, record_storage_read


class _StorageMetricsApi(Protocol):
    JOBS_FETCH_REPORT_PATH: Path
    runtime_config: Any


def storage_metrics_data_dir(api: _StorageMetricsApi) -> Path:
    data_dir = getattr(api.runtime_config, "data_dir", None)
    if data_dir:
        return Path(data_dir).expanduser().resolve()
    return Path(api.JOBS_FETCH_REPORT_PATH).expanduser().resolve().parent


def record_storage_read_metric(
    api: _StorageMetricsApi,
    *,
    surface: str,
    artifact: str,
    storage_kind: str,
    started_at: float,
    row_count: int = 0,
    bytes_read: int = 0,
    failed: bool = False,
) -> None:
    record_storage_read(
        surface=surface,
        artifact=artifact,
        storage_kind=storage_kind,
        duration_ms=duration_ms(started_at),
        bytes_read=max(0, int(bytes_read or 0)),
        row_count=max(0, int(row_count or 0)),
        failed=failed,
        data_dir=storage_metrics_data_dir(api),
    )
