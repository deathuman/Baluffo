"""Registry conflict adjudication task helpers — thin coordinator.

AI boundary owns: background adjudication launch, progress, and conflict decision plumbing.
AI boundary implement in: this coordinator (start + job thread state); row/url helpers in the core leaf,
probe/parse in the probe leaf, decision plumbing in the decide leaf, progress plumbing in the progress leaf,
and the run orchestrator in the run leaf.
AI boundary search before contracts: registry conflict routes, post admin routes, and adjudication tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry adjudication tests.
"""

from __future__ import annotations

import threading
import uuid
from typing import Any

from src.bridge.registry_conflict_adjudication_core import (
    ADJUDICATION_REASON,
    _artifact_path,
    _as_dict,
    _now_iso,
    load_registry_conflict_adjudication,
)
from src.bridge.registry_conflict_adjudication_progress import (
    _failed_adjudication_payload,
    _running_adjudication_payload,
)
from src.bridge.registry_conflict_adjudication_run import (
    overlay_adjudication,
    run_registry_conflict_adjudication,
)

__all__ = [
    "ADJUDICATION_REASON",
    "load_registry_conflict_adjudication",
    "overlay_adjudication",
    "run_registry_conflict_adjudication",
    "start_registry_conflict_adjudication",
]

_ADJUDICATION_JOB_LOCK = threading.Lock()

_ADJUDICATION_JOB_THREAD: threading.Thread | None = None


def start_registry_conflict_adjudication(
    api: Any, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    global _ADJUDICATION_JOB_THREAD

    data = _as_dict(payload)
    with _ADJUDICATION_JOB_LOCK:
        if _ADJUDICATION_JOB_THREAD is not None and _ADJUDICATION_JOB_THREAD.is_alive():
            current = load_registry_conflict_adjudication(api)
            if current.get("status") == "running":
                return {**current, "started": False, "alreadyRunning": True}
            return {
                "ok": True,
                "status": "running",
                "started": False,
                "alreadyRunning": True,
            }

        run_id = f"conflict_check_{uuid.uuid4().hex[:10]}"
        started_at = _now_iso()
        running = _running_adjudication_payload(data, run_id=run_id, started_at=started_at)
        api.save_json_atomic(_artifact_path(api), running)

        def _worker() -> None:
            try:
                run_registry_conflict_adjudication(
                    api,
                    {
                        **data,
                        "runId": run_id,
                        "startedAt": started_at,
                    },
                )
            except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                current = load_registry_conflict_adjudication(api)
                api.save_json_atomic(
                    _artifact_path(api),
                    _failed_adjudication_payload(
                        data,
                        run_id=run_id,
                        started_at=started_at,
                        error=str(exc),
                        current=current,
                    ),
                )

        _ADJUDICATION_JOB_THREAD = threading.Thread(
            target=_worker,
            name=f"registry-conflict-adjudication-{run_id}",
            daemon=True,
        )
        _ADJUDICATION_JOB_THREAD.start()
        return {**running, "started": True, "alreadyRunning": False}
