"""Packaged automatic-reclaim startup regression.

This lane seeds an unarmed data root whose database is bloated by disposable
freelist pages, starts the packaged bridge with no Admin page and no manual
click, and waits for the startup trigger to record an armed rehearsal and then
start the real automatic reclaim.

AI boundary owns: packaged storage-reclaim startup lane and its fixture.
AI boundary implement in: this file for fixture and bridge/reclaim observation.
AI boundary search before contracts: packaged smoke orchestrator, reclaim trigger
and journal semantics.
AI boundary verify: `npm run test:frontend:packaged:admin-reclaim-startup` plus
focused packaged reclaim tests.
"""

from __future__ import annotations

import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any

from src.storage.baluffo_store import DEFAULT_DB_NAME, BaluffoStore
from src.storage.job_runtime import JobRuntimeStore
from src.storage.reclaim_policy import ReclaimContext, reclaim_verdict
from src.storage.runtime_retention import preview_runtime_retention

root: Any | None = None

BLOAT_FREELIST_TARGET_BYTES = 640 * 1024 * 1024
_BLOAT_ROW_BYTES = 1024 * 1024
_BLOAT_TABLE = "__baluffo_packaged_reclaim_bloat"
RECLAIM_STARTUP_SLUG = "packaged-admin-reclaim-startup-rehearsal"
# The bridge schedules its first measurement 60 s after serving. Leave room for
# the rehearsal copy, the arm decision, and the real start on a slow CI disk.
AUTO_RECLAIM_MIN_WAIT_S = 180.0
_RECLAIM_POLL_S = 1.0


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.rehearsal_reclaim.root is not configured")
    return root


def _fixture_row() -> dict[str, Any]:
    return {
        "id": 1,
        "title": "Packaged reclaim fixture",
        "company": "Baluffo QA",
        "city": "Amsterdam",
        "country": "Netherlands",
        "jobLink": "https://example.test/packaged-reclaim-fixture",
        "source": "Rehearsal",
        "sourceJobId": "packaged-reclaim-fixture-1",
        "status": "active",
        "firstSeenAt": "2026-11-17T09:00:00Z",
        "lastSeenAt": "2026-11-17T09:00:00Z",
        "sourceBundle": [],
    }


def seed_unarmed_bloated_data_root(
    data_dir: Path | str,
    *,
    freelist_target_bytes: int = BLOAT_FREELIST_TARGET_BYTES,
) -> dict[str, Any]:
    """Seed a real store with disposable free pages and no reclaim journal.

    A dropped table leaves the pages in SQLite's freelist, which is exactly the
    evidence the reclaim verdict counts. The database also has one published
    feed generation, so the verdict has the current generation it requires.
    Maintenance is deferred while seeding: a normal post-serve VACUUM would
    remove the fixture's freelist before the startup trigger could see it.
    """
    deps = _root()
    resolved = Path(data_dir).expanduser().resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    journal_path = resolved / f"{DEFAULT_DB_NAME}.reclaim.json"
    journal_path.unlink(missing_ok=True)

    deps.LocalDataStore(deps.LocalDataPaths.from_data_dir(resolved)).sign_in(
        "Packaged Admin Reclaim Rehearsal"
    )
    target = max(1, int(freelist_target_bytes))
    rows = max(1, target // _BLOAT_ROW_BYTES)
    with BaluffoStore(resolved, defer_maintenance=True) as store:
        JobRuntimeStore(store, now_iso=deps.utc_now_iso).replace_feed(
            run_id="packaged_admin_reclaim_fixture",
            rows=[_fixture_row()],
        )
        store.write(lambda conn: conn.execute(f"CREATE TABLE {_BLOAT_TABLE} (payload BLOB)"))
        store.write(
            lambda conn: conn.execute(
                f"WITH RECURSIVE seq(n) AS ("
                f"SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < ?"
                f") INSERT INTO {_BLOAT_TABLE} SELECT zeroblob(?) FROM seq",
                (rows, _BLOAT_ROW_BYTES),
            )
        )
        store.write(lambda conn: conn.execute(f"DROP TABLE {_BLOAT_TABLE}"))
        sizes = store.file_sizes()
        preview = preview_runtime_retention(store)
        free_bytes = shutil.disk_usage(resolved).free
        verdict = reclaim_verdict(
            preview,
            sizes,
            context=ReclaimContext(
                free_space_bytes=int(free_bytes),
                auto_enabled=False,
            ),
        )

    if journal_path.exists():
        raise RuntimeError("the reclaim fixture seeded a reclaim journal")
    if int(sizes.get("freelistBytes") or 0) < target // 2:
        raise RuntimeError(
            "the reclaim fixture did not leave the expected freelist "
            f"({sizes.get('freelistBytes')} of {target} bytes)"
        )
    return {
        "dataDir": str(resolved),
        "journalPath": str(journal_path),
        "databaseBytes": int(sizes.get("databaseBytes") or 0),
        "freelistBytes": int(sizes.get("freelistBytes") or 0),
        "walBytes": int(sizes.get("walBytes") or 0),
        "verdict": verdict,
        "journalPresent": False,
    }


def _auto_reclaim_state(payload: dict[str, Any]) -> dict[str, Any]:
    """Read the arm and real-run facts the packaged bridge reports."""
    reclaim: dict[str, Any] = {}
    candidate = payload.get("storageReclaim")
    if isinstance(candidate, dict):
        reclaim = candidate
    journal: dict[str, Any] = {}
    candidate = reclaim.get("journal")
    if isinstance(candidate, dict):
        journal = candidate
    arm: dict[str, Any] = {}
    candidate = journal.get("autoArm")
    if isinstance(candidate, dict):
        arm = candidate
    arm_reason = str(arm.get("reason") or "")
    armed = bool(arm.get("armed"))
    mode = str(journal.get("mode") or "")
    status = str(journal.get("status") or "")
    auto = bool(journal.get("auto"))
    rehearsal_complete = bool(armed and arm_reason == "rehearsal-adequate")
    reclaim_started = bool(
        rehearsal_complete
        and auto
        and mode == "reclaim"
        and status in {"running", "cancelling", "complete"}
    )
    terminal_failure = bool(
        auto
        and mode == "reclaim"
        and status in {"failed", "cancelled", "superseded", "interrupted"}
    )
    return {
        "armed": armed,
        "armReason": arm_reason,
        "rehearsalComplete": rehearsal_complete,
        "mode": mode,
        "status": status,
        "auto": auto,
        "runId": str(journal.get("runId") or ""),
        "autoReclaimStarted": reclaim_started,
        "terminalFailure": terminal_failure,
        "journalError": str(journal.get("operatorError") or journal.get("error") or ""),
    }


def _wait_for_bridge(
    process: Any,
    *,
    bridge_base_url: str,
    timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    last_error = ""
    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(
                f"packaged bridge exited before the reclaim lane started (exit {exit_code})."
            )
        try:
            health: dict[str, Any] = deps.fetch_json(
                f"{bridge_base_url}/ops/health?view=ready", timeout_s=2.5
            )
            if str(health.get("service") or "") == "baluffo-bridge" and bool(
                health.get("desktopMode")
            ):
                return health
        except (OSError, TimeoutError, ValueError) as exc:
            last_error = str(exc)
        time.sleep(0.35)
    raise TimeoutError(
        f"packaged bridge did not become ready within {float(timeout_s):.1f}s."
        + (f" Last error: {last_error}" if last_error else "")
    )


def _wait_for_auto_reclaim(
    process: Any,
    *,
    bridge_base_url: str,
    timeout_s: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    deps = _root()
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    last_state: dict[str, Any] = {}
    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(
                f"packaged bridge exited while waiting for automatic reclaim (exit {exit_code})."
            )
        try:
            payload = deps.fetch_json(f"{bridge_base_url}/ops/storage-reclaim", timeout_s=5.0)
            state = _auto_reclaim_state(payload)
            last_state = state
            if state["terminalFailure"]:
                raise RuntimeError(
                    "the automatic reclaim failed after arming: "
                    f"{state['status']} {state['journalError']}".strip()
                )
            if state["autoReclaimStarted"]:
                return state, payload
        except (OSError, TimeoutError, ValueError) as exc:
            last_state = {**last_state, "lastReadError": str(exc)}
        time.sleep(_RECLAIM_POLL_S)
    raise TimeoutError(
        "automatic storage reclaim did not start without a manual trigger; "
        f"last state: {last_state or 'no reclaim payload'}"
    )


def run_packaged_admin_reclaim_startup_rehearsal(
    *,
    exe_path: Path,
    artifacts_dir: Path,
    runtime_timeout_s: float,
) -> dict[str, Any]:
    deps = _root()
    started = time.perf_counter()
    data_dir = artifacts_dir / "runtime-data"
    stdout_path = artifacts_dir / "admin-reclaim-startup.stdout.log"
    stderr_path = artifacts_dir / "admin-reclaim-startup.stderr.log"
    runtime_env = deps.os.environ.copy()
    runtime_env.update(
        deps.packaged_runtime_env_overrides(
            artifacts_dir=artifacts_dir,
            session_scope="admin-reclaim-startup",
        )
    )
    runtime_env["BALUFFO_DESKTOP_NO_BROWSER"] = "1"
    runtime_env["BALUFFO_SYNC_DISABLE"] = "1"
    runtime_env.pop("BALUFFO_RECLAIM_AUTO_ENABLED", None)
    deps.clear_packaged_desktop_session_state(runtime_env)
    process = None
    stdout_handle = None
    stderr_handle = None
    bridge_port = 0
    details: dict[str, Any] = {
        "runtimeStdout": str(stdout_path),
        "runtimeStderr": str(stderr_path),
        "manualRehearseClicked": False,
    }
    try:
        seed = seed_unarmed_bloated_data_root(data_dir)
        details["seed"] = seed
        if str((seed.get("verdict") or {}).get("verdict") or "") != "bloat":
            raise RuntimeError(
                f"the reclaim fixture did not produce a bloat verdict: {seed.get('verdict')}"
            )
        bridge_port = deps.choose_free_port()
        owner_token = deps.generate_packaged_smoke_run_token()
        session_id = deps.generate_packaged_smoke_run_token()
        process, stdout_handle, stderr_handle = deps.launch_packaged_desktop_child(
            exe_path,
            mode="bridge",
            port=bridge_port,
            data_dir=data_dir,
            owner_token=owner_token,
            desktop_session_id=session_id,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            env=runtime_env,
        )
        bridge_base_url = f"http://127.0.0.1:{bridge_port}"
        health = _wait_for_bridge(
            process,
            bridge_base_url=bridge_base_url,
            timeout_s=float(runtime_timeout_s),
        )
        details["health"] = {
            "service": str(health.get("service") or ""),
            "desktopMode": bool(health.get("desktopMode")),
        }
        state, payload = _wait_for_auto_reclaim(
            process,
            bridge_base_url=bridge_base_url,
            timeout_s=max(AUTO_RECLAIM_MIN_WAIT_S, float(runtime_timeout_s)),
        )
        reclaim_payload = payload.get("storageReclaim")
        reclaim_payload = reclaim_payload if isinstance(reclaim_payload, dict) else {}
        journal = reclaim_payload.get("journal")
        journal = journal if isinstance(journal, dict) else {}
        details.update(
            {
                "bridgePort": bridge_port,
                "autoArm": journal.get("autoArm")
                if isinstance(journal.get("autoArm"), dict)
                else {},
                "reclaim": {
                    "runId": state["runId"],
                    "mode": state["mode"],
                    "status": state["status"],
                    "auto": state["auto"],
                },
                "autoReclaimStarted": True,
            }
        )
        return {
            "name": "Packaged admin automatic reclaim startup rehearsal",
            "slug": RECLAIM_STARTUP_SLUG,
            "status": "passed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": "",
            "details": details,
        }
    except (OSError, RuntimeError, TimeoutError, ValueError, sqlite3.Error) as exc:
        return {
            "name": "Packaged admin automatic reclaim startup rehearsal",
            "slug": RECLAIM_STARTUP_SLUG,
            "status": "failed",
            "durationMs": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
            "details": details,
        }
    finally:
        deps.terminate_process_tree(process)
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
        deps.cleanup_orphaned_desktop_ports_nt(bridge_port)
        deps.clear_packaged_desktop_session_state(runtime_env)
