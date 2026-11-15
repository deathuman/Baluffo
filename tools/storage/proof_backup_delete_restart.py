"""Prove the retained-backup delete restart settlement against a real bridge.

What it does, end to end, in one process:

1. seeds a throwaway data dir with a published feed and a larger current
   generation, using the production store writers (`JobRuntimeStore.replace_feed`);
2. times the exact function the delete worker runs before unlinking
   (`validate_current_feed_database`), so the kill window below is derived from
   this machine rather than hard-coded;
3. starts `src/admin_bridge.py` on that dir, runs a current-feed rebuild, and
   waits for the retained `<db>.pre-rebuild` copy to exist;
4. starts `POST /ops/storage-rebuild/backup-delete`, confirms the journal says
   `running`, and SIGKILLs the bridge inside that measured validation window;
5. restarts the bridge on the same dir and reads `GET /ops/storage-rebuild` once.

Pass condition: the first status read after the restart reports
`backupDeleteStatus: "failed"` with the restart error, the delete keeps its run
id, `backupDeletedAt` stays empty, the retained copy is still on disk at its
unchanged size, rollback is still available, and the live database still
validates. Any other outcome exits non-zero with the failing check named.

Everything happens under a fresh throwaway directory (default
`_out/proof-backup-delete-restart/<token>`); it refuses to run against a
non-empty data dir, so it cannot be pointed at real state by accident. No
network access, no repo `data/` writes, no dependencies beyond the stdlib.

Usage:
    python tools/storage/proof_backup_delete_restart.py
    python tools/storage/proof_backup_delete_restart.py --rows 8000 --keep
    python tools/storage/proof_backup_delete_restart.py --json _out/restart-proof.json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:  # runnable as `python tools/storage/<this>.py`
    sys.path.insert(0, str(ROOT))
DEFAULT_OUTPUT_ROOT = ROOT / "_out" / "proof-backup-delete-restart"
HEALTH_PATH = "/ops/health"
REBUILD_PATH = "/ops/storage-rebuild"
DELETE_PATH = "/ops/storage-rebuild/backup-delete"
# The shipped settlement message for a delete with no live worker owner.
RESTART_ERROR_FRAGMENT = "restarted before the backup delete finished"
BRIDGE_START_TIMEOUT_S = 240.0
REBUILD_TIMEOUT_S = 900.0
# The kill has to land inside validation, so this is a fraction of the measured
# pass, clamped to a window that is long enough to schedule and short enough to
# stay well before an unlink on a slow machine.
KILL_FRACTION = 0.4
KILL_MIN_S = 0.35
KILL_MAX_S = 15.0


class HarnessError(RuntimeError):
    """A step could not complete; the tool stops and reports it."""


def _row(index: int) -> dict[str, object]:
    return {
        "id": index,
        "title": f"Job {index}",
        "company": "Studio",
        "city": "Amsterdam",
        "country": "Netherlands",
        "workType": "Hybrid",
        "contractType": "Full-time",
        "jobLink": f"https://example.test/jobs/{index}",
        "source": "Example",
        "sourceJobId": f"job-{index}",
        "status": "active",
        "firstSeenAt": "2026-05-12T10:00:00Z",
        "lastSeenAt": "2026-05-12T10:00:00Z",
        "sourceBundle": [
            {"sourceName": "Example", "sourceId": "example", "sourceJobId": f"job-{index}"}
        ],
    }


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _request(
    port: int, path: str, body: dict[str, Any] | None = None, timeout_s: float = 240.0
) -> tuple[int, dict[str, Any]]:
    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if data is not None else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            payload = json.loads(response.read() or b"{}")
            return int(response.status), payload if isinstance(payload, dict) else {}
    except urllib.error.HTTPError as exc:
        payload = json.loads(exc.read() or b"{}")
        return int(exc.code), payload if isinstance(payload, dict) else {}


def _storage(payload: dict[str, Any]) -> dict[str, Any]:
    rebuild = payload.get("storageRebuild")
    return rebuild if isinstance(rebuild, dict) else {}


def _delete_fields(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "status",
        "backupDeleteStatus",
        "backupDeleteRunId",
        "backupDeleteStartedAt",
        "backupDeleteFinishedAt",
        "backupDeleteError",
        "backupDeletedAt",
        "backupDeleteFreedBytes",
        "backupExists",
        "backupBytes",
        "canRollback",
    )
    rebuild = _storage(payload)
    return {key: rebuild.get(key) for key in keys}


def _seed(data_dir: Path, rows: int) -> tuple[Path, float]:
    """Write a published feed, then time the delete worker's validation pass."""
    from src.bridge.storage_health import close_storage_stores, get_storage_store
    from src.storage import JobRuntimeStore
    from src.storage.current_feed_rebuild import validate_current_feed_database

    store = get_storage_store(data_dir)
    try:
        db_path = Path(store.db_path)
        runtime = JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00Z")
        runtime.replace_feed(run_id="run_old", rows=[_row(0)])
        runtime.replace_feed(run_id="run_new", rows=[_row(index) for index in range(1, rows + 1)])
    finally:
        close_storage_stores()

    started = time.monotonic()
    validate_current_feed_database(db_path)
    return db_path, (time.monotonic() - started)


def _start_bridge(data_dir: Path, port: int, log_path: Path) -> subprocess.Popen[bytes]:
    log = log_path.open("wb")
    process = subprocess.Popen(
        [
            sys.executable,
            "src/admin_bridge.py",
            "--data-dir",
            str(data_dir),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--quiet-requests",
        ],
        cwd=str(ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    process.log_file = log  # type: ignore[attr-defined]
    return process


def _stop_bridge(process: subprocess.Popen[bytes], *, brutal: bool) -> None:
    if process.poll() is None:
        process.kill() if brutal else process.terminate()
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=30)
    log = getattr(process, "log_file", None)
    if log is not None:
        log.close()


def _wait_for_health(
    port: int, process: subprocess.Popen[bytes], log_path: Path, timeout_s: float
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise HarnessError(
                f"bridge exited before answering {HEALTH_PATH}; log tail:\n{_log_tail(log_path)}"
            )
        try:
            status, _ = _request(port, HEALTH_PATH, timeout_s=2.0)
        except (urllib.error.URLError, OSError, TimeoutError):
            status = 0
        if status == 200:
            return
        time.sleep(0.25)
    raise HarnessError(f"bridge did not answer {HEALTH_PATH} in {timeout_s:.0f}s")


def _log_tail(log_path: Path, lines: int = 12) -> str:
    try:
        content = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return "<no log>"
    return "\n".join(content[-lines:])


def _run_rebuild(port: int, timeout_s: float) -> dict[str, Any]:
    status, started = _request(port, REBUILD_PATH, {})
    if status != 202 or not started.get("started"):
        raise HarnessError(f"rebuild did not start: http={status} {started.get('reason')!r}")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        _, payload = _request(port, REBUILD_PATH)
        rebuild = _storage(payload)
        if str(rebuild.get("status") or "") not in {"running", "cancelling", ""}:
            if rebuild.get("status") != "complete":
                raise HarnessError(
                    f"rebuild ended {rebuild.get('status')!r}: {rebuild.get('error')!r}"
                )
            return rebuild
        time.sleep(0.25)
    raise HarnessError("rebuild did not reach a terminal status")


def _proc_evidence(pid: int, db_path: Path) -> dict[str, Any]:
    """Handles on the live DB and thread count, when /proc is available."""
    proc_dir = Path(f"/proc/{pid}")
    if not proc_dir.is_dir():
        return {"available": False}
    handles = 0
    try:
        for entry in (proc_dir / "fd").iterdir():
            try:
                if os.readlink(entry) == str(db_path):
                    handles += 1
            except OSError:
                continue
        threads = len(list((proc_dir / "task").iterdir()))
    except OSError:
        return {"available": False}
    return {"available": True, "handlesOnLiveDb": handles, "threads": threads}


def _kill_window(validation_s: float) -> float:
    return min(KILL_MAX_S, max(KILL_MIN_S, validation_s * KILL_FRACTION))


def run(
    rows: int, data_dir: Path, keep: bool, start_timeout_s: float, rebuild_timeout_s: float
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, ok: bool, detail: Any) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    data_dir.mkdir(parents=True, exist_ok=True)
    db_path, validation_s = _seed(data_dir, rows)
    kill_after_s = _kill_window(validation_s)
    print(
        f"seeded {rows} current rows; validation {validation_s:.2f}s "
        f"-> kill window {kill_after_s:.2f}s"
    )
    check("validation-measured", validation_s > 0, f"{validation_s * 1000:.0f} ms")
    if validation_s < KILL_MIN_S * 2:
        print(
            "warning: the measured validation pass is very short; raise --rows "
            "for a wide kill window",
            file=sys.stderr,
        )

    port = _free_port()
    first_log = data_dir / "bridge-first.log"
    second_log = data_dir / "bridge-restarted.log"
    first = _start_bridge(data_dir, port, first_log)
    second: subprocess.Popen[bytes] | None = None
    try:
        _wait_for_health(port, first, first_log, start_timeout_s)
        rebuild = _run_rebuild(port, rebuild_timeout_s)
        backup_bytes = int(rebuild.get("backupBytes") or 0)
        backup_path = Path(str(rebuild.get("backupPath") or ""))
        check("rebuild-complete", rebuild.get("status") == "complete", rebuild.get("status"))
        check(
            "backup-retained",
            bool(rebuild.get("backupExists")) and backup_path.is_file(),
            backup_bytes,
        )

        before_delete = _proc_evidence(first.pid, db_path)
        seen_at = time.monotonic()
        status, started = _request(
            port, DELETE_PATH, {"confirm": True, "expectedBackupBytes": backup_bytes}
        )
        run_id = str(started.get("runId") or "")
        check(
            "delete-started",
            status == 202 and bool(started.get("started")),
            f"http={status} runId={run_id} reason={started.get('reason')!r}",
        )

        time.sleep(min(0.4, kill_after_s / 2))
        _, during_payload = _request(port, REBUILD_PATH)
        during = _proc_evidence(first.pid, db_path)
        check(
            "delete-reported-running",
            _delete_fields(during_payload).get("backupDeleteStatus") == "running",
            _delete_fields(during_payload).get("backupDeleteStatus"),
        )
        if before_delete.get("available") and during.get("available"):
            check(
                "validation-connection-open",
                int(during.get("handlesOnLiveDb") or 0)
                > int(before_delete.get("handlesOnLiveDb") or 0),
                f"{before_delete.get('handlesOnLiveDb')} -> {during.get('handlesOnLiveDb')}",
            )

        remaining = kill_after_s - (time.monotonic() - seen_at)
        if remaining > 0:
            time.sleep(remaining)
        killed_after_s = time.monotonic() - seen_at
        _stop_bridge(first, brutal=True)

        from src.storage.current_feed_rebuild import (
            read_rebuild_journal,
            rebuild_journal_path,
        )

        journal_path = rebuild_journal_path(db_path)
        journal = read_rebuild_journal(journal_path)
        backup_after_kill = backup_path.stat().st_size if backup_path.is_file() else 0
        print(f"SIGKILL at +{killed_after_s:.2f}s of a {validation_s:.2f}s validation pass")
        print(
            f"journal after kill: status={journal.get('backupDeleteStatus')!r} "
            f"runId={journal.get('backupDeleteRunId')!r} "
            f"finishedAt={journal.get('backupDeleteFinishedAt')!r}"
        )
        check(
            "journal-still-running-at-kill",
            journal.get("backupDeleteStatus") == "running",
            journal.get("backupDeleteStatus"),
        )
        check(
            "delete-had-not-finished",
            not str(journal.get("backupDeleteFinishedAt") or ""),
            journal.get("backupDeleteFinishedAt"),
        )
        check(
            "backup-untouched-by-kill",
            backup_after_kill == backup_bytes,
            f"{backup_bytes} -> {backup_after_kill}",
        )

        second = _start_bridge(data_dir, port, second_log)
        _wait_for_health(port, second, second_log, start_timeout_s)
        status, payload = _request(port, REBUILD_PATH)
        settled = _delete_fields(payload)
        error = str(settled.get("backupDeleteError") or "")
        print(f"first status read after restart: {json.dumps(settled, sort_keys=True)}")
        check("status-read-ok", status == 200, status)
        check(
            "settled-failed",
            settled.get("backupDeleteStatus") == "failed",
            settled.get("backupDeleteStatus"),
        )
        check("settled-error-names-restart", RESTART_ERROR_FRAGMENT in error, error)
        check(
            "settled-keeps-run-id",
            settled.get("backupDeleteRunId") == run_id,
            f"{run_id!r} -> {settled.get('backupDeleteRunId')!r}",
        )
        check(
            "no-deletion-recorded",
            not str(settled.get("backupDeletedAt") or ""),
            settled.get("backupDeletedAt"),
        )
        check(
            "backup-still-on-disk",
            bool(settled.get("backupExists"))
            and backup_path.is_file()
            and backup_path.stat().st_size == backup_bytes,
            f"exists={backup_path.is_file()} bytes={settled.get('backupBytes')}",
        )
        check(
            "rollback-still-available",
            settled.get("canRollback") is True,
            settled.get("canRollback"),
        )

        from src.storage.current_feed_rebuild import validate_current_feed_database

        try:
            live = validate_current_feed_database(db_path)
            live_ok, live_detail = live.get("quickCheck") == "ok", live.get("quickCheck")
        except Exception as exc:  # noqa: BLE001 - the check needs the failure text, not a trace
            live_ok, live_detail = False, f"{type(exc).__name__}: {exc}"
        check("live-database-still-valid", live_ok, live_detail)
    finally:
        _stop_bridge(first, brutal=False)
        if second is not None:
            _stop_bridge(second, brutal=False)

    ok = all(item["ok"] for item in checks)
    report: dict[str, Any] = {
        "ok": ok,
        "dataDir": str(data_dir),
        "rows": rows,
        "validationMs": round(validation_s * 1000, 1),
        "checks": checks,
    }
    if not keep:
        shutil.rmtree(data_dir, ignore_errors=True)
        report["dataDirRemoved"] = True
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prove the backup-delete restart settlement against a real bridge."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=4000,
        help="current-feed rows to seed; more rows widen the kill window",
    )
    parser.add_argument(
        "--data-dir", default="", help="throwaway directory (must be absent or empty)"
    )
    parser.add_argument("--keep", action="store_true", help="keep the data dir and bridge logs")
    parser.add_argument("--json", default="", help="write the full report to this path")
    parser.add_argument(
        "--timeout-multiplier",
        type=float,
        default=1.0,
        help="scale the bridge/rebuild timeouts for a slow machine",
    )
    args = parser.parse_args(argv)

    multiplier = max(0.1, args.timeout_multiplier)

    if args.data_dir:
        data_dir = Path(args.data_dir).expanduser().resolve()
        if data_dir.exists() and any(data_dir.iterdir()):
            print(f"refusing to run: {data_dir} is not empty", file=sys.stderr)
            return 2
    else:
        data_dir = DEFAULT_OUTPUT_ROOT / uuid.uuid4().hex[:12]

    if args.rows < 50:
        print(
            "refusing to run: --rows below 50 cannot produce a measurable validation pass",
            file=sys.stderr,
        )
        return 2

    try:
        report = run(
            args.rows,
            data_dir,
            args.keep,
            BRIDGE_START_TIMEOUT_S * multiplier,
            REBUILD_TIMEOUT_S * multiplier,
        )
    except HarnessError as exc:
        print(f"HARNESS FAILURE: {exc}", file=sys.stderr)
        return 1

    for item in report["checks"]:
        print(f"  [{'pass' if item['ok'] else 'FAIL'}] {item['name']}: {item['detail']}")
    if args.json:
        out = Path(args.json).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"report: {out}")
    print(f"RESULT: {'PASS' if report['ok'] else 'FAIL'}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
