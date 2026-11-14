# Controlled Current-Feed Full Rebuild Plan

> - **Class:** cleanup
> - **Trigger:** explicit user approval of one maintenance window for the live rebuild
> - **Verified against:** `ffbc8772` plus the current working tree
> - **Status:** Deployment approved (2026-11-14) — release `0.3.061` is being pushed so the live bridge gains the rebuild routes; run preflight only after the Umbrel app update and a clean storage-health gate. No rebuild has started.

## Scope

Rebuild the live Baluffo runtime database around its published jobs-feed
generation, keeping every non-feed table and only the current `jobs` /
`job_sources` rows. This is the one-shot alternative to deleting historical
generations in bounded batches.

Last recorded live target:

- Host: `umbrel@192.168.50.61`, bridge `http://192.168.50.61:8877`
- Data directory: `/home/umbrel/umbrel/app-data/deathuman-baluffo/data`
- Database: `baluffo-runtime.db` (226,491,760,640 B at the last measurement)
- Live generation: `jobs_20261111T005431574678000_575b66f4e88d`
- Last recorded free space: 1,419,768,210,432 B

Re-measure every value below during preflight; do not trust these historical
numbers for the approval decision.

## Preflight (read-only)

Create an evidence directory and capture every response before starting:

```bash
export BALUFFO_BASE=http://192.168.50.61:8877
export BALUFFO_DATA=/home/umbrel/umbrel/app-data/deathuman-baluffo/data
export STAMP=$(date +%Y%m%dT%H%M%S)
mkdir -p "_out/rebuild-preflight-$STAMP"

curl -fsS "$BALUFFO_BASE/ops/storage-health" > "_out/rebuild-preflight-$STAMP/storage-health.json"
curl -fsS "$BALUFFO_BASE/ops/storage-rebuild" > "_out/rebuild-preflight-$STAMP/storage-rebuild-before.json"
ssh umbrel@192.168.50.61 \
  "df -B1 --output=avail '$BALUFFO_DATA'; stat -c '%n %s %Y' '$BALUFFO_DATA'/baluffo-runtime.db*; sha256sum '$BALUFFO_DATA'/jobs-unified.json* '$BALUFFO_DATA'/jobs-unified-light.json* '$BALUFFO_DATA'/jobs-unified-startup.json* 2>/dev/null || true" \
  > "_out/rebuild-preflight-$STAMP/files-before.txt"
```

Required preflight checks:

1. No active fetch, discovery, sync, or other bridge task, and no scheduled fetch
   due during the window.
2. `/ops/storage-health` reports `ok: true`, `storage.healthy: true`, and:
   - `compaction.heldByActiveTask: false`
   - `runtimeRetention.running: false`
   - `compaction.running: false`
   - `writeLock.held: false`
3. `/ops/storage-rebuild` reports `status: "idle"`, `backupExists: false`, and no
   `running`/`staged`/`backup-created` journal entry.
4. No stale retained copy or staging file exists:
   `baluffo-runtime.db.pre-rebuild`,
   `baluffo-runtime.db.*.rebuild.tmp`, or a journal in an in-flight state.
   If a retained copy exists, stop: choose **Roll back** or **Delete old
   database** with the user before starting a new rebuild.
5. Free disk is at least the gate value
   `min(3 GiB, max(256 MiB, source_bytes / 100)) + 64 MiB`. At 226 GB that is
   about 2.32 GB; the last recorded free space was about 1.4 TB.
6. Capture the baseline feed state read-only:

```bash
python3 - <<'PY'
import sqlite3
path = "/home/umbrel/umbrel/app-data/deathuman-baluffo/data/baluffo-runtime.db"
conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
row = conn.execute(
    "SELECT current_generation,row_count,row_hash,source_count,source_hash "
    "FROM job_feed_state WHERE id=1"
).fetchone()
print(dict(zip(("generation", "jobRowCount", "rowHash", "sourceRowCount", "sourceHash"), row)))
conn.close()
PY
```

7. Confirm the bridge is otherwise serving normally (`/jobs` loads), and record
   the exact bridge version/commit in the evidence directory.
8. Announce the maintenance window and keep the operator available for the whole
   copy and validation period.

If any check fails, do not start the rebuild.

## Start (approval required)

Only after the user explicitly approves this plan, start one run and record its
`runId`:

```bash
curl -fsS -X POST "$BALUFFO_BASE/ops/storage-rebuild" \
  > "_out/rebuild-preflight-$STAMP/storage-rebuild-start.json"
```

Expected: HTTP `202`, `started: true`, and a `storageRebuild.runId`. A
`started: false` response is a block, not a retry signal; resolve its `reason`
(`held-active-task`, `compaction-running`, or `backup-exists`) first.

## Monitor

Poll at 2–5 second intervals and stop polling only on a terminal status:

```bash
export LATEST="_out/rebuild-preflight-$STAMP/storage-rebuild-latest.json"
while true; do
  curl -fsS "$BALUFFO_BASE/ops/storage-rebuild" > "$LATEST"
  python3 -c '
import json, sys
state = json.load(open(sys.argv[1], encoding="utf-8"))["storageRebuild"]
print(state["status"], state["phase"], state.get("elapsedMs"), state.get("error", ""))
raise SystemExit(0 if state["status"] not in {"running", "cancelling"} else 1)
' "$LATEST"
  [ $? -eq 0 ] && break
  sleep 3
done
```

Phases are `queued`, `stopping-retention`, `checking-space`, `copying`,
`installing`, then a terminal status. A cancellation request is honored before
installation; the live database is untouched until the swap.

## Validation

A run is successful only when all of these are true:

1. `storageRebuild.status == "complete"` and `error == ""`.
2. `validation.quickCheck == "ok"` and `validation.foreignKeyViolations == 0`.
3. `currentGeneration`, `jobRowCount`, `sourceRowCount`, `rowHash`, and
   `sourceHash` equal the preflight values.
4. `backupExists: true`, `canRollback: true`, and the retained backup path is
   present.
5. `databaseBytes` is in the expected 0.5–1.5 GB range (or record and explain a
   different size); `reclaimedBytes` is non-negative.
6. Independent read-only validation succeeds:

```bash
python3 - <<'PY'
import sqlite3
path = "/home/umbrel/umbrel/app-data/deathuman-baluffo/data/baluffo-runtime.db"
conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
print("quick_check", conn.execute("PRAGMA quick_check").fetchone()[0])
print("foreign_key_violations", len(conn.execute("PRAGMA foreign_key_check").fetchall()))
state = conn.execute(
    "SELECT current_generation,row_count,row_hash,source_count,source_hash "
    "FROM job_feed_state WHERE id=1"
).fetchone()
print("feed_state", state)
conn.close()
PY
```

7. The three JSON projections have the same SHA-256 values as preflight. The
   rebuild changes only the database, so a projection change is a failure to
   investigate before accepting the run.
8. `/ops/storage-health` is healthy, `/jobs` loads, and no bridge/container error
   appears in the logs after the swap.
9. Keep the retained backup for at least one normal fetch/refresh cycle and the
   agreed observation window. Do not delete it until the user separately
   approves **Delete old database**.

## Rollback

Rollback is available while the retained backup exists and no rebuild is
running. Use the API first:

```bash
curl -fsS -X POST "$BALUFFO_BASE/ops/storage-rebuild/rollback" \
  > "_out/rebuild-preflight-$STAMP/storage-rebuild-rollback.json"
```

Verify `ok: true`, `storageRebuild.status == "rolled-back"`, the old
`currentGeneration` and hashes are live again, the retained copy is consumed,
`/jobs` loads, and the JSON projection hashes still match preflight.

If the rebuild fails before installation, the live database is untouched; remove
only the staging file after reading the journal, then retry when the cause is
fixed. If the bridge restarts mid-swap, restart it once and read
`/ops/storage-rebuild`; do not manually replace database files.

Manual file restoration is a last resort only with explicit user approval after
stopping every writer. Preserve the rebuilt file for forensics, keep the
retained copy until the restored database validates, and never delete the only
copy of either database.

## Stop conditions

Stop and ask the user instead of improvising if:

- free space is unknown or below the gate;
- any active task or write lock appears during preflight or copy;
- the run reports `superseded`, `interrupted`, `cancelled`, or `failed`;
- validation hashes/counts differ, JSON projections changed, or `/jobs` fails;
- the retained backup is missing or rollback is not available.
