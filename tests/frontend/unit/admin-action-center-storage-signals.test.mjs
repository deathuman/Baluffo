// Storage-signal integrity: the Action Center must name the recorded cause and date a
// WAL that cannot be checkpointed, including the post-S1.2 healthy-but-busy payload.
import test from "node:test";
import assert from "node:assert/strict";
import { cleanSyncPayload, createActionCenterFixture as createFixture } from "./helpers/action-center-fixture.mjs";

async function storageSignalHtml(storage, { ok = false } = {}) {
  const { refs, controller } = createFixture({
    getBridge: async path => {
      if (path === "/ops/fetch-kpis?view=summary") {
        return { alerts: [], kpis: { lastSuccessfulFetchAge: "1h", failedSourceRatioLatest: 0 } };
      }
      if (path === "/sync/status?view=summary") return cleanSyncPayload();
      if (path === "/ops/storage-health") return { ok, storage };
      return null;
    }
  });
  await controller.pollActionCenter({ includeStorage: true });
  return refs.actionCenterItemsEl.innerHTML;
}

test("storage copy names the recorded cause instead of guessing an integrity failure", async () => {
  const at = new Date(Date.now() - 90 * 60 * 1000).toISOString();
  const expectedAt = `${at.slice(0, 19).replace("T", " ")} UTC`;

  // A 0.3.062 payload: no unhealthyReason, and the integrity check was deferred at
  // this database size. It must be read as the failed checkpoint it was.
  const legacy = await storageSignalHtml({
    healthy: false,
    quickCheck: "deferred-large-database",
    quickCheckDeferred: true,
    databaseBytes: 555212800,
    walBytes: 997831072,
    walMaintenance: {
      status: "failed",
      mode: "TRUNCATE",
      error: "SQLite WAL checkpoint failed after busy retry attempts: (1, -1, -1)"
    }
  });
  assert.match(legacy, /WAL checkpoint failed/);
  assert.match(legacy, /951\.6 MB WAL/);
  assert.match(legacy, /529\.5 MB database/);
  assert.match(legacy, /attempt date not recorded/);
  assert.match(legacy, /integrity check deferred/);
  assert.doesNotMatch(legacy, /integrity check failed/);
  assert.match(legacy, /action-center-signal-warning/);

  // The attributed payload names the code and when the store gave up.
  const dated = await storageSignalHtml({
    healthy: false,
    quickCheck: "deferred-large-database",
    quickCheckDeferred: true,
    unhealthyReason: {
      code: "wal_checkpoint_busy",
      message: "SQLite WAL checkpoint busy: (1, -1, -1)",
      at
    },
    walMaintenance: { status: "busy", databaseBytes: 286019584, walBytes: 104857600 }
  });
  assert.match(dated, /WAL checkpoint could not finish/);
  assert.match(dated, /100\.0 MB WAL on 272\.8 MB database/);
  assert.ok(dated.includes(`on ${expectedAt}`), "the signal must date the cause");
  assert.match(dated, /SQLite WAL checkpoint busy/);
  assert.doesNotMatch(dated, /integrity check failed/);

  // A real integrity failure still says exactly that.
  const corrupt = await storageSignalHtml({
    healthy: false,
    quickCheck: "*** in database main ***",
    quickCheckDeferred: false
  });
  assert.match(corrupt, /SQLite integrity check failed/);

  // A write failure names its own cause and date, not the checkpoint.
  const writeFailure = await storageSignalHtml({
    healthy: false,
    unhealthyReason: {
      code: "write_lock_timeout",
      message: "store write lock still held",
      at
    }
  });
  assert.match(writeFailure, /Storage write lock timed out/);
  assert.match(writeFailure, /store write lock still held/);
  assert.ok(writeFailure.includes(`on ${expectedAt}`));
  assert.doesNotMatch(writeFailure, /integrity check failed/);
});

test("a healthy store with a busy WAL checkpoint still warns instead of going silent", async () => {
  // The payload S1.2 actually produces: the store stays healthy and only
  // `walMaintenance` records that a checkpoint could not finish. The maintenance
  // record itself must carry the warning, or a 1 GB WAL is invisible.
  const attemptedAt = new Date(Date.now() - 25 * 60 * 1000).toISOString();
  const expectedAttemptAt = `${attemptedAt.slice(0, 19).replace("T", " ")} UTC`;

  const html = await storageSignalHtml(
    {
      healthy: true,
      quickCheck: "deferred-large-database",
      quickCheckDeferred: true,
      walMaintenance: {
        status: "busy",
        attemptedAt,
        databaseBytes: 555212800,
        walBytes: 997831072
      }
    },
    { ok: true }
  );

  assert.match(html, /data-signal="storage_health"/);
  assert.match(html, /WAL checkpoint could not finish/);
  assert.match(html, /951\.6 MB WAL/);
  assert.match(html, /529\.5 MB database/);
  assert.ok(html.includes(`on ${expectedAttemptAt}`), "the warning must date the busy attempt");
  assert.match(html, /action-center-signal-warning/);
  assert.doesNotMatch(html, /integrity check failed/);
});
