// Storage-signal integrity: the Action Center must name the recorded cause and date a
// WAL that cannot be checkpointed, including the post-S1.2 healthy-but-busy payload.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { cleanSyncPayload, createActionCenterFixture as createFixture } from "./helpers/action-center-fixture.mjs";

const STORAGE_GATE_CASES = JSON.parse(
  readFileSync(new URL("../../fixtures/storage-alert-gate-cases.json", import.meta.url), "utf8")
);

async function storageSignalHtml(storage) {
  const { refs, controller } = createFixture({
    getBridge: async path => {
      if (path === "/ops/fetch-kpis?view=summary") {
        return { alerts: [], kpis: { lastSuccessfulFetchAge: "1h", failedSourceRatioLatest: 0 } };
      }
      if (path === "/sync/status?view=summary") return cleanSyncPayload();
      if (path === "/ops/storage-health") return { ok: storage.healthy !== false, storage };
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

test("a busy record whose WAL has already been emptied no longer warns", async () => {
  // The live 0.3.064 payload: the store is healthy, the WAL is 0 bytes, and the
  // record still names the 926 MB the 2026-11-17 attempt saw. The record is only
  // rewritten by a later attempt, so following it alone keeps a resolved condition
  // on the panel for as long as the process lives.
  const html = await storageSignalHtml(
    {
      healthy: true,
      quickCheck: "deferred-large-database",
      quickCheckDeferred: true,
      walBytes: 0,
      databaseBytes: 272695296,
      walMaintenance: {
        status: "busy",
        mode: "PASSIVE",
        attemptedAt: new Date(Date.now() - 12 * 3600 * 1000).toISOString(),
        attemptAgeS: 12 * 3600,
        walBytes: 926311992,
        databaseBytes: 545017856,
        checkpointThresholdBytes: 67108864
      }
    },
    { ok: true }
  );

  assert.doesNotMatch(html, /data-signal="storage_health"/);
  assert.doesNotMatch(html, /WAL checkpoint/);
});

test("a busy record whose WAL is still large warns with live sizes and its age", async () => {
  const html = await storageSignalHtml(
    {
      healthy: true,
      quickCheck: "deferred-large-database",
      quickCheckDeferred: true,
      walBytes: 943718400,
      databaseBytes: 272695296,
      walMaintenance: {
        status: "busy",
        attemptedAt: new Date(Date.now() - 25 * 60 * 1000).toISOString(),
        attemptAgeS: 1500,
        walBytes: 943718400,
        databaseBytes: 272695296,
        checkpointThresholdBytes: 67108864
      }
    },
    { ok: true }
  );

  assert.match(html, /data-signal="storage_health"/);
  assert.match(html, /WAL checkpoint could not finish/);
  assert.match(html, /900\.0 MB WAL on 260\.1 MB database/);
  assert.match(html, /25m ago/);
  assert.match(html, /action-center-signal-warning/);
});

test("storage copy names the attempt's sizes only when they differ from the live files", async () => {
  const html = await storageSignalHtml(
    {
      healthy: true,
      quickCheck: "deferred-large-database",
      quickCheckDeferred: true,
      walBytes: 734003200,
      databaseBytes: 272695296,
      walMaintenance: {
        status: "busy",
        attemptedAt: new Date(Date.now() - 90 * 1000).toISOString(),
        attemptAgeS: 120,
        walBytes: 943718400,
        databaseBytes: 272695296,
        checkpointThresholdBytes: 67108864
      }
    },
    { ok: true }
  );

  assert.match(html, /700\.0 MB WAL on 260\.1 MB database/);
  assert.match(html, /the attempt saw 900\.0 MB WAL on 260\.1 MB database/);
  assert.match(html, /2m ago/);
});

test("a busy record with an unreadable live WAL still warns", async () => {
  // A null, blank or unparsable size is unknown, not zero. Treating it as an empty
  // WAL would silence a checkpoint that is still stuck.
  for (const walBytes of [null, "", "n/a", -1]) {
    const html = await storageSignalHtml({
      healthy: true,
      walBytes,
      databaseBytes: 272695296,
      walMaintenance: {
        status: "busy",
        attemptedAt: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
        checkpointThresholdBytes: 67108864
      }
    });
    assert.match(html, /data-signal="storage_health"/, `walBytes=${String(walBytes)}`);
    assert.match(html, /WAL checkpoint could not finish/, `walBytes=${String(walBytes)}`);
    assert.match(html, /action-center-signal-warning/, `walBytes=${String(walBytes)}`);
  }
});

test("the storage signal gate matches the shared payload matrix", async () => {
  // tests/admin/test_admin_bridge_ops_storage_alert.py runs the same matrix through
  // storage_health_alert: one verdict per payload, or the surfaces drift again.
  for (const item of STORAGE_GATE_CASES) {
    const html = await storageSignalHtml(item.payload.storage);
    const hasSignal = /data-signal="storage_health"/.test(html);
    assert.equal(hasSignal, item.expect.alert, item.name);
    if (item.expect.alert) {
      assert.ok(html.includes(`action-center-signal-${item.expect.severity}`), item.name);
    }
  }
});
