import test from "node:test";
import assert from "node:assert/strict";

import { createStorageBreakdownController } from "../../../frontend/admin/app/storage-breakdown.js";
import {
  renderStorageRebuildHtml
} from "../../../frontend/admin/render/storage-tables.js";
import {
  createElement,
  stubScheduledTimers
} from "./helpers/admin-controller-test-helpers.mjs";

const RUNNING = {
  status: "running",
  phase: "copying",
  running: true,
  runId: "current_feed_rebuild_abc",
  currentGeneration: "jobs_20261111T005431574678000_575b66f4e88d",
  jobRowCount: 39_518,
  sourceRowCount: 159_198,
  stagingBytes: 1024,
  progress: { tablesDone: 4, tablesTotal: 16, stagingBytes: 1024 }
};

const COMPLETE = {
  status: "complete",
  phase: "complete",
  running: false,
  runId: "current_feed_rebuild_abc",
  currentGeneration: "jobs_20261111T005431574678000_575b66f4e88d",
  jobRowCount: 39_518,
  sourceRowCount: 159_198,
  stagingBytes: 1024,
  databaseBytes: 2048,
  reclaimedBytes: 226_491_000_000,
  backupExists: true,
  backupBytes: 226_491_760_640,
  canRollback: true,
  rowHash: "90e85df7e1d0154ec93f54464c5155db8d3acd62b3387fe41ab7bec4fcdcef13",
  sourceHash: "ebd387d0adf604a89c8c0e43ee53927e1a54f816b74a98ac7a3606f7f9672dd6",
  validation: {
    quickCheck: "ok",
    foreignKeyViolations: 0,
    rowHash: "90e85df7e1d0154ec93f54464c5155db8d3acd62b3387fe41ab7bec4fcdcef13",
    sourceHash: "ebd387d0adf604a89c8c0e43ee53927e1a54f816b74a98ac7a3606f7f9672dd6"
  }
};

function fixture({ startPayload = { ok: true, started: true, storageRebuild: RUNNING }, rebuildPayload = { ok: true, storageRebuild: COMPLETE } } = {}) {
  const listeners = {};
  const posts = [];
  const gets = [];
  let currentRebuild = rebuildPayload?.storageRebuild || COMPLETE;
  const refs = {
    adminStorageTablesStatusEl: createElement(),
    adminStorageTablesBodyEl: createElement(),
    adminStorageRebuildStatusEl: createElement(),
    adminStorageRebuildBodyEl: createElement(),
    adminStorageRebuildBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.start = handler;
      }
    }),
    adminStorageRebuildCancelBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.cancel = handler;
      }
    }),
    adminStorageRebuildRollbackBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.rollback = handler;
      }
    }),
    adminStorageRebuildBackupDeleteBtnEl: createElement({
      addEventListener(type, handler) {
        listeners.deleteBackup = handler;
      }
    })
  };
  const controller = createStorageBreakdownController({
    refs,
    getBridge: async path => {
      gets.push(path);
      if (path === "/ops/storage-rebuild") {
        return { ok: true, storageRebuild: currentRebuild };
      }
      return { storage: {} };
    },
    postBridge: async (path, body, options) => {
      posts.push({ path, body, options });
      if (path === "/ops/storage-rebuild") return startPayload;
      if (path.endsWith("/cancel")) {
        return { ok: true, cancelling: true, storageRebuild: { ...RUNNING, status: "cancelling" } };
      }
      if (path.endsWith("/rollback")) {
        currentRebuild = {
          ...COMPLETE,
          status: "rolled-back",
          backupExists: false,
          canRollback: false
        };
        return { ok: true, storageRebuild: currentRebuild };
      }
      if (path.endsWith("/backup-delete")) {
        return {
          ok: true,
          freedBytes: 1024,
          storageRebuild: { ...COMPLETE, backupExists: false, backupBytes: 0, canRollback: false }
        };
      }
      return {};
    },
    formatBytes: value => `${value} B`,
    shouldDefer: () => false
  });
  return { controller, refs, listeners, posts, gets };
}

test("rebuild button starts the background rebuild", async () => {
  const timers = stubScheduledTimers();
  const f = fixture();
  try {
    f.controller.init();
    await f.listeners.start();

    assert.equal(f.posts.length, 1);
    assert.equal(f.posts[0].path, "/ops/storage-rebuild");
    assert.deepEqual(f.posts[0].body, {});
    assert.equal(f.posts[0].options.affectsReachability, false);
    assert.equal(f.refs.adminStorageRebuildStatusEl.textContent, "Rebuild running");
    assert.equal(f.refs.adminStorageRebuildCancelBtnEl.hidden, false);
    assert.equal(f.refs.adminStorageRebuildBtnEl.hidden, true);
    assert.match(f.refs.adminStorageRebuildBodyEl.innerHTML, /Copying current feed and history/);
    assert.match(f.refs.adminStorageRebuildBodyEl.innerHTML, /4 of 16 tables/);
    assert.ok(timers.scheduled.length > 0);
    f.controller.stopRebuildPoll();
  } finally {
    timers.restore();
  }
});

test("a completed rebuild renders verified hashes and rollback controls", () => {
  const f = fixture();
  f.controller.acceptRebuild({ ok: true, storageRebuild: COMPLETE });

  assert.equal(f.refs.adminStorageRebuildStatusEl.textContent, "Rebuild complete");
  assert.match(f.refs.adminStorageRebuildBodyEl.innerHTML, /90e85df7e1d0/);
  assert.match(f.refs.adminStorageRebuildBodyEl.innerHTML, /ebd387d0adf6/);
  assert.match(f.refs.adminStorageRebuildBodyEl.innerHTML, /Quick check ok/);
  assert.match(f.refs.adminStorageRebuildBodyEl.innerHTML, /Rollback copy/);
  assert.equal(f.refs.adminStorageRebuildRollbackBtnEl.hidden, false);
  assert.equal(f.refs.adminStorageRebuildBackupDeleteBtnEl.hidden, false);
  f.controller.stopRebuildPoll();
});

test("stopping a rebuild posts the active run id", async () => {
  const f = fixture();
  f.controller.acceptRebuild({ ok: true, storageRebuild: RUNNING });

  await f.controller.cancelRebuild();

  assert.equal(f.posts.length, 1);
  assert.equal(f.posts[0].path, "/ops/storage-rebuild/cancel");
  assert.deepEqual(f.posts[0].body, { runId: RUNNING.runId });
  assert.match(f.refs.adminStorageRebuildStatusEl.textContent, /Stopping rebuild/);
  f.controller.stopRebuildPoll();
});

test("rollback restores the old database and reloads storage", async () => {
  const f = fixture();
  f.controller.acceptRebuild({ ok: true, storageRebuild: COMPLETE });

  await f.controller.rollbackRebuild();
  await Promise.resolve();

  assert.equal(f.posts[0].path, "/ops/storage-rebuild/rollback");
  assert.equal(f.refs.adminStorageRebuildStatusEl.textContent, "Old database restored");
  assert.equal(f.refs.adminStorageRebuildRollbackBtnEl.hidden, true);
  assert.ok(f.gets.includes("/ops/storage-health"));
  f.controller.stopRebuildPoll();
});

test("deleting the retained backup hides rollback", async () => {
  const f = fixture();
  f.controller.acceptRebuild({ ok: true, storageRebuild: COMPLETE });

  await f.controller.deleteRebuildBackup();

  assert.equal(f.posts[0].path, "/ops/storage-rebuild/backup-delete");
  assert.equal(f.refs.adminStorageRebuildRollbackBtnEl.hidden, true);
  assert.equal(f.refs.adminStorageRebuildBackupDeleteBtnEl.hidden, true);
  assert.match(f.refs.adminStorageRebuildBodyEl.innerHTML, /No pre-rebuild database is retained/);
});

test("a superseded rebuild is shown as a warning, not a failure to install", () => {
  const f = fixture();
  f.controller.acceptRebuild({
    ok: true,
    storageRebuild: {
      ...COMPLETE,
      status: "superseded",
      error: "a database write committed while the rebuild was copying"
    }
  });

  assert.match(f.refs.adminStorageRebuildStatusEl.textContent, /superseded/i);
  assert.equal(f.refs.adminStorageRebuildStatusEl.attributes["data-tone"], "warning");
  assert.match(f.refs.adminStorageRebuildBodyEl.innerHTML, /database write committed/);
  f.controller.stopRebuildPoll();
});

test("the rebuild renderer keeps a failed run's error visible", () => {
  const html = renderStorageRebuildHtml(
    {
      status: "failed",
      phase: "failed",
      error: "not enough free disk space for the rebuilt copy",
      backupExists: false
    },
    value => `${value} B`
  );

  assert.match(html, /Failed/);
  assert.match(html, /not enough free disk space/);
  assert.match(html, /No pre-rebuild database is retained/);
});
