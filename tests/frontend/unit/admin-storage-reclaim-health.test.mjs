import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createStorageBreakdownController } from "../../../frontend/admin/app/storage-breakdown.js";
import {
  renderStorageReclaimHealthHtml,
  storageReclaimWarnings
} from "../../../frontend/admin/render/storage-reclaim.js";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";

const DAY_SECONDS = 24 * 60 * 60;

function verdict(overrides = {}) {
  return {
    measuredStagingBytes: 900 * 1024 * 1024,
    measuredStagingAgeS: 2 * DAY_SECONDS,
    measuredStagingStale: false,
    lastRehearsalGateAdequate: true,
    ...overrides
  };
}

test("a fresh, adequate gate warns about nothing", () => {
  assert.deepEqual(storageReclaimWarnings(verdict()), []);
  assert.equal(renderStorageReclaimHealthHtml(verdict()), "");
});

test("an unmeasured host is not a warning", () => {
  // null means no run has measured a copy yet: the first-run state, not a defect.
  const unmeasured = verdict({
    measuredStagingBytes: 0,
    measuredStagingAgeS: null,
    measuredStagingStale: null,
    lastRehearsalGateAdequate: null
  });
  assert.deepEqual(storageReclaimWarnings(unmeasured), []);
  assert.deepEqual(storageReclaimWarnings(null), []);
  assert.deepEqual(storageReclaimWarnings(undefined), []);
});

test("a stale anchor names its age", () => {
  const warnings = storageReclaimWarnings(
    verdict({ measuredStagingAgeS: 41 * DAY_SECONDS, measuredStagingStale: true })
  );
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0].code, "stale-anchor");
  assert.match(warnings[0].text, /measured 41 days ago/);
  assert.match(warnings[0].text, /rehearsal/);
});

test("a stale anchor without a readable age still warns", () => {
  const warnings = storageReclaimWarnings(
    verdict({ measuredStagingAgeS: null, measuredStagingStale: true })
  );
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0].code, "stale-anchor");
  assert.doesNotMatch(warnings[0].text, /ago/);
});

test("a thin margin is a warning even while the anchor is fresh", () => {
  const warnings = storageReclaimWarnings(verdict({ lastRehearsalGateAdequate: false }));
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0].code, "thin-margin");
  assert.match(warnings[0].text, /larger than the free-space gate/);
});

test("both problems are reported, age first", () => {
  const warnings = storageReclaimWarnings(
    verdict({
      measuredStagingAgeS: 60 * DAY_SECONDS,
      measuredStagingStale: true,
      lastRehearsalGateAdequate: false
    })
  );
  assert.deepEqual(warnings.map(warning => warning.code), ["stale-anchor", "thin-margin"]);
  const html = renderStorageReclaimHealthHtml(
    verdict({ measuredStagingStale: true, lastRehearsalGateAdequate: false })
  );
  assert.match(html, /data-tone="warning"/);
  assert.match(html, /data-warning="stale-anchor"/);
  assert.match(html, /data-warning="thin-margin"/);
});

function controllerFixture({ reclaimPayload = null, reclaimFails = false } = {}) {
  const calls = [];
  const refs = {
    adminStorageTablesStatusEl: createElement(),
    adminStorageTablesBodyEl: createElement(),
    adminStorageReclaimHealthEl: createElement({ hidden: true })
  };
  const controller = createStorageBreakdownController({
    refs,
    getBridge: async (path, options) => {
      calls.push({ path, options });
      if (path === "/ops/storage-reclaim") {
        if (reclaimFails) throw new Error("bridge unavailable");
        return reclaimPayload || { storageReclaim: { verdict: verdict() } };
      }
      return { storage: {}, tableSizes: { status: "ok", tables: [] } };
    },
    postBridge: async () => ({ ok: true }),
    formatBytes: value => `${value} B`,
    shouldDefer: () => false
  });
  return { controller, refs, calls };
}

test("the panel shows a stale gate after loading the reclaim verdict", async () => {
  const { controller, refs, calls } = controllerFixture({
    reclaimPayload: {
      storageReclaim: { verdict: verdict({ measuredStagingAgeS: 41 * DAY_SECONDS, measuredStagingStale: true }) }
    }
  });

  await controller.load();

  assert.ok(calls.some(call => call.path === "/ops/storage-reclaim"));
  assert.equal(refs.adminStorageReclaimHealthEl.hidden, false);
  assert.match(refs.adminStorageReclaimHealthEl.innerHTML, /data-warning="stale-anchor"/);
  assert.match(refs.adminStorageReclaimHealthEl.innerHTML, /41 days/);
});

test("the panel shows a thin margin and hides nothing else", async () => {
  const { controller, refs } = controllerFixture({
    reclaimPayload: { storageReclaim: { verdict: verdict({ lastRehearsalGateAdequate: false }) } }
  });

  await controller.load();

  assert.equal(refs.adminStorageReclaimHealthEl.hidden, false);
  assert.match(refs.adminStorageReclaimHealthEl.innerHTML, /data-warning="thin-margin"/);
});

test("a healthy gate leaves the warning block empty and hidden", async () => {
  const { controller, refs } = controllerFixture();

  await controller.load();

  assert.equal(refs.adminStorageReclaimHealthEl.hidden, true);
  assert.equal(refs.adminStorageReclaimHealthEl.innerHTML, "");
});

test("a failed verdict read leaves the panel alone", async () => {
  const { controller, refs } = controllerFixture({ reclaimFails: true });

  await controller.load();

  // Still hidden: an unread verdict is unknown, not healthy.
  assert.equal(refs.adminStorageReclaimHealthEl.hidden, true);
  assert.equal(refs.adminStorageReclaimHealthEl.innerHTML, "");
});

test("the storage panel markup hosts the warning block", () => {
  const adminHtml = readFileSync(new URL("../../../admin.html", import.meta.url), "utf8");
  assert.match(adminHtml, /data-ui="admin-storage-reclaim-health"/);
  assert.match(adminHtml, /aria-live="polite" hidden/);
});
