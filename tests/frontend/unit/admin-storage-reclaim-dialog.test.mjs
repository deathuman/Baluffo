/**
 * The Runtime Database Storage reclaim controls: Rehearse, Run and Stop against the
 * shipped routes, container modal and desktop inline from one payload, the measured
 * gate's warnings, and no polling timer.
 *
 * The run belongs to the bridge, so every case here asserts only that the panel
 * reflects a payload, never that it owns or drives a copy.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createStorageReclaimController } from "../../../frontend/admin/app/storage-reclaim-panel.js";
import {
  reclaimButtonState,
  reclaimPhaseLabel,
  reclaimStatusText,
  renderStorageReclaimModalHtml
} from "../../../frontend/admin/render/storage-reclaim-dialog.js";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";

const DAY_SECONDS = 24 * 60 * 60;
const MB = 1024 * 1024;

function verdict(overrides = {}) {
  return {
    verdict: "bloat",
    blockers: [],
    measuredStagingBytes: 900 * MB,
    measuredStagingAgeS: 2 * DAY_SECONDS,
    measuredStagingStale: false,
    lastRehearsalGateAdequate: true,
    ...overrides
  };
}

function payload(reclaim = {}, verdictOverrides = {}) {
  return {
    storageReclaim: {
      scope: "inline",
      status: "idle",
      phase: "idle",
      runId: "",
      mode: "",
      verdict: verdict(verdictOverrides),
      ...reclaim
    }
  };
}

function refs() {
  return {
    adminStorageReclaimStatusEl: createElement(),
    adminStorageReclaimDialogEl: createElement(),
    adminStorageReclaimHealthEl: createElement({ hidden: true }),
    adminStorageReclaimRehearseBtnEl: createElement(),
    adminStorageReclaimRunBtnEl: createElement(),
    adminStorageReclaimStopBtnEl: createElement({ hidden: true }),
    adminStorageReclaimModalEl: createElement({ hidden: true }),
    adminStorageReclaimModalBodyEl: createElement(),
    adminStorageReclaimModalStopBtnEl: createElement(),
    adminStorageReclaimModalHideBtnEl: createElement()
  };
}

function controller(target, { get, post } = {}) {
  const calls = { get: [], post: [] };
  const instance = createStorageReclaimController({
    refs: target,
    getBridge: async url => {
      calls.get.push(url);
      return get ? get(url) : payload();
    },
    postBridge: async (url, body) => {
      calls.post.push({ url, body });
      return post ? post(url, body) : payload({ status: "running", runId: "run-1", mode: "reclaim" });
    },
    formatBytes: value => `${Math.round(value / MB)} MB`
  });
  return { instance, calls };
}

const settle = () => new Promise(resolve => setImmediate(resolve));

test("an idle payload offers both starts and no Stop", () => {
  const target = refs();
  const { instance } = controller(target);

  instance.accept(payload());

  assert.equal(target.adminStorageReclaimStatusEl.textContent, "Not started");
  assert.equal(target.adminStorageReclaimRehearseBtnEl.disabled, false);
  assert.equal(target.adminStorageReclaimRunBtnEl.disabled, false);
  assert.equal(target.adminStorageReclaimStopBtnEl.hidden, true);
  assert.equal(target.adminStorageReclaimModalEl.hidden, true);
});

test("a container run opens the modal from the same payload", () => {
  const target = refs();
  const { instance } = controller(target);

  instance.accept(payload({ scope: "modal", status: "running", phase: "copying", runId: "run-7", mode: "reclaim" }));

  assert.equal(target.adminStorageReclaimModalEl.hidden, false);
  assert.match(target.adminStorageReclaimModalBodyEl.innerHTML, /copying the database/);
  assert.match(target.adminStorageReclaimModalBodyEl.innerHTML, /only Stop cancels it/);
  assert.equal(target.adminStorageReclaimStatusEl.textContent, "Reclaim running — copying the database");
  assert.equal(target.adminStorageReclaimStopBtnEl.hidden, false);
  assert.equal(target.adminStorageReclaimModalStopBtnEl.disabled, false);
  // Starting is refused while the bridge owns the run; Stop is the only action.
  assert.equal(target.adminStorageReclaimRehearseBtnEl.disabled, true);
  assert.equal(target.adminStorageReclaimRunBtnEl.disabled, true);
});

test("desktop renders the same run inline with no modal", () => {
  const target = refs();
  const { instance } = controller(target);

  instance.accept(payload({ scope: "inline", status: "running", phase: "copying", runId: "run-8", mode: "reclaim" }));

  assert.equal(target.adminStorageReclaimModalEl.hidden, true);
  assert.match(target.adminStorageReclaimDialogEl.innerHTML, /copying the database/);
  assert.equal(target.adminStorageReclaimStopBtnEl.hidden, false);
});

test("Rehearse posts the dry run and then reads the route with refresh", async () => {
  const target = refs();
  const { instance, calls } = controller(target, {
    get: () => payload({ status: "running", phase: "evaluating", runId: "run-2", mode: "rehearsal" }),
    post: () => ({ ok: true, started: true, storageReclaim: { status: "running", mode: "rehearsal", runId: "run-2" } })
  });

  instance.accept(payload());
  await instance.rehearse();
  await settle();

  assert.deepEqual(calls.post, [{ url: "/ops/storage-reclaim", body: { dryRun: true } }]);
  assert.equal(calls.get.at(-1), "/ops/storage-reclaim?refresh=1");
  assert.equal(target.adminStorageReclaimRehearseBtnEl.textContent, "Rehearsing…");
});

test("Run posts an explicit start, not an automatic one", async () => {
  const target = refs();
  const { instance, calls } = controller(target);

  instance.accept(payload());
  await instance.run();
  await settle();

  // No `auto` field: the backend defaults to an explicit start that overrides
  // the cooldown and the not-armed state.
  assert.deepEqual(calls.post, [{ url: "/ops/storage-reclaim", body: {} }]);
});

test("a safety blocker refuses Run but not Rehearse, and says why", () => {
  const target = refs();
  const { instance } = controller(target);

  instance.accept(payload({}, { blockers: ["free-space"] }));

  assert.equal(target.adminStorageReclaimRunBtnEl.disabled, true);
  assert.equal(target.adminStorageReclaimRehearseBtnEl.disabled, false);
  assert.match(target.adminStorageReclaimRunBtnEl.attributes["data-tooltip"], /Free space is below the copy gate/);
  assert.match(target.adminStorageReclaimDialogEl.innerHTML, /Free space is below the copy gate/);
});

test("the trigger guards do not disable Run", () => {
  const target = refs();
  const { instance } = controller(target);

  // `cooldown` and `auto-disabled` gate automatic starts; a click overrides both.
  instance.accept(payload({}, { blockers: ["cooldown", "auto-disabled"] }));

  assert.equal(target.adminStorageReclaimRunBtnEl.disabled, false);
  assert.doesNotMatch(target.adminStorageReclaimDialogEl.innerHTML, /not armed/);
});

test("the measured gate's stale and thin warnings render beside the controls", () => {
  const target = refs();
  const { instance } = controller(target);

  instance.accept(
    payload({}, {
      measuredStagingAgeS: 41 * DAY_SECONDS,
      measuredStagingStale: true,
      lastRehearsalGateAdequate: false
    })
  );

  assert.equal(target.adminStorageReclaimHealthEl.hidden, false);
  assert.match(target.adminStorageReclaimHealthEl.innerHTML, /free-space gate comes from a copy/);
  assert.match(target.adminStorageReclaimHealthEl.innerHTML, /measured 41 days ago/);
  assert.match(target.adminStorageReclaimHealthEl.innerHTML, /thin-margin/);
});

test("the panel explains an armed host from the verdict's own decision", () => {
  const target = refs();
  const { instance } = controller(target);

  instance.accept(
    payload({}, {
      autoStart: {
        enabled: true,
        source: "rehearsal",
        switch: "unset",
        armed: true,
        reason: "rehearsal-adequate",
        decidedAt: "2026-11-16T10:00:00Z",
        evidence: { stagingBytes: 900 * MB, gateAdequate: true }
      }
    })
  );

  const html = target.adminStorageReclaimDialogEl.innerHTML;
  assert.match(html, /Auto-reclaim is armed: the last rehearsal measured an adequate copy/);
  assert.match(html, /Decided 2026-11-16/);
  assert.match(html, /The recorded copy was 900 MB/);
  assert.match(html, /data-auto-source="rehearsal"/);
});

test("the panel says when no rehearsal has armed auto", () => {
  const target = refs();
  const { instance } = controller(target);

  instance.accept(
    payload({}, {
      autoStart: {
        enabled: false,
        source: "none",
        switch: "unset",
        armed: false,
        reason: "not-armed",
        decidedAt: "",
        evidence: null
      }
    })
  );

  const html = target.adminStorageReclaimDialogEl.innerHTML;
  assert.match(html, /Auto-reclaim is not armed yet: a rehearsal has to measure this host first/);
  assert.match(html, /data-auto-source="none"/);
});

test("the panel names the kill switch over a recorded arm", () => {
  const target = refs();
  const { instance } = controller(target);

  instance.accept(
    payload({}, {
      autoStart: {
        enabled: false,
        source: "env",
        switch: "off",
        armed: true,
        reason: "kill-switch",
        decidedAt: "2026-11-16T10:00:00Z",
        evidence: { stagingBytes: 900 * MB }
      }
    })
  );

  const html = target.adminStorageReclaimDialogEl.innerHTML;
  assert.match(html, /set to off, even though a rehearsal measured this host/);
  assert.match(html, /data-auto-source="env"/);
  assert.match(html, /data-tone="warning"/);
});


test("an unmeasured gate is not a warning", () => {
  const target = refs();
  const { instance } = controller(target);

  instance.accept(payload({}, {
    measuredStagingBytes: 0,
    measuredStagingAgeS: null,
    measuredStagingStale: null,
    lastRehearsalGateAdequate: null
  }));

  assert.equal(target.adminStorageReclaimHealthEl.hidden, true);
  assert.equal(target.adminStorageReclaimHealthEl.innerHTML, "");
});

test("Stop posts the current run id and reports a stale one", async () => {
  const target = refs();
  const { instance, calls } = controller(target, {
    post: () => ({ ok: true, stale: true, storageReclaim: { status: "idle", runId: "run-9", mode: "reclaim" } })
  });

  instance.accept(payload({ status: "running", runId: "run-8", mode: "reclaim" }));
  await instance.stop();

  assert.deepEqual(calls.post, [{ url: "/ops/storage-reclaim/cancel", body: { runId: "run-8" } }]);
  assert.match(target.adminStorageReclaimDialogEl.innerHTML, /stale/);
});

test("Stop without a run id refuses instead of posting", async () => {
  const target = refs();
  const { instance, calls } = controller(target);

  instance.accept(payload({ status: "running", runId: "", mode: "reclaim" }));
  await instance.stop();

  assert.equal(calls.post.length, 0);
  assert.match(target.adminStorageReclaimDialogEl.innerHTML, /run id/);
});

test("hiding the container modal leaves the run going", async () => {
  const target = refs();
  const { instance, calls } = controller(target);

  instance.accept(payload({ scope: "modal", status: "running", runId: "run-5", mode: "reclaim" }));
  instance.dismissModal();

  assert.equal(target.adminStorageReclaimModalEl.hidden, true);
  assert.equal(calls.post.length, 0);
  assert.equal(target.adminStorageReclaimStopBtnEl.hidden, false);
});

test("Refresh reads the route without the verdict only when asked", async () => {
  const target = refs();
  const { instance, calls } = controller(target);

  await instance.refresh();
  await instance.refresh({ refreshVerdict: true });

  assert.deepEqual(calls.get, ["/ops/storage-reclaim", "/ops/storage-reclaim?refresh=1"]);
});

test("the panel has no polling timer", () => {
  const source = readFileSync(
    new URL("../../../frontend/admin/app/storage-reclaim-panel.js", import.meta.url),
    "utf8"
  );

  // Progress arrives when the user refreshes; nothing schedules itself.
  assert.doesNotMatch(source, /\bsetTimeout\b|\bsetInterval\b/);
});

test("a run's phase and status read as sentences", () => {
  assert.equal(reclaimPhaseLabel("stopping-retention"), "stopping retention");
  assert.deepEqual(reclaimStatusText({ status: "running", phase: "verifying", mode: "reclaim" }), {
    text: "Reclaim running — verifying the copy",
    tone: ""
  });
  assert.deepEqual(reclaimStatusText({ status: "failed", mode: "rehearsal", error: "disk full" }), {
    text: "Rehearsal failed: disk full",
    tone: "warning"
  });
  assert.equal(renderStorageReclaimModalHtml({ scope: "inline", status: "running" }), "");
  assert.equal(reclaimButtonState({ status: "running", runId: "r" }).stop.hidden, false);
});
