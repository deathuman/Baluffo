import test from "node:test";
import assert from "node:assert/strict";
import { createAdminAuthController } from "../../../frontend/admin/app/auth.js";
import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";
import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { applyAdminTaskProgress } from "../../../frontend/admin/app/progress-ui.js";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { createAdminSyncController } from "../../../frontend/admin/app/sync.js";
import { createRestoreActiveRunWatches } from "../../../frontend/admin/app/live-task.js";
import { appendAdminLogRow } from "../../../frontend/admin/render.js";
import {
  FakeInputElement,
  createClassList,
  createDiscoveryControllerFixture,
  createElement,
  createFetcherControllerFixture,
  createRegistryControllerFixture,
  stubDateNow,
  stubScheduledTimers,
  withDom
} from "./helpers/admin-controller-test-helpers.mjs";

test("shared admin task progress renderer resets indeterminate state before determinate fill", () => {
  const rootEl = createElement({ style: {}, classList: createClassList(["hidden"]) });
  const barEl = createElement({ style: {} });
  const labelEl = createElement();

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: true,
    determinate: false,
    label: "Fetcher: Executing sources"
  });
  assert.equal(rootEl.classList.contains("indeterminate"), true);
  assert.equal(barEl.style.width, "36%");
  assert.equal(rootEl.attributes["aria-hidden"], "false");
  assert.equal(rootEl.attributes["aria-valuetext"], "Fetcher: Executing sources");

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: true,
    determinate: true,
    ratio: 0.65,
    label: "Fetcher: 65% complete"
  });
  assert.equal(rootEl.classList.contains("determinate"), true);
  assert.equal(rootEl.classList.contains("indeterminate"), false);
  assert.equal(barEl.style.width, "65%");
  assert.equal(barEl.style.left, "0");
  assert.equal(barEl.style.animation, "none");
  assert.equal(rootEl.attributes["aria-valuenow"], "65");
  assert.equal(rootEl.attributes["aria-valuetext"], "Fetcher: 65% complete");

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: false
  });
  assert.equal(rootEl.classList.contains("hidden"), true);
  assert.equal(barEl.style.width, "0%");
  assert.equal(rootEl.attributes["aria-hidden"], "true");

  applyAdminTaskProgress(rootEl, barEl, labelEl, {
    active: true,
    determinate: true,
    ratio: 1,
    label: "Discovery: Discovery completed"
  });
  assert.equal(rootEl.classList.contains("complete"), true);
  assert.equal(barEl.style.width, "100%");
  assert.equal(rootEl.attributes["aria-valuenow"], "100");
});

