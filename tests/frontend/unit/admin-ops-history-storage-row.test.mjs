import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminOpsHistory } from "../../../frontend/admin/render.js";
import { createRenderEl as makeEl } from "./helpers/dom-test-helpers.mjs";

test("admin ops history: storage rows use their own type label and offer no abort action", () => {
  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "storage_maintenance",
        taskType: "storage_maintenance",
        runId: "retention_1",
        active: true,
        startedAt: "2026-03-08T10:00:00.000Z",
        heartbeatAt: "2026-03-08T10:00:02.000Z",
        status: "running",
        taskProgress: {
          active: true,
          phaseKey: "deleting-generations",
          phaseLabel: "Deleting generations",
          mode: "indeterminate"
        },
        summary: {}
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, /Maintenance/);
  assert.match(historyEl.innerHTML, /Deleting generations/);
  assert.doesNotMatch(historyEl.innerHTML, /Abort/);
});
