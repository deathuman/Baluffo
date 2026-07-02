import test from "node:test";
import assert from "node:assert/strict";

import {
  buildTaskRunView
} from "../../../frontend/shared/task-run-view-model.js";

test("task run view model renders active fetch preparation progress", () => {
  const view = buildTaskRunView({
    taskType: "fetch",
    active: true,
    startedAt: "2026-03-08T10:00:00.000Z",
    heartbeatAt: "2026-03-08T10:01:30.000Z",
    taskProgress: {
      active: true,
      phaseKey: "selecting_sources",
      phaseLabel: "Selecting sources",
      mode: "indeterminate",
      counts: {
        seededOutputRows: 47388,
        selectedSourceCount: 333,
        setupElapsedMs: 90000
      }
    }
  }, { nowMs: Date.parse("2026-03-08T10:10:00.000Z") });

  assert.equal(view.status, "running");
  assert.match(view.progressLabel, /Selecting sources/i);
  assert.match(view.progressLabel, /seeded 47,388 jobs/i);
  assert.match(view.progressLabel, /selected 333 sources/i);
  assert.doesNotMatch(view.progressLabel, /0 sources resolved/i);
});
