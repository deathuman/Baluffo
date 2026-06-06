import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const compositionSource = readFileSync(
  new URL("../../../frontend/admin/app/runtime/composition.js", import.meta.url),
  "utf8"
);

function extractPostInteractiveTasks(source) {
  const match = source.match(/const tasks = \[([\s\S]*?)\];/);
  assert.ok(match, "expected post-interactive task list");
  return match[1];
}

test("admin startup deferred queue avoids full diagnostics fan-out", () => {
  const tasks = extractPostInteractiveTasks(compositionSource);
  assert.doesNotMatch(tasks, /loadOpsOverviewDetailData/);
  assert.doesNotMatch(tasks, /loadLatestFetcherReport/);
  assert.doesNotMatch(tasks, /loadOpsHistoryData/);
  assert.doesNotMatch(tasks, /loadFetcherLogChunk/);
  assert.doesNotMatch(tasks, /loadDiscoveryLogChunk/);
  assert.doesNotMatch(tasks, /sourceTablesOnly:\s*true/);
  assert.match(tasks, /refreshDiscoveryTabBadge/);
});
