import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const compositionSource = readFileSync(
  new URL("../../../frontend/admin/app/runtime/composition.js", import.meta.url),
  "utf8"
);

test("admin startup has no automatic deferred diagnostics fan-out", () => {
  const match = compositionSource.match(/async function loadPostInteractiveDiagnostics\(\) \{([\s\S]*?)\n  \}/);
  assert.ok(match, "expected post-interactive diagnostics helper");
  const body = match[1];
  assert.match(body, /return null;/);
  assert.doesNotMatch(body, /getBridge\(/);
  assert.doesNotMatch(body, /loadOpsOverviewDetailData/);
  assert.doesNotMatch(body, /loadLatestFetcherReport/);
  assert.doesNotMatch(body, /loadOpsHistoryData/);
  assert.doesNotMatch(body, /loadFetcherLogChunk/);
  assert.doesNotMatch(body, /loadDiscoveryLogChunk/);
  assert.doesNotMatch(body, /sourceTablesOnly:\s*true/);
  assert.doesNotMatch(body, /\/discovery\/report\?view=summary/);
});

test("admin bootstrap schedules source table loading without report diagnostics", () => {
  const schedulerMatch = compositionSource.match(/function scheduleBootstrapSourceTablesLoad\(\) \{([\s\S]*?)\n  \}/);
  assert.ok(schedulerMatch, "expected bootstrap source table scheduler");
  const schedulerBody = schedulerMatch[1];
  assert.match(schedulerBody, /sourceTablesOnly:\s*true/);
  assert.match(schedulerBody, /logChanges:\s*false/);
  assert.doesNotMatch(schedulerBody, /loadLatestFetcherReport/);
  assert.doesNotMatch(schedulerBody, /loadDiscoveryLogChunk/);
  assert.doesNotMatch(schedulerBody, /getBridge\(/);

  const bootstrapMatch = compositionSource.match(/async function loadAdminBootstrap\(\) \{([\s\S]*?)\n  \}/);
  assert.ok(bootstrapMatch, "expected bootstrap loader");
  assert.match(bootstrapMatch[1], /scheduleBootstrapSourceTablesLoad\(\)/);
});
