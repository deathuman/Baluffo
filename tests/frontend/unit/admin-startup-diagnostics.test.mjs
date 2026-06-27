import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const compositionSource = readFileSync(
  new URL("../../../frontend/admin/app/runtime/composition.js", import.meta.url),
  "utf8"
);
const opsSource = readFileSync(
  new URL("../../../frontend/admin/app/ops.js", import.meta.url),
  "utf8"
);
const activeFetchProofSource = readFileSync(
  new URL("../../../scripts/admin_active_fetch_browser_proof.mjs", import.meta.url),
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

test("admin degraded bootstrap refreshes overview instead of rendering false empty", () => {
  const bootstrapMatch = compositionSource.match(/async function loadAdminBootstrap\(\) \{([\s\S]*?)\n  \}/);
  assert.ok(bootstrapMatch, "expected bootstrap loader");
  const body = bootstrapMatch[1];
  assert.match(body, /bootstrapDegraded/);
  assert.match(body, /renderOverview\(payload\?\.overview \|\| \{\}, \{ degraded: bootstrapDegraded \}\)/);
  assert.match(body, /refreshOverview\(\{\s*detail: "summary",\s*scheduleFullRefresh: true,\s*timeoutMs: 5000,\s*background: true/s);
  assert.match(body, /loadPipelineScheduleData\(\{\s*silent: true,\s*force: true/s);
});

test("admin critical bootstrap fallback gates source tables behind compact active summary", () => {
  const match = compositionSource.match(/async function loadCriticalBootstrapFallbacks\(\) \{([\s\S]*?)\n  \}/);
  assert.ok(match, "expected critical bootstrap fallback helper");
  const body = match[1];
  assert.match(body, /loadActiveOpsSummaryData/);
  assert.match(body, /returnMeta:\s*true/);
  assert.match(body, /markSourceTablesDelayedForActiveWork/);
  assert.match(body, /activeAdminWork\s*\?\s*Promise\.resolve/);
  assert.match(body, /sourceTablesOnly:\s*true/);
});

test("admin ops controller forwards compact active summary loader to composition", () => {
  assert.match(opsSource, /loadActiveOpsSummaryData:\s*healthController\.loadActiveOpsSummaryData/);
});

test("admin active-fetch browser proof treats full fetch report as heavy", () => {
  assert.match(activeFetchProofSource, /\/ops\\\/fetch-report/);
  assert.match(activeFetchProofSource, /view=\(\?:summary\|live\)/);
});
