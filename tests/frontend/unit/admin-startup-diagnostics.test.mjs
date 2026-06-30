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
const opsHealthSource = readFileSync(
  new URL("../../../frontend/admin/app/ops/health.js", import.meta.url),
  "utf8"
);
const authSource = readFileSync(
  new URL("../../../frontend/admin/app/auth.js", import.meta.url),
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
  assert.match(schedulerBody, /markSourceTablesLoadingForBootstrap/);
  assert.match(schedulerBody, /sourceTablesOnly:\s*true/);
  assert.match(schedulerBody, /logChanges:\s*false/);
  assert.match(schedulerBody, /suppressRegistryRetry:\s*true/);
  assert.match(schedulerBody, /enqueueAdminStartupBridgeTask/);
  assert.doesNotMatch(schedulerBody, /setTimeout/);
  assert.doesNotMatch(schedulerBody, /60000/);
  assert.doesNotMatch(schedulerBody, /loadLatestFetcherReport/);
  assert.doesNotMatch(schedulerBody, /loadDiscoveryLogChunk/);
  assert.doesNotMatch(schedulerBody, /getBridge\(/);

  const bootstrapMatch = compositionSource.match(/async function loadAdminBootstrap\(\) \{([\s\S]*?)\n  \}/);
  assert.ok(bootstrapMatch, "expected bootstrap loader");
  assert.match(bootstrapMatch[1], /scheduleBootstrapSourceTablesLoad\(\)/);
  assert.match(bootstrapMatch[1], /scheduleBootstrapOpsFallbackHydration\(\{ bootstrapScheduleNeedsRefresh \}\)/);
  assert.ok(
    bootstrapMatch[1].indexOf("scheduleBootstrapSourceTablesLoad()")
      < bootstrapMatch[1].indexOf("scheduleBootstrapOpsFallbackHydration"),
    "source table load must be queued before fallback Ops hydration"
  );
  assert.doesNotMatch(bootstrapMatch[1], /await opsController\.loadPipelineScheduleData/);
  assert.doesNotMatch(bootstrapMatch[1], /await opsController\.loadOpsHistoryData/);
});

test("admin degraded bootstrap refreshes overview instead of rendering false empty", () => {
  const bootstrapMatch = compositionSource.match(/async function loadAdminBootstrap\(\) \{([\s\S]*?)\n  \}/);
  assert.ok(bootstrapMatch, "expected bootstrap loader");
  const body = bootstrapMatch[1];
  assert.match(body, /bootstrapDegraded/);
  assert.match(body, /bootstrapScheduleNeedsRefresh/);
  assert.match(body, /renderOverview\(payload\?\.overview \|\| \{\}, \{ degraded: bootstrapDegraded \}\)/);
  assert.match(body, /refreshOverview\(\{\s*detail: "summary",\s*scheduleFullRefresh: true,\s*timeoutMs: 5000,\s*background: true/s);
  assert.match(body, /scheduleBootstrapSourceTablesLoad\(\)/);
  assert.match(body, /scheduleBootstrapOpsFallbackHydration\(\{ bootstrapScheduleNeedsRefresh \}\)/);
});

test("admin auth leaves first fallback schedule and history ownership to bootstrap", () => {
  const initMatch = authSource.match(/function initAdminPage\(\) \{([\s\S]*?)\n  \}/);
  assert.ok(initMatch, "expected auth init");
  const body = initMatch[1];
  assert.match(body, /loadPipelineStatusFallbackData/);
  assert.match(body, /loadAdminBootstrap/);
  assert.doesNotMatch(body, /loadPipelineScheduleData\(\{ force: true, silent: true \}\)/);
  assert.doesNotMatch(body, /loadOpsHistoryData\(\{ force: true, silent: true \}\)/);
});

test("admin startup heavy hydration is sequential and defers while source load is busy", () => {
  const deferMatch = opsHealthSource.match(/function shouldDeferIdleOpsHeavyHydration\(options = \{\}\) \{([\s\S]*?)\n  \}/);
  assert.ok(deferMatch, "expected idle hydration deferral helper");
  assert.match(deferMatch[1], /adminStartupBridgeHydrationInFlight/);
  assert.match(deferMatch[1], /adminBusyState\?\.discoveryLoad/);
  assert.match(deferMatch[1], /allowStartupBridgeLane/);

  const loaderMatch = opsHealthSource.match(/async function loadIdleOpsHeavyHydration\(renderToken = opsRenderToken, options = \{\}\) \{([\s\S]*?)\n  \}/);
  assert.ok(loaderMatch, "expected idle hydration loader");
  const body = loaderMatch[1];
  assert.doesNotMatch(body, /Promise\.allSettled/);
  assert.ok(
    body.indexOf("await loadRegistryConflictsSummaryData") < body.indexOf("await loadFetchKpisSummaryData"),
    "registry conflicts summary should run before fetch KPIs"
  );
  assert.ok(
    body.indexOf("await loadFetchKpisSummaryData") < body.indexOf("await loadOpsTabCountsSummaryData"),
    "fetch KPIs should run before tab counts"
  );
  assert.match(body, /shouldDeferIdleOpsHeavyHydration\(options\)/);
  assert.match(compositionSource, /allowStartupBridgeLane:\s*true/);
  assert.match(opsHealthSource, /deferIdleHydration/);
});

test("admin critical bootstrap fallback hydrates summaries before delayed source tables", () => {
  const match = compositionSource.match(/async function loadCriticalBootstrapFallbacks\(\) \{([\s\S]*?)\n  \}/);
  assert.ok(match, "expected critical bootstrap fallback helper");
  const body = match[1];
  assert.match(body, /loadActiveOpsSummaryData/);
  assert.match(body, /returnMeta:\s*true/);
  assert.match(body, /markSourceTablesDelayedForActiveWork/);
  assert.match(body, /overviewController\.refreshOverview\(\{/);
  assert.match(body, /syncController\.loadSyncStatus\(\{/);
  assert.match(body, /opsController\.loadPipelineScheduleData\(\{/);
  assert.match(body, /opsController\.loadOpsHistoryData\(\{/);
  assert.doesNotMatch(body, /registryController\.loadDiscoveryData/);

  const schedulerMatch = compositionSource.match(/function scheduleBootstrapSourceTablesLoad\(\) \{([\s\S]*?)\n  \}/);
  assert.ok(schedulerMatch, "expected bootstrap source table scheduler");
  const schedulerBody = schedulerMatch[1];
  assert.match(schedulerBody, /markSourceTablesLoadingForBootstrap/);
  assert.match(schedulerBody, /sourceTablesOnly:\s*true/);
  assert.match(schedulerBody, /skipIfFreshMs:\s*10000/);
  assert.match(schedulerBody, /suppressRegistryRetry:\s*true/);
  assert.doesNotMatch(schedulerBody, /60000/);
});

test("admin ops controller forwards compact active summary loader to composition", () => {
  assert.match(opsSource, /loadActiveOpsSummaryData:\s*healthController\.loadActiveOpsSummaryData/);
});

test("admin active-fetch browser proof treats full fetch report as heavy", () => {
  assert.match(activeFetchProofSource, /\/ops\\\/fetch-report/);
  assert.match(activeFetchProofSource, /view=\(\?:summary\|live\)/);
});
