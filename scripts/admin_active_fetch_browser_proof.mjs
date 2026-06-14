import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(SCRIPT_DIR, "..");
const DEFAULT_OUTPUT_ROOT = path.join(REPO_ROOT, "_out", "admin-active-fetch-browser-proof");
const SOURCE_TABLES_DELAYED_LABEL = "Source tables delayed while job update is running.";
const HEAVY_ROUTE_PATTERNS = [
  /\/registry\/sources(?:\?|$)/i,
  /\/registry\/conflicts(?:\?|$)/i,
  /\/admin\/ops-tab-counts(?:\?|$)/i,
  /\/ops\/dashboard-health(?:\?|$)/i
];

function slugify(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "scenario";
}

function timestampSlug() {
  return new Date().toISOString().replace(/[:.]/g, "").replace("T", "-").replace("Z", "Z");
}

function normalizeBaseUrl(value, fallback) {
  return String(value || fallback || "").replace(/\/+$/, "");
}

function bridgeUrl(bridgeBase, route) {
  return `${normalizeBaseUrl(bridgeBase, "http://127.0.0.1:8877")}${String(route || "").startsWith("/") ? "" : "/"}${route}`;
}

function adminUrl({ baseUrl, bridgeBase, scenario }) {
  const bridge = new URL(normalizeBaseUrl(bridgeBase, "http://127.0.0.1:8877"));
  const url = new URL("/admin.html", `${normalizeBaseUrl(baseUrl, "http://127.0.0.1:8080")}/`);
  url.searchParams.set("desktop", "1");
  url.searchParams.set("bridgePort", bridge.port || "8877");
  url.searchParams.set("bridgeHost", bridge.hostname || "127.0.0.1");
  url.searchParams.set("visual-check", `${Date.now()}-${slugify(scenario)}`);
  return url.toString();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function writeJson(filePath, payload) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

async function fetchBridgeJson(bridgeBase, route, options = {}) {
  assert.equal(typeof fetch, "function", "global fetch is required for bridge probes");
  const response = await fetch(bridgeUrl(bridgeBase, route), {
    method: options.method || "GET",
    headers: options.body ? { "content-type": "application/json" } : undefined,
    body: options.body ? JSON.stringify(options.body) : undefined
  });
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = {};
  }
  if (!response.ok && options.allowError !== true) {
    throw new Error(`${route} returned HTTP ${response.status}: ${String(payload?.error || response.statusText || "error")}`);
  }
  return { status: response.status, ok: response.ok, payload };
}

async function waitFor(label, callback, { timeoutMs = 30_000, intervalMs = 250 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let lastValue;
  let lastError;
  while (Date.now() < deadline) {
    try {
      lastValue = await callback();
      if (lastValue) return lastValue;
    } catch (error) {
      lastError = error;
    }
    await sleep(intervalMs);
  }
  const detail = lastError instanceof Error ? ` Last error: ${lastError.message}` : "";
  throw new Error(`${label} did not become true within ${timeoutMs}ms.${detail}`);
}

function taskRows(payload) {
  if (Array.isArray(payload?.tasks)) return payload.tasks;
  if (Array.isArray(payload?.current)) return payload.current;
  if (Array.isArray(payload?.rows)) return payload.rows;
  return [];
}

function activeFetchRow(payload) {
  return taskRows(payload).find(row => {
    const type = String(row?.taskType || row?.type || "").trim().toLowerCase();
    const status = String(row?.status || row?.lifecycleStatus || "").trim().toLowerCase();
    return type === "fetch"
      && row?.active !== false
      && !String(row?.finishedAt || "").trim()
      && !["ok", "success", "succeeded", "failed", "error", "canceled", "cancelled"].includes(status);
  });
}

async function ensureLocalFixtures(bridgeBase) {
  const signIn = await fetchBridgeJson(bridgeBase, "/desktop-local-data/sign-in", {
    method: "POST",
    body: { name: "Browser Proof User" }
  });
  const schedule = await fetchBridgeJson(bridgeBase, "/tasks/jobs-pipeline-schedule", {
    method: "POST",
    body: { enabled: false, intervalHours: 12 }
  });
  return { signIn: signIn.payload, schedule: schedule.payload };
}

async function startOrFindControlledFetch(bridgeBase) {
  const existing = await fetchBridgeJson(bridgeBase, "/ops/task-state?view=summary").catch(() => null);
  const existingFetch = activeFetchRow(existing?.payload || {});
  if (existingFetch) {
    return {
      runId: String(existingFetch.runId || existingFetch.id || ""),
      started: false,
      reused: true,
      taskState: existing?.payload || {}
    };
  }
  const started = await fetchBridgeJson(bridgeBase, "/tasks/run-jobs-bootstrap", {
    method: "POST",
    body: {
      source: "admin_active_fetch_browser_proof",
      forceBootstrap: true
    },
    allowError: true
  });
  if (!started.ok && !started.payload?.alreadyRunning) {
    throw new Error(`/tasks/run-jobs-bootstrap failed: HTTP ${started.status} ${String(started.payload?.error || "")}`);
  }
  const active = await waitFor("controlled fetch task-state row", async () => {
    const taskState = await fetchBridgeJson(bridgeBase, "/ops/task-state?view=summary");
    const row = activeFetchRow(taskState.payload);
    return row ? { row, taskState: taskState.payload } : null;
  }, { timeoutMs: 15_000, intervalMs: 250 });
  return {
    runId: String(started.payload?.runId || active.row?.runId || active.row?.id || ""),
    started: Boolean(started.payload?.started),
    reused: false,
    startPayload: started.payload,
    taskState: active.taskState
  };
}

async function extractAdminState(tab) {
  return tab.playwright.evaluate(() => {
    function text(id) {
      const node = document.getElementById(id);
      return String(node?.textContent || "").replace(/\s+/g, " ").trim();
    }
    function htmlLength(id) {
      return Number(document.getElementById(id)?.innerHTML?.length || 0);
    }
    function visible(id) {
      const node = document.getElementById(id);
      return Boolean(node && node.offsetParent !== null);
    }
    let resourceEntries = [];
    try {
      const perf = globalThis.performance;
      resourceEntries = typeof perf?.getEntriesByType === "function"
        ? perf.getEntriesByType("resource").slice(-200).map(entry => ({
          name: String(entry.name || ""),
          duration: Number(entry.duration || 0),
          transferSize: Number(entry.transferSize || 0),
          responseStatus: Number(entry.responseStatus || 0)
        }))
        : [];
    } catch {
      resourceEntries = [];
    }
    return {
      href: String(location.href || ""),
      title: String(document.title || ""),
      bridgeBadge: text("admin-bridge-status-badge"),
      sourceStatus: text("admin-source-status"),
      usersList: text("admin-users-list"),
      totals: text("admin-totals"),
      opsSchedule: text("admin-ops-schedule"),
      syncStatus: text("admin-sync-status"),
      syncConfigHint: text("admin-sync-config-hint"),
      pendingSources: text("admin-pending-sources"),
      activeSources: text("admin-active-sources"),
      rejectedSources: text("admin-rejected-sources"),
      currentRuns: text("admin-ops-history"),
      fetcherProgress: text("admin-fetcher-progress"),
      fetcherProgressLabel: text("admin-fetcher-progress-label"),
      fetcherLogTail: text("admin-fetcher-log").slice(-1000),
      panels: {
        usersVisible: visible("admin-users-list"),
        syncVisible: visible("admin-sync-status"),
        scheduleVisible: visible("admin-ops-schedule"),
        pendingSourcesHtmlLength: htmlLength("admin-pending-sources"),
        activeSourcesHtmlLength: htmlLength("admin-active-sources"),
        rejectedSourcesHtmlLength: htmlLength("admin-rejected-sources")
      },
      resources: resourceEntries
    };
  }, undefined, { timeoutMs: 5000 });
}

function assertHydratedAdminState(state, { scenario, requireSyncReady = false }) {
  assert.match(state.bridgeBadge, /Bridge (Online|Degraded)/i, `${scenario}: bridge badge should not be offline/checking`);
  assert.ok(state.usersList.length > 0 || /Loaded \d+ user account/i.test(state.sourceStatus), `${scenario}: Stored Profiles Overview should not be blank`);
  assert.doesNotMatch(state.usersList, /^Loading/i, `${scenario}: Stored Profiles Overview should not remain loading`);
  assert.ok(state.opsSchedule.length > 0, `${scenario}: Pipeline schedule panel should not be blank`);
  assert.doesNotMatch(state.opsSchedule, /Pipeline:\s*unknown/i, `${scenario}: Pipeline schedule should not be unknown`);
  assert.ok(state.syncStatus.length > 0, `${scenario}: Source Sync status should not be blank`);
  assert.match(state.syncStatus, /Ready|Disabled|Needs Attention|Remote Conflict|Rate Limited|Connected|Source sync/i, `${scenario}: Source Sync should render status/meta copy`);
  if (requireSyncReady) {
    assert.match(state.syncStatus, /Ready|Connected/i, `${scenario}: live Source Sync should be ready`);
  }
  for (const [label, textValue] of [
    ["pending", state.pendingSources],
    ["active", state.activeSources],
    ["rejected", state.rejectedSources]
  ]) {
    assert.match(textValue, new RegExp(SOURCE_TABLES_DELAYED_LABEL.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i"), `${scenario}: ${label} sources should show active-run delayed placeholder`);
  }
}

function heavyRouteEvidence(resources, logs) {
  const resourceMatches = resources
    .map(entry => String(entry?.name || ""))
    .filter(name => HEAVY_ROUTE_PATTERNS.some(pattern => pattern.test(name)));
  const logMatches = logs
    .map(entry => String(entry?.message || ""))
    .filter(message => HEAVY_ROUTE_PATTERNS.some(pattern => pattern.test(message)) && /504|timeout|timed out|Gateway Timeout/i.test(message));
  return { resourceMatches, logMatches };
}

async function runBrowserScenario({
  browser,
  baseUrl,
  bridgeBase,
  outputDir,
  scenario,
  expectBootstrapStatus = 0,
  requireSyncReady = false,
  keepTabOpen = false
}) {
  const tab = await browser.tabs.new();
  const scenarioSlug = slugify(scenario);
  const targetUrl = adminUrl({ baseUrl, bridgeBase, scenario });
  const screenshots = {};
  try {
    await tab.goto(targetUrl);
    await tab.playwright.waitForLoadState({ state: "domcontentloaded", timeoutMs: 30_000 });
    const settled = await waitFor(`${scenario} Browser-visible Admin panels`, async () => {
      const state = await extractAdminState(tab);
      const hasSourcesDelayed = [state.pendingSources, state.activeSources, state.rejectedSources]
        .every(textValue => textValue.includes(SOURCE_TABLES_DELAYED_LABEL));
      const hasProfile = state.usersList.length > 0 || /Loaded \d+ user account/i.test(state.sourceStatus);
      const hasSchedule = state.opsSchedule.length > 0 && !/Pipeline:\s*unknown/i.test(state.opsSchedule);
      const hasSync = state.syncStatus.length > 0;
      return hasSourcesDelayed && hasProfile && hasSchedule && hasSync ? state : null;
    }, { timeoutMs: 30_000, intervalMs: 500 });

    assertHydratedAdminState(settled, { scenario, requireSyncReady });
    const beforeProgress = await extractAdminState(tab);
    await sleep(2500);
    const afterProgress = await extractAdminState(tab);
    assert.match(afterProgress.currentRuns, /fetch|pipeline|Current Runs/i, `${scenario}: current runs should stay visible`);
    assert.ok(
      beforeProgress.currentRuns !== afterProgress.currentRuns
        || beforeProgress.fetcherLogTail !== afterProgress.fetcherLogTail
        || beforeProgress.fetcherProgress !== afterProgress.fetcherProgress
        || beforeProgress.fetcherProgressLabel !== afterProgress.fetcherProgressLabel,
      `${scenario}: current run or fetch log/progress should update without reload`
    );

    const screenshotPath = path.join(outputDir, `${scenarioSlug}.png`);
    await fs.writeFile(screenshotPath, await tab.screenshot({ fullPage: true }));
    screenshots.fullPage = screenshotPath;
    const logs = await tab.dev.logs({ levels: ["error", "warn"], limit: 200 }).catch(() => []);
    const finalState = await extractAdminState(tab);
    const heavyEvidence = heavyRouteEvidence(finalState.resources || [], logs || []);
    assert.equal(
      heavyEvidence.logMatches.length,
      0,
      `${scenario}: console should not spam heavy-route timeout errors: ${heavyEvidence.logMatches.join(" | ")}`
    );
    assert.equal(
      heavyEvidence.resourceMatches.length,
      0,
      `${scenario}: active Admin should not request heavy routes: ${heavyEvidence.resourceMatches.join(" | ")}`
    );

    const bootstrapResources = (finalState.resources || [])
      .filter(entry => String(entry?.name || "").includes("/admin/bootstrap"));
    if (expectBootstrapStatus > 0 && bootstrapResources.some(entry => entry.responseStatus > 0)) {
      assert.ok(
        bootstrapResources.some(entry => Number(entry.responseStatus) === Number(expectBootstrapStatus)),
        `${scenario}: expected /admin/bootstrap resource status ${expectBootstrapStatus}`
      );
    }

    return {
      ok: true,
      scenario,
      url: targetUrl,
      screenshots,
      initialState: settled,
      beforeProgress,
      afterProgress,
      finalState,
      consoleLogs: logs,
      heavyEvidence,
      bootstrapResources
    };
  } finally {
    if (!keepTabOpen) {
      await tab.close().catch(() => {});
    }
  }
}

export async function runAdminActiveFetchBrowserProof(options = {}) {
  const browser = options.browser || globalThis.browser;
  assert.ok(browser, "A Codex in-app Browser object is required");
  const baseUrl = normalizeBaseUrl(options.baseUrl, "http://127.0.0.1:8080");
  const bridgeBase = normalizeBaseUrl(options.bridgeBase, "http://127.0.0.1:8877");
  const runId = timestampSlug();
  const outputDir = path.resolve(options.outputDir || path.join(DEFAULT_OUTPUT_ROOT, runId));
  await fs.mkdir(outputDir, { recursive: true });
  if (options.makeVisible !== false) {
    await (await browser.capabilities.get("visibility")).set(true);
  }
  if (typeof browser.nameSession === "function") {
    await browser.nameSession("Baluffo Admin active-fetch proof");
  }

  const readiness = await waitFor("local bridge readiness", async () => {
    const ready = await fetchBridgeJson(bridgeBase, "/app/ready", { allowError: true });
    return ready.ok ? ready.payload : null;
  }, { timeoutMs: Number(options.bridgeReadyTimeoutMs || 30_000), intervalMs: 500 });
  const fixtures = options.seedFixtures === false ? null : await ensureLocalFixtures(bridgeBase);
  const activeFetch = options.startFetch === false
    ? null
    : await startOrFindControlledFetch(bridgeBase);

  const scenarios = [];
  if (options.expectBootstrapFailOnce) {
    scenarios.push(await runBrowserScenario({
      browser,
      baseUrl,
      bridgeBase,
      outputDir,
      scenario: "bootstrap-fail-once-fallback",
      expectBootstrapStatus: 504,
      requireSyncReady: Boolean(options.requireSyncReady),
      keepTabOpen: Boolean(options.keepTabOpen)
    }));
  }
  scenarios.push(await runBrowserScenario({
    browser,
    baseUrl,
    bridgeBase,
    outputDir,
    scenario: "active-fetch-lightweight-bootstrap",
    requireSyncReady: Boolean(options.requireSyncReady),
    keepTabOpen: Boolean(options.keepTabOpen)
  }));

  const report = {
    ok: true,
    startedAt: new Date().toISOString(),
    baseUrl,
    bridgeBase,
    readiness,
    fixtures,
    activeFetch,
    scenarios,
    artifacts: {
      outputDir
    }
  };
  const reportPath = path.join(outputDir, "admin-active-fetch-browser-proof.json");
  await writeJson(reportPath, report);
  return { ...report, reportPath };
}
