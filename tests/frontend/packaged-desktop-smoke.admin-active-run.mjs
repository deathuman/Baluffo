import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium, request as playwrightRequest } from "@playwright/test";
import {
  fetchBridgeJson,
  isActiveTaskRow,
  postBridgeJson,
  waitUntil
} from "./helpers/packaged-first-run-smoke-helpers.mjs";
import {
  buildDesktopUrl,
  buildWriteReport,
  BASE_URL,
  BRIDGE_BASE,
  BRIDGE_PORT,
  BRIDGE_HOST
} from "./helpers/packaged-smoke-shared.mjs";

const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/admin-active-run-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/admin-active-run-output");
const ABORT_REASON = "packaged_admin_active_run_smoke_abort";
const HEADED = process.env.PACKAGED_SMOKE_HEADED === "1";
const writeReport = buildWriteReport(REPORT_PATH);
const desktopUrl = buildDesktopUrl;

function bridgeRouteKey(rawUrl) {
  const url = new URL(rawUrl);
  const params = [...url.searchParams.entries()]
    .filter(([key]) => key !== "t")
    .sort(([left], [right]) => left.localeCompare(right));
  const query = new URLSearchParams(params).toString();
  return `${url.pathname}${query ? `?${query}` : ""}`;
}

function isForbiddenActiveRoute(rawUrl) {
  const url = new URL(rawUrl);
  if (url.pathname === "/ops/storage-health") return true;
  if (url.pathname === "/ops/fetch-report") {
    const view = url.searchParams.get("view");
    return view !== "summary" && view !== "live";
  }
  if (url.pathname === "/registry/summary" || url.pathname === "/registry/sources") return true;
  if (url.pathname === "/discovery/report" || url.pathname === "/discovery/candidates") return true;
  return false;
}

function assertNoHighRateRouteStorm(requests, routeKey, label) {
  const rows = requests
    .filter(row => row.routeKey === routeKey)
    .sort((left, right) => left.atMs - right.atMs);
  for (let index = 0; index < rows.length; index += 1) {
    const windowRows = rows.filter(row => row.atMs >= rows[index].atMs && row.atMs < rows[index].atMs + 1000);
    assert.ok(
      windowRows.length <= 2,
      `${label} should not issue more than two requests in one second; saw ${windowRows.length}`
    );
  }
  assert.ok(rows.length <= 12, `${label} should remain bounded during the packaged active-run smoke`);
}

function rowRunId(row) {
  return String(row?.runId || row?.id || "").trim();
}

function rowTaskType(row) {
  return String(row?.taskType || row?.type || "").trim().toLowerCase();
}

async function waitForDesktopAdapter(page) {
  await page.waitForFunction(() => Boolean(window.JobAppLocalData), null, { timeout: 30_000 });
}

async function waitForBridgeTasksIdle(apiRequest) {
  return waitUntil("bridge lifecycle idle", async () => {
    const payload = await fetchBridgeJson(
      apiRequest,
      BRIDGE_BASE,
      "/ops/task-state?view=summary",
      "task state summary"
    );
    const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
    return tasks.filter(isActiveTaskRow).length === 0 ? true : null;
  }, 60_000, 500);
}

async function waitForActiveRun(apiRequest, taskType, runId) {
  return waitUntil("active task lifecycle row", async () => {
    const payload = await fetchBridgeJson(
      apiRequest,
      BRIDGE_BASE,
      "/ops/task-state?view=summary",
      "task state summary"
    );
    const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
    return tasks.find(row => rowRunId(row) === runId && rowTaskType(row) === taskType && isActiveTaskRow(row)) || null;
  }, 15_000, 500);
}

async function waitForRunGoneFromCurrentTasks(apiRequest, runId) {
  return waitUntil("task leaves current task state", async () => {
    const payload = await fetchBridgeJson(
      apiRequest,
      BRIDGE_BASE,
      "/ops/task-state?view=summary",
      "task state summary"
    );
    const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
    return tasks.some(row => rowRunId(row) === runId && isActiveTaskRow(row)) ? null : true;
  }, 45_000, 500);
}

async function seedScheduleConfig(apiRequest) {
  const payload = await postBridgeJson(
    apiRequest,
    BRIDGE_BASE,
    "/tasks/jobs-pipeline-schedule",
    { enabled: false, intervalHours: 24 },
    "seed jobs pipeline schedule"
  );
  assert.equal(Number(payload?.savedConfig?.intervalHours), 24, "schedule seed should persist an interval");
  return payload;
}

async function startDeterministicFetch(apiRequest) {
  const started = await postBridgeJson(
    apiRequest,
    BRIDGE_BASE,
    "/tasks/run-jobs-bootstrap",
    { source: "jobs_first_run" },
    "jobs bootstrap start"
  );
  assert.equal(Boolean(started?.started), true, "bootstrap should start");
  assert.equal(String(started?.taskType || ""), "fetch", "bootstrap task type should be fetch");
  assert.equal(
    String(started?.smokeMode || ""),
    "controlled-heartbeat-success",
    "bootstrap smoke mode should keep deterministic active work"
  );
  assert.match(String(started?.runId || ""), /^jobs_bootstrap_[a-f0-9]{10}$/i);
  return started;
}

async function waitForAdminActiveHydration(page) {
  return page.waitForFunction(() => {
    const kpis = document.querySelector('[data-ui="admin-ops-kpis"]')?.textContent || "";
    const schedule = document.querySelector('[data-ui="admin-ops-schedule"]')?.textContent || "";
    const hasKpis = /OPS STATUS|LAST SUCCESSFUL FETCH|PENDING SOURCES/i.test(kpis);
    const kpisNotBlanketDelayed = !/Updating while job is running\./i.test(kpis);
    const pendingSourcesNotTableBlocked = !/Pending Sources\s*Not loaded yet/i.test(kpis.replace(/\s+/g, " "));
    const scheduleReady = /Pipeline:/i.test(schedule) && !/loading schedule/i.test(schedule);
    const hasScheduleControl = Boolean(document.querySelector('[data-ui="admin-pipeline-schedule-interval"]'));
    return hasKpis && kpisNotBlanketDelayed && pendingSourcesNotTableBlocked && scheduleReady && hasScheduleControl;
  }, null, { timeout: 15_000 });
}

async function collectAdminState(page) {
  return page.evaluate(() => ({
    kpis: document.querySelector('[data-ui="admin-ops-kpis"]')?.textContent || "",
    schedule: document.querySelector('[data-ui="admin-ops-schedule"]')?.textContent || "",
    bridgeBadge: document.querySelector("#admin-bridge-status-badge")?.textContent || ""
  }));
}

async function abortRun(apiRequest, runId) {
  const payload = await postBridgeJson(
    apiRequest,
    BRIDGE_BASE,
    "/tasks/abort",
    { taskType: "fetch", runId, reason: ABORT_REASON },
    "task abort"
  );
  assert.equal(Boolean(payload?.abortAccepted), true, "abort should be accepted");
  assert.equal(String(payload?.runId || ""), runId, "abort response should target the active run");
  return payload;
}

async function main() {
  const capturedBridgeRequests = [];
  const scenarios = [];
  const errors = [];
  const pageErrors = [];
  const consoleErrors = [];
  let browser;
  let context;
  let page;
  let apiRequest;
  let activeStartedAt = 0;
  let abortStartedAt = 0;
  let abortFinishedAt = 0;
  try {
    await fs.mkdir(OUTPUT_DIR, { recursive: true });
    browser = await chromium.launch({
      headless: !HEADED,
      ...(process.env.PACKAGED_SMOKE_SYSTEM_CHROMIUM === "1" ? { channel: "chromium" } : {})
    });
    context = await browser.newContext({ baseURL: BASE_URL });
    page = await context.newPage();
    apiRequest = await playwrightRequest.newContext({ baseURL: BRIDGE_BASE });
    page.on("pageerror", error => pageErrors.push(String(error?.message || error)));
    page.on("console", message => {
      if (message.type() === "error") consoleErrors.push(message.text());
    });
    page.on("request", request => {
      const rawUrl = request.url();
      if (rawUrl.startsWith(BRIDGE_BASE)) {
        capturedBridgeRequests.push({
          atMs: Date.now(),
          method: request.method(),
          routeKey: bridgeRouteKey(rawUrl),
          url: rawUrl
        });
      }
    });

    await waitForBridgeTasksIdle(apiRequest);
    await seedScheduleConfig(apiRequest);
    await page.goto(desktopUrl("admin.html"), { waitUntil: "domcontentloaded" });
    await waitForDesktopAdapter(page);
    await page.locator("#admin-content").waitFor({ state: "visible", timeout: 30_000 });
    await waitForAdminActiveHydration(page);

    const started = await startDeterministicFetch(apiRequest);
    const runId = String(started.runId);
    await waitForActiveRun(apiRequest, "fetch", runId);
    activeStartedAt = Date.now();
    await waitForAdminActiveHydration(page);
    const activeAdminState = await collectAdminState(page);
    assert.doesNotMatch(activeAdminState.kpis, /Updating while job is running\./i);
    assert.doesNotMatch(activeAdminState.schedule, /loading schedule/i);
    assert.doesNotMatch(activeAdminState.kpis.replace(/\s+/g, " "), /Pending Sources\s*Not loaded yet/i);

    abortStartedAt = Date.now();
    const abortPayload = await abortRun(apiRequest, runId);
    await waitForRunGoneFromCurrentTasks(apiRequest, runId);
    abortFinishedAt = Date.now();
    await page.waitForTimeout(2_000);

    const activeOnlyRequests = capturedBridgeRequests.filter(
      row => row.atMs >= activeStartedAt && row.atMs <= abortStartedAt
    );
    const activeAndAbortRequests = capturedBridgeRequests.filter(
      row => row.atMs >= activeStartedAt && row.atMs <= abortFinishedAt + 2_000
    );
    const forbiddenRoutes = activeOnlyRequests.filter(row => isForbiddenActiveRoute(row.url));
    assert.deepEqual(
      forbiddenRoutes.map(row => row.routeKey),
      [],
      "Admin active-run smoke should not call forbidden heavy routes"
    );
    assertNoHighRateRouteStorm(
      activeAndAbortRequests,
      "/tasks/run-jobs-pipeline-status",
      "pipeline status polling"
    );
    assertNoHighRateRouteStorm(
      activeAndAbortRequests,
      "/ops/task-state?view=summary",
      "task-state polling"
    );
    assert.equal(pageErrors.length, 0, `unexpected Admin page errors: ${pageErrors.join("; ")}`);
    assert.equal(consoleErrors.length, 0, `unexpected Admin console errors: ${consoleErrors.join("; ")}`);

    scenarios.push({
      name: "Packaged Admin hydrates desktop active run and aborts without request storm",
      slug: "packaged-admin-active-run",
      status: "passed",
      durationMs: abortFinishedAt - activeStartedAt,
      runId,
      abortState: String(abortPayload?.state || ""),
      activeAdminState,
      activeWindowRequestCount: activeOnlyRequests.length,
      activeAndAbortRequestCount: activeAndAbortRequests.length
    });
  } catch (error) {
    let failureAdminState = null;
    if (page && !page.isClosed()) {
      failureAdminState = await collectAdminState(page).catch(() => null);
    }
    errors.push(String(error?.stack || error?.message || error));
    scenarios.push({
      name: "Packaged Admin hydrates desktop active run and aborts without request storm",
      slug: "packaged-admin-active-run",
      status: "failed",
      durationMs: 0,
      error: String(error?.message || error),
      failureAdminState
    });
  } finally {
    if (apiRequest) {
      await postBridgeJson(
        apiRequest,
        BRIDGE_BASE,
        "/tasks/jobs-pipeline-schedule",
        { enabled: false, intervalHours: 24 },
        "schedule cleanup"
      ).catch(() => {});
      await apiRequest.dispose().catch(() => {});
    }
    await context?.close().catch(() => {});
    await browser?.close().catch(() => {});
  }

  const report = {
    ok: errors.length === 0 && scenarios.every(scenario => scenario.status === "passed"),
    scenarios,
    capturedBridgeRequests,
    errors,
    artifacts: { outputDir: OUTPUT_DIR }
  };
  await writeReport(report);
  if (!report.ok) {
    console.error("Admin active-run packaged smoke failed:", report.errors);
    process.exitCode = 1;
  }
}

await main();
