import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium, request as playwrightRequest } from "@playwright/test";
import {
  buildGotoDesktop,
  buildWriteReport,
  runScenario,
  BASE_URL,
  BRIDGE_BASE,
  BRIDGE_PORT,
  BRIDGE_HOST
} from "./helpers/packaged-smoke-shared.mjs";

const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/admin-startup-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/admin-startup-output");
const HEADED = process.env.PACKAGED_SMOKE_HEADED === "1";
const PAUSE_ON_FAILURE = process.env.PACKAGED_SMOKE_PAUSE_ON_FAILURE === "1";
const writeReport = buildWriteReport(REPORT_PATH);
const gotoDesktop = buildGotoDesktop();

async function waitForDesktopAdapter(page) {
  await page.waitForFunction(() => Boolean(window.JobAppLocalData), null, { timeout: 30_000 });
}

async function fetchStartupMetricRows(apiRequest, limit = 600) {
  const response = await apiRequest.get(`${BRIDGE_BASE}/desktop-local-data/startup-metrics?limit=${Number(limit) || 600}`);
  assert.equal(response.ok(), true, "startup metrics request should succeed");
  const payload = await response.json();
  return Array.isArray(payload?.rows) ? payload.rows : [];
}

async function waitForStartupMetric(apiRequest, eventName, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  let rows = [];
  let lastError = "";
  while (Date.now() < deadline) {
    try {
      rows = await fetchStartupMetricRows(apiRequest);
      lastError = "";
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    if (rows.some(row => String(row?.event || "") === eventName)) return rows;
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  assert.fail(`${eventName} startup metric was not recorded${lastError ? `; last error: ${lastError}` : ""}`);
}

function isFullStartupOpsRequest(url) {
  if (url.pathname === "/ops/health") return url.searchParams.get("view") !== "ready";
  if (url.pathname === "/ops/dashboard-health") return url.searchParams.get("view") !== "summary";
  if (url.pathname === "/ops/task-state") return url.searchParams.get("view") !== "summary";
  if (url.pathname === "/sync/status") return url.searchParams.get("view") !== "summary";
  if (url.pathname === "/registry/conflicts") return url.searchParams.get("view") !== "summary";
  return url.pathname === "/discovery/report";
}

async function waitForCapturedBridgeRequest(capturedBridgeRequests, predicate, label, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const parsedRequests = capturedBridgeRequests.map(rawUrl => new URL(rawUrl));
    if (parsedRequests.some(predicate)) return parsedRequests;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  assert.equal(false, true, label);
  return [];
}

async function main() {
  const scenarios = [];
  const errors = [];
  const capturedBridgeRequests = [];
  let browser;
  let context;
  let page;
  let apiRequest;
  try {
    await fs.mkdir(OUTPUT_DIR, { recursive: true });
    browser = await chromium.launch({ headless: !HEADED, ...(process.env.PACKAGED_SMOKE_SYSTEM_CHROMIUM === "1" ? { channel: "chromium" } : {}) });
    context = await browser.newContext({ baseURL: BASE_URL });
    page = await context.newPage();
    apiRequest = await playwrightRequest.newContext({ baseURL: BRIDGE_BASE });
    page.on("request", request => {
      const rawUrl = request.url();
      if (rawUrl.startsWith(BRIDGE_BASE)) {
        capturedBridgeRequests.push(rawUrl);
      }
    });

    await runScenario("Admin startup settles without full ops payloads", async () => {
      await gotoDesktop(page, "admin.html");
      await waitForDesktopAdapter(page);
      await page.locator("#admin-content").waitFor({ state: "visible", timeout: 30_000 });
      await page.waitForFunction(
        () => !/Loading operations health/i.test(document.querySelector("#admin-ops-trends")?.textContent || ""),
        null,
        { timeout: 2_500 }
      );
      await page.waitForFunction(
        () => /Loaded \d+ user account|failed|could not|error/i.test(document.querySelector("#admin-source-status")?.textContent || ""),
        null,
        { timeout: 30_000 }
      );
      await page.waitForFunction(
        () => /Bridge Online/i.test(document.querySelector("#admin-bridge-status-badge")?.textContent || ""),
        null,
        { timeout: 30_000 }
      );
      const sourceStatus = await page.locator("#admin-source-status").textContent();
      assert.match(String(sourceStatus || ""), /Loaded \d+ user account/i);
      const bridgeBadgeText = await page.locator("#admin-bridge-status-badge").textContent();
      assert.match(String(bridgeBadgeText || ""), /Bridge Online/i);
      assert.doesNotMatch(String(bridgeBadgeText || ""), /Bridge Checking/i);
      const opsTrendsText = await page.locator("#admin-ops-trends").textContent();
      assert.doesNotMatch(String(opsTrendsText || ""), /Loading operations health/i);

      const parsedRequests = await waitForCapturedBridgeRequest(
        capturedBridgeRequests,
        url => url.pathname === "/admin/bootstrap",
        "Admin startup should request the bounded bootstrap payload"
      );
      const fullStartupRequests = parsedRequests.filter(isFullStartupOpsRequest);
      assert.deepEqual(
        fullStartupRequests.map(url => `${url.pathname}${url.search}`),
        [],
        "Admin startup should not request full health/task/sync/discovery diagnostics"
      );
    }, scenarios);

    await runScenario("Admin packaged bridge reports desktop mode and first interactive", async () => {
      const health = await apiRequest.get(`${BRIDGE_BASE}/ops/health`);
      assert.equal(health.ok(), true, "ops health should be reachable");
      const healthPayload = await health.json();
      assert.equal(Boolean(healthPayload?.desktopMode), true, "packaged bridge should report desktopMode true");
      await waitForStartupMetric(apiRequest, "admin_first_interactive");
    }, scenarios);
  } catch (error) {
    errors.push(error instanceof Error ? error.message : String(error));
  } finally {
    if (PAUSE_ON_FAILURE && errors.length > 0 && page) {
      await page.pause();
    }
    await apiRequest?.dispose().catch(() => {});
    await context?.close().catch(() => {});
    await browser?.close().catch(() => {});
  }

  const report = {
    ok: errors.length === 0 && scenarios.every(scenario => scenario.status === "passed"),
    scenarios,
    capturedBridgeRequests,
    errors
  };
  await writeReport(report);
  if (!report.ok) {
    console.error("Admin startup smoke failed:", report.errors);
    process.exitCode = 1;
    return;
  }
}

main().catch(async error => {
  await writeReport({
    ok: false,
    scenarios: [],
    capturedBridgeRequests: [],
    errors: [error instanceof Error ? error.message : String(error)]
  });
  console.error(error);
  process.exitCode = 1;
});
