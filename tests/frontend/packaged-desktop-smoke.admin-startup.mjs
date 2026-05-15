import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium, request as playwrightRequest } from "@playwright/test";

const BASE_URL = process.env.PACKAGED_DESKTOP_BASE_URL || "http://127.0.0.1:8080";
const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";
const bridgeUrl = new URL(BRIDGE_BASE);
const BRIDGE_PORT = bridgeUrl.port || "8877";
const BRIDGE_HOST = bridgeUrl.hostname || "127.0.0.1";
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

function slugifyToken(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "scenario";
}

function createScenario(name) {
  return {
    name,
    slug: slugifyToken(name),
    status: "passed",
    durationMs: 0,
    error: ""
  };
}

async function writeReport(report) {
  await fs.mkdir(path.dirname(REPORT_PATH), { recursive: true });
  await fs.writeFile(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`, "utf8");
}

async function gotoDesktop(page, relativePath) {
  const separator = relativePath.includes("?") ? "&" : "?";
  await page.goto(
    `${BASE_URL}/${relativePath}${separator}desktop=1&bridgePort=${encodeURIComponent(BRIDGE_PORT)}&bridgeHost=${encodeURIComponent(BRIDGE_HOST)}`
  );
}

async function runScenario(name, callback, scenarios) {
  const startedAt = Date.now();
  const scenario = createScenario(name);
  try {
    await callback();
  } catch (error) {
    scenario.status = "failed";
    scenario.error = error instanceof Error ? error.message : String(error);
    throw error;
  } finally {
    scenario.durationMs = Date.now() - startedAt;
    scenarios.push(scenario);
  }
}

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
  while (Date.now() < deadline) {
    rows = await fetchStartupMetricRows(apiRequest);
    if (rows.some(row => String(row?.event || "") === eventName)) return rows;
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  assert.fail(`${eventName} startup metric was not recorded`);
}

function isSummaryRequest(url, pathname) {
  return url.pathname === pathname && url.searchParams.get("view") === "summary";
}

function isFullStartupOpsRequest(url) {
  const isFullTaskState = url.pathname === "/ops/task-state" && url.searchParams.get("view") !== "summary";
  const isFullRegistryConflicts = url.pathname === "/registry/conflicts" && url.searchParams.get("view") !== "summary";
  return isFullTaskState || isFullRegistryConflicts;
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
    browser = await chromium.launch({ headless: !HEADED });
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
        () => !/Loading admin overview/i.test(document.querySelector("#admin-source-status")?.textContent || ""),
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

      const parsedRequests = capturedBridgeRequests.map(rawUrl => new URL(rawUrl));
      assert.equal(
        parsedRequests.some(url => isSummaryRequest(url, "/ops/task-state")),
        true,
        "Admin startup should request summary task state"
      );
      assert.equal(
        parsedRequests.some(url => isSummaryRequest(url, "/registry/conflicts")),
        true,
        "Admin startup should request summary registry conflicts"
      );
      const fullStartupRequests = parsedRequests.filter(isFullStartupOpsRequest);
      assert.deepEqual(
        fullStartupRequests.map(url => `${url.pathname}${url.search}`),
        [],
        "Admin startup should not request full task-state or registry-conflicts payloads"
      );
    }, scenarios);

    await runScenario("Admin packaged bridge reports desktop mode and first interactive", async () => {
      const health = await apiRequest.get(`${BRIDGE_BASE}/ops/health`);
      assert.equal(health.ok(), true, "ops health should be reachable");
      const healthPayload = await health.json();
      assert.equal(Boolean(healthPayload?.desktopMode), true, "packaged bridge should report desktopMode true");
      await waitForStartupMetric(apiRequest, "admin_first_interactive");
      await waitForStartupMetric(apiRequest, "admin_ops_health_first_render");
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
  }
  assert.equal(report.ok, true, "packaged Admin startup smoke should pass");
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
