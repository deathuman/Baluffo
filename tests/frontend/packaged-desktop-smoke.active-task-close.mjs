import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium, request as playwrightRequest } from "@playwright/test";
import {
  fetchBridgeJson,
  isActiveTaskRow,
  waitUntil
} from "./helpers/packaged-first-run-smoke-helpers.mjs";

const BASE_URL = process.env.PACKAGED_DESKTOP_BASE_URL || "http://127.0.0.1:8080";
const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";
const CDP_PORT = Number(process.env.BALUFFO_PACKAGED_SMOKE_CDP_PORT || 0);
const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/active-task-close-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/active-task-close-output");
const bridgeUrl = new URL(BRIDGE_BASE);
const BRIDGE_PORT = bridgeUrl.port || "8877";
const BRIDGE_HOST = bridgeUrl.hostname || "127.0.0.1";
const FIRST_RUN_SMOKE_QUERY =
  "jobsColdStart=1&jobsFirstRunBootstrapTimeoutMs=3000&jobsFirstRunBootstrapProgressStaleMs=10000";

async function writeReport(report) {
  await fs.mkdir(path.dirname(REPORT_PATH), { recursive: true });
  await fs.writeFile(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`, "utf8");
}

function desktopUrl(relativePath) {
  const separator = relativePath.includes("?") ? "&" : "?";
  return `${BASE_URL}/${relativePath}${separator}desktop=1&bridgePort=${encodeURIComponent(BRIDGE_PORT)}&bridgeHost=${encodeURIComponent(BRIDGE_HOST)}`;
}

function allPages(browser) {
  return browser.contexts().flatMap(context => context.pages());
}

async function managedPage(browser) {
  return waitUntil("managed Chromium page", () => {
    const pages = allPages(browser).filter(page => !page.isClosed());
    return pages.find(page => String(page.url() || "").startsWith(BASE_URL)) || pages[0] || null;
  }, 15_000);
}

function isBootstrapRequest(request) {
  try {
    const url = new URL(request.url());
    return request.method() === "POST" && url.pathname === "/tasks/run-jobs-bootstrap";
  } catch {
    return false;
  }
}

async function waitForUiBootstrapRequest(requests, startIndex = 0) {
  return waitUntil(
    "UI bootstrap request",
    async () => requests.slice(startIndex).find(isBootstrapRequest) || null,
    15_000
  );
}

async function assertUiBootstrapRequest(request) {
  const body = JSON.parse(request.postData() || "{}");
  assert.deepEqual(body, { source: "jobs_first_run" }, "Jobs UI bootstrap request should own active work startup");
  const response = await waitUntil(
    "UI bootstrap response",
    () => request.response(),
    15_000,
    100
  );
  const payload = await response.json();
  assert.equal(
    Boolean(response?.ok() || (response?.status() === 409 && payload?.alreadyRunning)),
    true,
    "Jobs UI bootstrap request should start or attach to active work"
  );
  assert.equal(
    payload?.smokeMode,
    "controlled-heartbeat-success",
    "bootstrap route should expose long-heartbeat smoke mode"
  );
  assert.equal(
    Boolean(payload?.started || payload?.alreadyRunning),
    true,
    "UI bootstrap should start or attach to active work"
  );
  assert.match(String(payload?.runId || ""), /^jobs_bootstrap_[a-f0-9]{10}$/i);
  return payload;
}

async function connectManagedBrowser() {
  return waitUntil("managed Chromium CDP endpoint", async () => {
    try {
      return await chromium.connectOverCDP(`http://127.0.0.1:${CDP_PORT}`);
    } catch {
      return null;
    }
  }, 20_000, 500);
}

async function waitForActiveBootstrap(apiRequest, runId) {
  return waitUntil("active bootstrap task", async () => {
    const payload = await fetchBridgeJson(
      apiRequest,
      BRIDGE_BASE,
      "/ops/task-state?view=summary",
      "task state summary"
    );
    const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
    return tasks.find(row => String(row?.runId || "") === runId && isActiveTaskRow(row)) || null;
  }, 15_000);
}

async function main() {
  const report = {
    ok: false,
    scenarios: [],
    errors: [],
    startedAt: new Date().toISOString(),
    finishedAt: ""
  };
  let browser;
  let apiRequest;
  try {
    await fs.mkdir(OUTPUT_DIR, { recursive: true });
    assert.ok(CDP_PORT > 0, "active-task close smoke requires BALUFFO_PACKAGED_SMOKE_CDP_PORT");
    apiRequest = await playwrightRequest.newContext();
    browser = await connectManagedBrowser();
    const page = await managedPage(browser);
    const requests = [];
    page.on("request", request => {
      if (isBootstrapRequest(request)) requests.push(request);
    });
    await page.goto(desktopUrl(`jobs.html?${FIRST_RUN_SMOKE_QUERY}`), {
      waitUntil: "domcontentloaded"
    });
    await page.waitForFunction(() => Boolean(window.JobAppLocalData), null, { timeout: 30_000 });
    const startPayload = await assertUiBootstrapRequest(await waitForUiBootstrapRequest(requests));
    const runId = String(startPayload?.runId || "");
    await waitForActiveBootstrap(apiRequest, runId);
    await page.waitForTimeout(5500);

    let dialogAccepted = false;
    let dialogMessage = "";
    page.once("dialog", async dialog => {
      dialogMessage = String(dialog.message() || "");
      dialogAccepted = true;
      await dialog.accept();
    });
    const closePromise = page.close({ runBeforeUnload: true }).catch(error => {
      const message = String(error?.message || error || "");
      if (!/Target page, context or browser has been closed|browser has been closed/i.test(message)) {
        throw error;
      }
    });
    await waitUntil("active-work beforeunload confirmation", () => dialogAccepted, 10_000, 100);
    await closePromise;
    report.scenarios.push({
      name: "Confirmed active-task desktop close exits without relaunch",
      slug: "confirmed-active-task-desktop-close",
      status: "passed",
      durationMs: 0,
      error: "",
      runId,
      dialogAccepted,
      dialogMessage
    });
    report.ok = true;
  } catch (error) {
    report.errors.push(String(error?.stack || error?.message || error));
    report.scenarios.push({
      name: "Confirmed active-task desktop close exits without relaunch",
      slug: "confirmed-active-task-desktop-close",
      status: "failed",
      durationMs: 0,
      error: String(error?.message || error)
    });
  } finally {
    report.finishedAt = new Date().toISOString();
    await writeReport(report);
    await apiRequest?.dispose().catch(() => {});
    await browser?.close().catch(() => {});
  }
  if (!report.ok) {
    process.exitCode = 1;
  }
}

await main();
