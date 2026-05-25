import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium, request as playwrightRequest } from "@playwright/test";
import {
  capturePopup,
  collectAndAssertPopupStyles,
  coverageScope,
  dismissFirstRunNotice,
  fetchBridgeJson as fetchBridgeJsonWithBase,
  isActiveTaskRow,
  setThemeAndViewport,
  waitUntil
} from "./helpers/packaged-first-run-smoke-helpers.mjs";

const BASE_URL = process.env.PACKAGED_DESKTOP_BASE_URL || "http://127.0.0.1:8080";
const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";
const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/first-run-jobs-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/first-run-jobs-output");
const HEADED = process.env.PACKAGED_SMOKE_HEADED === "1";
const bridgeUrl = new URL(BRIDGE_BASE);
const BRIDGE_PORT = bridgeUrl.port || "8877";
const BRIDGE_HOST = bridgeUrl.hostname || "127.0.0.1";
const FIRST_RUN_TITLE = "Packaged First-Run Technical Cinematic Animator";
const FIRST_RUN_SMOKE_QUERY =
  "jobsColdStart=1&jobsFirstRunBootstrapTimeoutMs=3000&jobsFirstRunBootstrapProgressStaleMs=10000";
const VIEWPORTS = [
  { name: "desktop", width: 1280, height: 860 },
  { name: "mobile", width: 390, height: 760 }
];
const THEMES = ["light", "dark"];

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

async function fetchBridgeJson(apiRequest, relativePath, label) {
  return fetchBridgeJsonWithBase(apiRequest, BRIDGE_BASE, relativePath, label);
}

async function assertNoSeededJobsArtifacts(apiRequest) {
  for (const name of ["jobs-unified-light.json", "jobs-unified.json", "jobs-unified.csv"]) {
    const response = await apiRequest.get(`${BASE_URL}/data/${name}?preFirstRun=${Date.now()}`);
    if (!response.ok()) continue;
    const body = await response.text();
    assert.doesNotMatch(
      body,
      /Packaged Smoke Seed Job|Packaged First-Run Technical Cinematic Animator/,
      `${name} should not contain row-bearing startup artifacts before first-run bootstrap`
    );
    if (name.endsWith(".json")) {
      const parsed = JSON.parse(body || "[]");
      assert.equal(Array.isArray(parsed) ? parsed.length : 0, 0, `${name} should start empty`);
    }
  }
}

async function waitForFirstRunUi(page) {
  await page.waitForFunction(() => {
    const bodyText = String(document.body?.innerText || "");
    const listText = String(document.querySelector("#jobs-list")?.textContent || "");
    const state = document.body?.getAttribute("data-jobs-startup-state") || "";
    return Boolean(document.querySelector(".jobs-first-run-notice"))
      || /Preparing first-run jobs/i.test(bodyText)
      || (state === "error" && /Retry|first-run/i.test(listText + bodyText));
  }, null, { timeout: 30_000 });
  const bodyText = String(await page.locator("body").innerText({ timeout: 10_000 }) || "");
  const listText = String(await page.locator("#jobs-list").textContent({ timeout: 10_000 }) || "");
  assert.doesNotMatch(bodyText, /Bridge timed out/i, "first-run UI must not show raw bridge timeout");
  assert.doesNotMatch(bodyText, /first-run sheet refresh timed out|Retry quick refresh/i, "first-run UI must stay in progress while bootstrap is active");
  assert.match(
    `${bodyText}\n${listText}`,
    /Preparing first-run jobs/i,
    "cold Jobs page should show first-run progress"
  );
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
  return waitUntil("UI bootstrap request", async () => requests.slice(startIndex).find(isBootstrapRequest) || null, 15_000);
}

async function assertUiBootstrapRequest(request) {
  const body = JSON.parse(request.postData() || "{}");
  assert.deepEqual(body, { source: "jobs_first_run" }, "Jobs UI bootstrap request must not force duplicate refreshes");
  const response = await request.response();
  assert.equal(Boolean(response?.ok()), true, "Jobs UI bootstrap request should receive a successful bridge response");
  const payload = await response.json();
  assert.equal(payload?.smokeMode, "controlled-heartbeat-success", "bootstrap route should expose long-heartbeat smoke mode");
  assert.equal(
    Boolean(payload?.started || payload?.alreadyRunning || payload?.alreadyCompleted),
    true,
    "UI bootstrap should start, attach, or report already-completed state"
  );
  assert.match(String(payload?.runId || ""), /^jobs_bootstrap_[a-f0-9]{10}$/i);
  return payload;
}

async function waitForRunningBootstrapEvidence(apiRequest, runId) {
  const report = await waitUntil("running bootstrap report", async () => {
    const payload = await fetchBridgeJson(apiRequest, "/ops/fetch-report", "fetch report");
    if (
      String(payload?.runId || "") === runId
      && !String(payload?.finishedAt || "").trim()
      && coverageScope(payload) === "bootstrap_sheets"
    ) {
      return payload;
    }
    return null;
  }, 15_000);
  const taskState = await waitUntil("running bootstrap task state", async () => {
    const payload = await fetchBridgeJson(apiRequest, "/ops/task-state?view=summary", "task state summary");
    const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
    const active = tasks.find(row => String(row?.runId || "") === runId && isActiveTaskRow(row));
    return active ? payload : null;
  }, 15_000);
  return { report, taskState };
}

async function waitForPromotedFeed(page, apiRequest, runId) {
  const report = await waitUntil("terminal first-run bootstrap report", async () => {
    const payload = await fetchBridgeJson(apiRequest, "/ops/fetch-report", "fetch report");
    const outputCount = Number(payload?.summary?.outputCount || 0);
    if (
      String(payload?.runId || "") === runId
      && String(payload?.finishedAt || "").trim()
      && coverageScope(payload) === "bootstrap_sheets"
      && outputCount > 0
    ) {
      return payload;
    }
    return null;
  }, 60_000, 500);
  await page.waitForFunction(
    title => String(document.querySelector("#jobs-list")?.innerText || "").includes(title),
    FIRST_RUN_TITLE,
    { timeout: 30_000 }
  );
  const sourceStatus = String(await page.locator("#source-status").textContent() || "");
  assert.match(sourceStatus, /Sheet-limited first-run refresh/i);
  const bodyText = String(await page.locator("body").innerText({ timeout: 10_000 }) || "");
  assert.doesNotMatch(bodyText, /Bridge timed out/i, "promoted first-run feed must not show raw timeout text");
  assert.doesNotMatch(bodyText, /first-run sheet refresh timed out|Retry quick refresh/i, "promoted first-run feed must not show timeout or retry UI");
  const response = await apiRequest.get(`${BASE_URL}/data/jobs-unified-light.json?firstRun=${encodeURIComponent(runId)}`);
  assert.equal(response.ok(), true, "promoted first-run feed should be served from static JSON");
  const rows = await response.json();
  assert.equal(rows?.[0]?.title, FIRST_RUN_TITLE);
  return report;
}

async function assertActiveHeartbeatWithoutTimeout(page, apiRequest, runId) {
  const deadline = Date.now() + 5_000;
  let heartbeatSeen = false;
  while (Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, 500));
    const bodyText = String(await page.locator("body").innerText({ timeout: 10_000 }) || "");
    assert.doesNotMatch(bodyText, /first-run sheet refresh timed out|Retry quick refresh|Bridge timed out/i, "fresh bootstrap heartbeat must suppress timeout UI");
    const taskLive = await fetchBridgeJson(apiRequest, "/ops/task-live/fetch", "fetch task-live").catch(() => null);
    if (String(taskLive?.runId || "") === runId && taskLive?.active) {
      const heartbeatAt = Date.parse(String(taskLive?.heartbeatAt || taskLive?.taskProgress?.updatedAt || ""));
      if (Number.isFinite(heartbeatAt) && Date.now() - heartbeatAt < 10_000) {
        heartbeatSeen = true;
      }
    }
  }
  assert.equal(heartbeatSeen, true, "task-live should expose a fresh active bootstrap heartbeat");
}

async function assertNoDuplicateBootstrapAfterSuccess(page, requests) {
  const startIndex = requests.length;
  await gotoDesktop(page, `jobs.html?${FIRST_RUN_SMOKE_QUERY}`);
  await page.waitForFunction(
    title => String(document.querySelector("#jobs-list")?.innerText || "").includes(title),
    FIRST_RUN_TITLE,
    { timeout: 30_000 }
  );
  await new Promise(resolve => setTimeout(resolve, 1_000));
  const duplicateRequests = requests.slice(startIndex).filter(isBootstrapRequest);
  for (const request of duplicateRequests) {
    const response = await request.response();
    const payload = response ? await response.json() : {};
    assert.equal(payload?.alreadyCompleted, true, "post-success cold reload must not start a second bootstrap");
  }
}

async function seedExistingProfile(page) {
  await dismissFirstRunNotice(page);
  await page.locator("#auth-sign-in-btn").click();
  await page.locator("#local-auth-name-input").waitFor({ state: "visible", timeout: 10_000 });
  await page.locator("#local-auth-name-input").fill("Packaged First Run Smoke User");
  await page.locator("#local-auth-name-input").press("Enter");
  await page.locator("#auth-sign-out-btn").waitFor({ state: "visible", timeout: 10_000 });
  await page.locator("#auth-sign-out-btn").click();
  await page.locator("#auth-sign-in-btn").waitFor({ state: "visible", timeout: 10_000 });
}

async function captureVisuals(page, styles) {
  for (const viewport of VIEWPORTS) {
    for (const theme of THEMES) {
      await setThemeAndViewport(page, theme, viewport);
      await capturePopup(page, styles, OUTPUT_DIR, "first-run-notice", theme, viewport);
    }
  }
  await seedExistingProfile(page);
  for (const viewport of VIEWPORTS) {
    for (const theme of THEMES) {
      await setThemeAndViewport(page, theme, viewport);
      await page.locator("#auth-sign-in-btn").click();
      await page.locator("#local-auth-profile-select").waitFor({ state: "visible", timeout: 10_000 });
      await collectAndAssertPopupStyles(
        page,
        styles,
        `local-auth-existing-${theme}-${viewport.name}`,
        { requireSecondary: true, requireSelect: true }
      );
      await page.locator("#local-auth-create-btn").click();
      await page.locator("#local-auth-name-input").waitFor({ state: "visible", timeout: 10_000 });
      await capturePopup(page, styles, OUTPUT_DIR, "local-auth", theme, viewport, {
        requireSecondary: true,
        requireTertiary: true,
        requireInput: true
      });
      await page.locator("#local-auth-cancel-btn").click();
      await page.locator(".local-auth-dialog").waitFor({ state: "detached", timeout: 10_000 });
    }
  }
}

async function main() {
  const report = {
    ok: false,
    startedAt: new Date().toISOString(),
    finishedAt: "",
    scenarios: [],
    errors: [],
    artifacts: { outputDir: OUTPUT_DIR }
  };
  const styles = [];
  let browser;
  let context;
  let apiRequest;
  try {
    await fs.mkdir(OUTPUT_DIR, { recursive: true });
    apiRequest = await playwrightRequest.newContext();
    await assertNoSeededJobsArtifacts(apiRequest);
    browser = await chromium.launch({ headless: !HEADED, ...(process.env.PACKAGED_SMOKE_SYSTEM_CHROMIUM === "1" ? { channel: "chromium" } : {}) });
    context = await browser.newContext({ viewport: VIEWPORTS[0] });
    const page = await context.newPage();
    const bootstrapRequests = [];
    page.on("request", request => {
      if (isBootstrapRequest(request)) bootstrapRequests.push(request);
    });
    await gotoDesktop(page, `jobs.html?${FIRST_RUN_SMOKE_QUERY}`);
    await waitForFirstRunUi(page);
    const uiBootstrapRequest = await waitForUiBootstrapRequest(bootstrapRequests);
    const startPayload = await assertUiBootstrapRequest(uiBootstrapRequest);
    const runId = String(startPayload.runId || "");
    const runningEvidence = await waitForRunningBootstrapEvidence(apiRequest, runId);
    await assertActiveHeartbeatWithoutTimeout(page, apiRequest, runId);
    await captureVisuals(page, styles);
    const terminalReport = await waitForPromotedFeed(page, apiRequest, runId);
    await assertNoDuplicateBootstrapAfterSuccess(page, bootstrapRequests);
    const styleReportPath = path.join(OUTPUT_DIR, "first-run-style-report.json");
    await fs.writeFile(styleReportPath, `${JSON.stringify(styles, null, 2)}\n`, "utf8");
    report.artifacts.styleReport = styleReportPath;
    report.scenarios.push({
      name: "Deterministic packaged first-run Jobs bootstrap",
      slug: "deterministic-packaged-first-run-jobs-bootstrap",
      status: "passed",
      durationMs: 0,
      error: "",
      runId,
      runningReportRunId: runningEvidence.report.runId,
      terminalOutputCount: Number(terminalReport?.summary?.outputCount || 0)
    });
    report.ok = true;
  } catch (error) {
    report.errors.push(String(error?.stack || error?.message || error));
    report.scenarios.push({
      name: "Deterministic packaged first-run Jobs bootstrap",
      slug: "deterministic-packaged-first-run-jobs-bootstrap",
      status: "failed",
      durationMs: 0,
      error: String(error?.message || error)
    });
    report.ok = false;
  } finally {
    report.finishedAt = new Date().toISOString();
    try {
      if (styles.length) {
        const styleReportPath = path.join(OUTPUT_DIR, "first-run-style-report.json");
        await fs.writeFile(styleReportPath, `${JSON.stringify(styles, null, 2)}\n`, "utf8");
        report.artifacts.styleReport = styleReportPath;
      }
    } catch {
      // Keep the primary smoke failure visible if artifact cleanup fails.
    }
    await writeReport(report);
    await apiRequest?.dispose().catch(() => {});
    await context?.close().catch(() => {});
    await browser?.close().catch(() => {});
  }
  if (!report.ok) {
    process.exitCode = 1;
  }
}

await main();
