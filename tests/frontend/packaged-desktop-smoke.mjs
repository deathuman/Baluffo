import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium, request as playwrightRequest } from "@playwright/test";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BASE_URL = process.env.PACKAGED_DESKTOP_BASE_URL || "http://127.0.0.1:8080";
const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";
const DESKTOP_USER_NAME = "Packaged Smoke User";
const ATTACHMENT_FIXTURE_PATH = path.resolve(__dirname, "fixtures", "attachment-smoke.txt");
const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/smoke-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/smoke-output");
const HEADED = process.env.PACKAGED_SMOKE_HEADED === "1";
const PAUSE_ON_FAILURE = process.env.PACKAGED_SMOKE_PAUSE_ON_FAILURE === "1";
const bridgeUrl = new URL(BRIDGE_BASE);
const BRIDGE_PORT = bridgeUrl.port || "8877";
const BRIDGE_HOST = bridgeUrl.hostname || "127.0.0.1";

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

async function signInWithProfile(page, buttonSelector, profileName, expectedFocusSelector) {
  await page.locator(buttonSelector).click();
  const profileInput = page.locator("#local-auth-name-input");
  await profileInput.waitFor({ state: "visible", timeout: 10_000 });
  await profileInput.fill(profileName);
  await profileInput.press("Enter");
  await profileInput.waitFor({ state: "detached", timeout: 10_000 });
  if (expectedFocusSelector) {
    await page.waitForFunction(
      selector => document.activeElement === document.querySelector(selector),
      expectedFocusSelector,
      { timeout: 10_000 }
    );
  }
}

async function waitForDesktopAdapter(page) {
  await page.waitForFunction(() => Boolean(window.JobAppLocalData), null, { timeout: 30_000 });
}

async function assertJobsPageReady(page) {
  await page.waitForFunction(() => {
    const state = document.body?.getAttribute("data-jobs-startup-state") || "loading";
    return state === "interactive" || state === "error";
  }, null, { timeout: 30_000 });
  const startupState = await page.locator("body").getAttribute("data-jobs-startup-state");
  assert.notEqual(startupState, "loading", "jobs page should not stay in loading state");
  await page.locator("#refresh-jobs-btn").waitFor({ state: "visible", timeout: 20_000 });
  await page.locator("#auth-sign-in-btn").waitFor({ state: "visible", timeout: 20_000 });
  await page.waitForFunction(() => !document.querySelector("#auth-sign-in-btn")?.disabled, null, { timeout: 20_000 });
  assert.equal(await page.locator("#refresh-jobs-btn").isEnabled(), true, "jobs refresh button should be enabled");
  assert.equal(await page.locator("#auth-sign-in-btn").isEnabled(), true, "jobs sign-in button should be enabled");
  const jobsListText = await page.locator("#jobs-list").textContent();
  assert.doesNotMatch(String(jobsListText || ""), /loading jobs/i);
}

async function fetchStartupMetricRows(apiRequest, limit = 400) {
  const response = await apiRequest.get(`${BRIDGE_BASE}/desktop-local-data/startup-metrics?limit=${Number(limit) || 400}`);
  assert.equal(response.ok(), true, "startup metrics request should succeed");
  const payload = await response.json();
  return Array.isArray(payload?.rows) ? payload.rows : [];
}

async function fetchDesktopSession(apiRequest) {
  const response = await apiRequest.get(`${BRIDGE_BASE}/desktop-local-data/session`);
  assert.equal(response.ok(), true, "desktop session request should succeed");
  const payload = await response.json();
  return payload && typeof payload === "object" ? payload : {};
}

function firstEventIndex(rows, eventName) {
  return rows.findIndex(row => String(row?.event || "") === String(eventName || ""));
}

function eventElapsedMs(row) {
  const fieldValue = row?.fields?.elapsedMs;
  if (Number.isFinite(Number(fieldValue))) return Number(fieldValue);
  const payloadValue = row?.payload?.elapsedMs;
  if (Number.isFinite(Number(payloadValue))) return Number(payloadValue);
  return null;
}

function eventInstanceKey(row) {
  const payloadValue = row?.payload?.browserCreatedAtMs;
  if (payloadValue !== undefined && payloadValue !== null && String(payloadValue).trim()) {
    return `browser-created:${String(payloadValue)}`;
  }
  const fieldValue = row?.fields?.browserCreatedAtMs;
  if (fieldValue !== undefined && fieldValue !== null && String(fieldValue).trim()) {
    return `browser-created:${String(fieldValue)}`;
  }
  const browserTsValue = row?.browserTsMs;
  if (browserTsValue !== undefined && browserTsValue !== null && String(browserTsValue).trim()) {
    return `browser-ts:${String(browserTsValue)}`;
  }
  return "";
}

function findStartupMetricPair(rows, firstEventName, secondEventName) {
  const firstRows = rows
    .map((row, index) => ({ row, index }))
    .filter(entry => String(entry.row?.event || "") === String(firstEventName || ""));
  const secondRows = rows
    .map((row, index) => ({ row, index }))
    .filter(entry => String(entry.row?.event || "") === String(secondEventName || ""));
  for (let firstIndex = firstRows.length - 1; firstIndex >= 0; firstIndex -= 1) {
    const firstEntry = firstRows[firstIndex];
    const key = eventInstanceKey(firstEntry.row);
    if (!key) continue;
    const secondEntry = secondRows.find(entry =>
      entry.index >= firstEntry.index && eventInstanceKey(entry.row) === key
    );
    if (secondEntry) {
      return {
        firstRow: firstEntry.row,
        firstIndex: firstEntry.index,
        secondRow: secondEntry.row,
        secondIndex: secondEntry.index
      };
    }
  }
  return null;
}

async function assertFacadeStartupOrdering(apiRequest) {
  let rows = [];
  for (let attempt = 0; attempt < 50; attempt += 1) {
    rows = await fetchStartupMetricRows(apiRequest, 600);
    const jobsStartupPair = findStartupMetricPair(rows, "jobs_first_render", "jobs_first_interactive");
    if (
      ["desktop_browser_launch_selected", "desktop_shell_window_shown"].every(eventName => firstEventIndex(rows, eventName) >= 0)
      && jobsStartupPair
    ) break;
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  const sessionPayload = await fetchDesktopSession(apiRequest);
  const desktopSession = sessionPayload?.desktopSession && typeof sessionPayload.desktopSession === "object"
    ? sessionPayload.desktopSession
    : {};
  const hasEvent = eventName => firstEventIndex(rows, eventName) >= 0;
  assert.equal(hasEvent("desktop_browser_launch_selected"), true, "desktop browser launch event missing");
  assert.equal(hasEvent("desktop_shell_window_shown"), true, "desktop shell shown event missing");
  assert.equal(hasEvent("jobs_first_render"), true, "jobs first render missing");
  assert.equal(hasEvent("jobs_first_interactive"), true, "jobs first interactive missing");
  const jobsStartupPair = findStartupMetricPair(rows, "jobs_first_render", "jobs_first_interactive");
  assert.ok(jobsStartupPair, "jobs first render/interactive pair missing for the same page instance");
  assert.ok(String(desktopSession.sessionId || "").trim(), "desktop session id missing");
  assert.ok(String(desktopSession.ownerToken || "").trim(), "desktop owner token missing");
  assert.ok(
    String(desktopSession.lastActivityAt || sessionPayload.lastActivityAt || "").trim(),
    "desktop session activity timestamp missing"
  );
  const browserLaunchRow = rows.find(row => String(row?.event || "") === "desktop_browser_launch_selected");
  const shellLoadedRow = rows.find(row => String(row?.event || "") === "desktop_shell_window_shown");
  const firstRenderRow = jobsStartupPair.firstRow;
  const firstInteractiveRow = jobsStartupPair.secondRow;
  assert.ok(browserLaunchRow, "desktop browser launch row missing");
  assert.ok(shellLoadedRow, "desktop shell shown row missing");
  assert.ok(firstRenderRow, "jobs first render row missing");
  assert.ok(firstInteractiveRow, "jobs first interactive row missing");
  const browserLaunchElapsedMs = eventElapsedMs(browserLaunchRow);
  const shellLoadedElapsedMs = eventElapsedMs(shellLoadedRow);
  const firstRenderElapsedMs = eventElapsedMs(firstRenderRow);
  const firstInteractiveElapsedMs = eventElapsedMs(firstInteractiveRow);
  if (browserLaunchElapsedMs !== null && shellLoadedElapsedMs !== null) {
    assert.ok(shellLoadedElapsedMs <= browserLaunchElapsedMs, "desktop shell shown should not lag browser launch selection");
  }
  assert.ok(jobsStartupPair.firstIndex <= jobsStartupPair.secondIndex, "jobs first render should be recorded before jobs interactive");
  if (firstRenderElapsedMs !== null && firstInteractiveElapsedMs !== null) {
    assert.ok(firstRenderElapsedMs <= firstInteractiveElapsedMs, "jobs first render should happen before jobs interactive");
  }
}

async function visibleErrorToastText(page) {
  const messages = await page.locator(".toast.error.visible").evaluateAll(nodes =>
    nodes.map(node => String(node.textContent || "").trim()).filter(Boolean)
  ).catch(() => []);
  return messages.join("; ");
}

async function assertNoImmediateAdminError(page, { buttonLocator, actionName = "admin action", observeMs = 8_000 }) {
  const errorLogCountBefore = await page.locator(".admin-fetcher-line.log-error").count();
  const errorToast = page.locator(".toast.error").first();
  let toastSeen = false;
  try {
    await errorToast.waitFor({ state: "visible", timeout: observeMs });
    toastSeen = true;
  } catch {
    toastSeen = false;
  }
  if (toastSeen) {
    const toastText = await visibleErrorToastText(page);
    assert.fail(`unexpected admin error toast after ${actionName}${toastText ? `: ${toastText}` : ""}`);
  }

  await page.waitForTimeout(observeMs);
  const sourceStatus = await page.locator("#admin-source-status").textContent();
  assert.ok(!/failed|could not|error/i.test(String(sourceStatus || "")), "admin source status indicates failure");

  const buttonText = await buttonLocator.textContent();
  assert.ok(!/error|failed/i.test(String(buttonText || "")), "admin action button indicates failure");

  const errorLogCountAfter = await page.locator(".admin-fetcher-line.log-error").count();
  assert.equal(errorLogCountAfter, errorLogCountBefore, "admin error log count changed unexpectedly");
}

async function assertAdminStartupSettled(page) {
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
  assert.doesNotMatch(String(sourceStatus || ""), /Loading admin overview/i);
  assert.match(String(sourceStatus || ""), /Loaded \d+ user account/i);
  const bridgeBadgeText = await page.locator("#admin-bridge-status-badge").textContent();
  assert.match(String(bridgeBadgeText || ""), /Bridge Online/i);
  assert.doesNotMatch(String(bridgeBadgeText || ""), /Bridge Checking/i);
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

async function main() {
  const scenarios = [];
  const errors = [];
  let browser;
  let context;
  let page;
  let apiRequest;
  try {
    await fs.mkdir(OUTPUT_DIR, { recursive: true });
    browser = await chromium.launch({ headless: !HEADED });
    context = await browser.newContext({
      baseURL: BASE_URL,
      acceptDownloads: true
    });
    page = await context.newPage();
    apiRequest = await playwrightRequest.newContext({ baseURL: BRIDGE_BASE });
    await runScenario("Jobs startup and facade ordering", async () => {
      const pageErrors = [];
      page.on("pageerror", error => pageErrors.push(String(error?.message || error)));
      await gotoDesktop(page, "jobs.html");
      await waitForDesktopAdapter(page);
      await page.locator("#jobs-list").waitFor({ state: "visible", timeout: 30_000 });
      await assertJobsPageReady(page);
      assert.equal(pageErrors.length, 0, `unexpected jobs startup page errors: ${pageErrors.join("; ")}`);
      const sourceStatus = await page.locator("#source-status").textContent();
      assert.ok(String(sourceStatus || "").trim().length > 0, "jobs source status should not be empty");
      const health = await apiRequest.get(`${BRIDGE_BASE}/ops/health`);
      assert.equal(health.ok(), true, "ops health should be reachable");
      const healthPayload = await health.json();
      assert.equal(Boolean(healthPayload?.desktopMode), true, "desktop bridge should report desktop mode");
      await assertFacadeStartupOrdering(apiRequest);
    }, scenarios);

    await runScenario("Desktop update UI is visible on jobs page", async () => {
      const updateToggle = page.locator("#desktop-update-toggle-btn");
      await updateToggle.waitFor({ state: "visible", timeout: 15_000 });
      assert.equal(await updateToggle.isEnabled(), true, "desktop update toggle should be enabled");
      await updateToggle.click();
      const updatePanel = page.locator("#desktop-update-panel");
      await updatePanel.waitFor({ state: "visible", timeout: 15_000 });
      const titleText = await page.locator("#desktop-update-title").textContent();
      assert.match(String(titleText || ""), /Desktop updates|Baluffo is up to date|available|Could not check/i);
    }, scenarios);

    await runScenario("Jobs sign-in succeeds", async () => {
      const signInBtn = page.locator("#auth-sign-in-btn");
      await signInBtn.waitFor({ state: "visible", timeout: 10_000 });
      assert.equal(await signInBtn.isEnabled(), true, "sign in button should be enabled");
      await signInWithProfile(page, "#auth-sign-in-btn", DESKTOP_USER_NAME, "#saved-jobs-btn");
      await page.locator("#saved-jobs-btn").waitFor({ state: "visible", timeout: 15_000 });
      const authStatus = await page.locator("#auth-status").textContent();
      assert.match(String(authStatus || ""), /Packaged Smoke User/);
      assert.doesNotMatch(String(authStatus || ""), /Guest/i);
    }, scenarios);

    await runScenario("Navigate to Saved and keep same profile", async () => {
      await page.click("#saved-jobs-btn");
      await page.waitForURL(/saved\.html/, { timeout: 15_000 });
      await waitForDesktopAdapter(page);
      await page.waitForFunction(() => /Packaged Smoke User/.test(document.querySelector("#saved-auth-status")?.textContent || ""), null, { timeout: 15_000 });
      const savedAuth = await page.locator("#saved-auth-status").textContent();
      assert.match(String(savedAuth || ""), /Packaged Smoke User/);
      assert.doesNotMatch(String(savedAuth || ""), /Guest/i);
    }, scenarios);

    await runScenario("Saved custom job plus notes and attachments", async () => {
      await page.click("#add-custom-job-btn");
      await page.fill("#custom-job-title", "Packaged Sequential Smoke QA");
      await page.fill("#custom-job-company", "Baluffo QA");
      await page.click("#custom-job-save-btn");

      const expandToggle = page.locator(".details-toggle-btn").first();
      await expandToggle.waitFor({ state: "visible", timeout: 10_000 });
      await expandToggle.click();

      const notesInput = page.locator(".job-notes-input").first();
      await notesInput.fill("Packaged sequential smoke notes persistence");
      assert.equal(await notesInput.inputValue(), "Packaged sequential smoke notes persistence");
      await page.waitForTimeout(1800);
      const saveState = await page.locator(".note-save-state").first().textContent();
      assert.match(String(saveState || ""), /Saved|Saving/i);

      await page.locator('.saved-details-tab-btn[data-details-tab="attachments"]').first().click();
      const fileInput = page.locator(".attach-file-input").first();
      await fileInput.setInputFiles(ATTACHMENT_FIXTURE_PATH);
      await page.waitForFunction(
        () => /attachment-smoke\.txt/i.test(document.querySelector(".attachments-list")?.textContent || ""),
        null,
        { timeout: 10_000 }
      );
      const attachmentsText = await page.locator(".attachments-list").first().textContent();
      assert.match(String(attachmentsText || ""), /attachment-smoke\.txt/);
      const deleteBtn = page.locator(".att-delete-btn").first();
      if (await deleteBtn.isVisible()) {
        await deleteBtn.click();
        await page.waitForFunction(
          () => /No attachments yet|attachment-smoke\.txt/i.test(document.querySelector(".attachments-list")?.textContent || ""),
          null,
          { timeout: 10_000 }
        );
        const postDeleteText = await page.locator(".attachments-list").first().textContent();
        assert.match(String(postDeleteText || ""), /No attachments yet|attachment-smoke\.txt/i);
      }
    }, scenarios);

    await runScenario("Navigate to Admin from jobs button", async () => {
      const adminPageBtn = page.locator("#admin-page-btn");
      if (await adminPageBtn.count()) {
        await page.waitForFunction(
          () => /Admin Online/i.test(document.querySelector("#admin-page-btn")?.textContent || ""),
          null,
          { timeout: 30_000 }
        );
        assert.equal(await adminPageBtn.isEnabled(), true, "jobs admin button should be enabled when bridge is online");
        await adminPageBtn.first().click();
      } else {
        await page.locator("a[href='admin.html']").first().click();
      }
      await page.waitForURL(/admin\.html/, { timeout: 15_000 });
      await page.locator("#admin-content").waitFor({ state: "visible", timeout: 15_000 });
      await assertAdminStartupSettled(page);
    }, scenarios);

    await runScenario("Bridge badge reaches online", async () => {
      const bridgeBadge = page.locator("#admin-bridge-status-badge");
      await bridgeBadge.waitFor({ state: "visible", timeout: 10_000 });
      await page.waitForFunction(
        () => /Bridge Online/i.test(document.querySelector("#admin-bridge-status-badge")?.textContent || ""),
        null,
        { timeout: 30_000 }
      );
      const bridgeBadgeText = await bridgeBadge.textContent();
      assert.doesNotMatch(String(bridgeBadgeText || ""), /Bridge Checking/i);
    }, scenarios);

    await runScenario("Admin discovery run launches without immediate error", async () => {
      const discoveryBtn = page.locator("#admin-run-discovery-btn");
      await discoveryBtn.waitFor({ state: "visible", timeout: 15_000 });
      await page.waitForFunction(
        () => {
          const button = document.querySelector("#admin-run-discovery-btn");
          return Boolean(button) && !button.disabled;
        },
        null,
        { timeout: 20_000 }
      );
      await discoveryBtn.click();
      await assertNoImmediateAdminError(page, { buttonLocator: discoveryBtn, actionName: "discovery", observeMs: 8_000 });
    }, scenarios);

    await runScenario("Admin remains responsive after discovery launch", async () => {
      await page.locator("#admin-run-discovery-btn").waitFor({ state: "visible", timeout: 15_000 });
      const taskState = await apiRequest.get(`${BRIDGE_BASE}/ops/task-state`);
      assert.equal(taskState.ok(), true, "admin task state should stay reachable after discovery launch");
      await page.waitForFunction(
        () => /Bridge Online/i.test(document.querySelector("#admin-bridge-status-badge")?.textContent || ""),
        null,
        { timeout: 30_000 }
      );
      const bridgeBadgeText = await page.locator("#admin-bridge-status-badge").textContent();
      assert.match(String(bridgeBadgeText || ""), /Bridge Online/i);
      const sourceStatus = await page.locator("#admin-source-status").textContent();
      assert.ok(!/failed|could not|error/i.test(String(sourceStatus || "")), "admin source status indicates failure");
      const toastText = await visibleErrorToastText(page);
      assert.equal(toastText, "", `unexpected admin error toast appeared: ${toastText}`);
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
    errors
  };
  await writeReport(report);
  if (!report.ok) {
    console.error("Smoke test failed:", report.errors);
  }
  process.exit(report.ok ? 0 : 1);
}

await main().catch(err => {
  console.error("Fatal error in main():", err);
  process.exit(1);
});
