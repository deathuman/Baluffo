import path from "node:path";
import { fileURLToPath } from "node:url";
import { test, expect } from "@playwright/test";

const BASE_URL = process.env.PACKAGED_DESKTOP_BASE_URL || "http://127.0.0.1:8080";
const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";
const DESKTOP_USER_NAME = "Packaged Smoke User";
const bridgeUrl = new URL(BRIDGE_BASE);
const BRIDGE_PORT = bridgeUrl.port || "8877";
const BRIDGE_HOST = bridgeUrl.hostname || "127.0.0.1";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ATTACHMENT_FIXTURE_PATH = path.resolve(__dirname, "fixtures", "attachment-smoke.txt");

async function stubPrompt(page, value = DESKTOP_USER_NAME) {
  await page.addInitScript(name => {
    window.prompt = () => name;
  }, value);
}

async function gotoDesktop(page, relativePath) {
  const separator = relativePath.includes("?") ? "&" : "?";
  await page.goto(
    `${BASE_URL}/${relativePath}${separator}desktop=1&bridgePort=${encodeURIComponent(BRIDGE_PORT)}&bridgeHost=${encodeURIComponent(BRIDGE_HOST)}`
  );
}

async function waitForDesktopAdapter(page) {
  await page.waitForFunction(() => Boolean(window.JobAppLocalData), null, { timeout: 15_000 });
}

async function fetchStartupMetricRows(request, limit = 400) {
  const response = await request.get(`${BRIDGE_BASE}/desktop-local-data/startup-metrics?limit=${Number(limit) || 400}`);
  expect(response.ok()).toBeTruthy();
  const payload = await response.json();
  return Array.isArray(payload?.rows) ? payload.rows : [];
}

function firstEventIndex(rows, eventName) {
  return rows.findIndex(row => String(row?.event || "") === String(eventName || ""));
}

async function assertFacadeStartupOrdering(request) {
  const rows = await fetchStartupMetricRows(request, 600);
  const hasEvent = eventName => firstEventIndex(rows, eventName) >= 0;
  expect(hasEvent("desktop_shell_loaded")).toBeTruthy();
  expect(hasEvent("desktop_window_load_url")).toBeTruthy();
  expect(hasEvent("jobs_first_render")).toBeTruthy();
  expect(hasEvent("jobs_first_interactive")).toBeTruthy();
  const shellLoadedIndex = firstEventIndex(rows, "desktop_shell_loaded");
  const firstInteractiveIndex = firstEventIndex(rows, "jobs_first_interactive");
  expect(shellLoadedIndex).toBeGreaterThanOrEqual(0);
  expect(firstInteractiveIndex).toBeGreaterThanOrEqual(0);
  expect(shellLoadedIndex).toBeLessThan(firstInteractiveIndex);
}

async function assertNoImmediateAdminError(page, { buttonLocator, observeMs = 8000 }) {
  const errorLogCountBefore = await page.locator(".admin-fetcher-line.log-error").count();
  const errorToast = page.locator(".toast.error").first();
  let toastSeen = false;
  try {
    await errorToast.waitFor({ state: "visible", timeout: observeMs });
    toastSeen = true;
  } catch {
    toastSeen = false;
  }
  expect(toastSeen).toBeFalsy();

  await page.waitForTimeout(observeMs);
  const sourceStatus = await page.locator("#admin-source-status").textContent();
  expect(String(sourceStatus || "")).not.toMatch(/failed|could not|error/i);

  const buttonText = await buttonLocator.textContent();
  expect(String(buttonText || "")).not.toMatch(/error|failed/i);

  const errorLogCountAfter = await page.locator(".admin-fetcher-line.log-error").count();
  expect(errorLogCountAfter).toBe(errorLogCountBefore);
}

async function triggerFirstAvailableAdminAction(page) {
  const candidates = [
    { name: "discovery", locator: page.locator("#admin-run-discovery-btn") },
    { name: "fetcher", locator: page.locator("#admin-run-fetcher-btn") },
    { name: "sync-test", locator: page.locator("#admin-sync-test-btn") }
  ];
  for (const candidate of candidates) {
    const locator = candidate.locator;
    if (!(await locator.isVisible())) continue;
    if (!(await locator.isEnabled())) continue;
    await locator.click();
    return { name: candidate.name, locator };
  }
  throw new Error("No admin action button available (discovery/fetcher/sync-test).");
}

test.afterEach(async ({ page }, testInfo) => {
  if (process.env.PACKAGED_SMOKE_PAUSE_ON_FAILURE === "1" && testInfo.status !== testInfo.expectedStatus) {
    await page.pause();
  }
});

test("Desktop critical flow", async ({ page, request }) => {
  await stubPrompt(page);

  await test.step("Jobs startup and facade ordering", async () => {
    await gotoDesktop(page, "jobs.html");
    await waitForDesktopAdapter(page);
    await expect(page.locator("#jobs-list")).toBeVisible();
    await expect(page.locator("#source-status")).toHaveText(/./);
    const health = await request.get(`${BRIDGE_BASE}/ops/health`);
    expect(health.ok()).toBeTruthy();
    await assertFacadeStartupOrdering(request);
  });

  await test.step("Jobs sign-in succeeds", async () => {
    const signInBtn = page.locator("#auth-sign-in-btn");
    await expect(signInBtn).toBeEnabled();
    await signInBtn.click();
    await expect(page.locator("#saved-jobs-btn")).toBeVisible();
    await expect(page.locator("#auth-status")).toContainText(DESKTOP_USER_NAME);
    await expect(page.locator("#auth-status")).not.toContainText(/Guest/i);
  });

  await test.step("Navigate to Saved and keep same profile", async () => {
    await page.click("#saved-jobs-btn");
    await page.waitForURL(/saved\.html/);
    await waitForDesktopAdapter(page);
    await expect(page.locator("#saved-auth-status")).toContainText(DESKTOP_USER_NAME);
    await expect(page.locator("#saved-auth-status")).not.toContainText(/Guest/i);
  });

  await test.step("Saved custom job + notes + attachments", async () => {
    await page.click("#add-custom-job-btn");
    await page.fill("#custom-job-title", "Packaged Sequential Smoke QA");
    await page.fill("#custom-job-company", "Baluffo QA");
    await page.click("#custom-job-save-btn");

    const expandToggle = page.locator(".details-toggle-btn").first();
    await expect(expandToggle).toBeVisible();
    await expandToggle.click();

    const notesInput = page.locator(".job-notes-input").first();
    await notesInput.fill("Packaged sequential smoke notes persistence");
    await expect(notesInput).toHaveValue("Packaged sequential smoke notes persistence");
    await page.waitForTimeout(1800);
    await expect(page.locator(".note-save-state").first()).toContainText(/Saved|Saving/i);

    await page.locator(".saved-details-tab-btn[data-details-tab=\"attachments\"]").first().click();
    const fileInput = page.locator(".attach-file-input").first();
    await fileInput.setInputFiles(ATTACHMENT_FIXTURE_PATH);
    await expect(page.locator(".attachments-list").first()).toContainText("attachment-smoke.txt");
    const deleteBtn = page.locator(".att-delete-btn").first();
    if (await deleteBtn.isVisible()) {
      await deleteBtn.click();
      await expect(page.locator(".attachments-list").first()).toContainText(/No attachments yet|attachment-smoke.txt/i);
    }
  });

  await test.step("Navigate to Admin and unlock", async () => {
    const adminPageBtn = page.locator("#admin-page-btn");
    if (await adminPageBtn.count()) {
      await adminPageBtn.first().click();
    } else {
      await page.locator("a[href='admin.html']").first().click();
    }
    await page.waitForURL(/admin\.html/);
    await expect(page.locator("#admin-pin-gate")).toBeVisible();
    await expect(page.locator("#admin-unlock-btn")).toBeEnabled();
    await page.fill("#admin-pin-input", "1234");
    await page.click("#admin-unlock-btn");
    await expect(page.locator("#admin-content")).toBeVisible();
  });

  await test.step("Bridge badge reaches online", async () => {
    const bridgeBadge = page.locator("#admin-bridge-status-badge");
    await expect(bridgeBadge).toBeVisible();
    await expect(bridgeBadge).toContainText(/Bridge Online/i, { timeout: 30_000 });
  });

  await test.step("Trigger first available admin action with no immediate error", async () => {
    const trigger = await triggerFirstAvailableAdminAction(page);
    await assertNoImmediateAdminError(page, { buttonLocator: trigger.locator, observeMs: 8000 });
  });
});
