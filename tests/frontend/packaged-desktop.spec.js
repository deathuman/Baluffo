import { test, expect } from "@playwright/test";

const BASE_URL = process.env.PACKAGED_DESKTOP_BASE_URL || "http://127.0.0.1:8080";
const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";
const DESKTOP_USER_NAME = "Packaged Smoke User";
const bridgeUrl = new URL(BRIDGE_BASE);
const BRIDGE_PORT = bridgeUrl.port || "8877";
const BRIDGE_HOST = bridgeUrl.hostname || "127.0.0.1";

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

async function signInOnJobs(page) {
  await stubPrompt(page);
  await gotoDesktop(page, "jobs.html");
  await waitForDesktopAdapter(page);
  await expect(page.locator("#jobs-list")).toBeVisible();
  const signInBtn = page.locator("#auth-sign-in-btn");
  await expect(signInBtn).toBeEnabled();
  await signInBtn.click();
  await expect(page.locator("#saved-jobs-btn")).toBeVisible();
  await expect(page.locator("#auth-status")).not.toContainText(/Guest/i);
}

async function signInOnSaved(page) {
  await stubPrompt(page);
  await gotoDesktop(page, "saved.html");
  await waitForDesktopAdapter(page);
  const authStatus = page.locator("#saved-auth-status");
  if ((await authStatus.textContent() || "").match(/guest/i)) {
  const signInBtn = page.locator("#saved-auth-sign-in-btn");
    await expect(signInBtn).toBeVisible();
    await expect(signInBtn).toBeEnabled();
    await signInBtn.click();
  }
  await expect(authStatus).not.toContainText(/Guest/i);
}

test.afterEach(async ({ page }, testInfo) => {
  if (process.env.PACKAGED_SMOKE_PAUSE_ON_FAILURE === "1" && testInfo.status !== testInfo.expectedStatus) {
    await page.pause();
  }
});

test("Startup", async ({ page, request }) => {
  await gotoDesktop(page, "jobs.html");
  await waitForDesktopAdapter(page);
  await expect(page.locator("#jobs-list")).toBeVisible();
  await expect(page.locator("#source-status")).toHaveText(/./);

  const health = await request.get(`${BRIDGE_BASE}/ops/health`);
  expect(health.ok()).toBeTruthy();

  const metricsResponse = await request.get(`${BRIDGE_BASE}/desktop-local-data/startup-metrics?limit=200`);
  expect(metricsResponse.ok()).toBeTruthy();
  const metricsPayload = await metricsResponse.json();
  const events = new Set((metricsPayload.rows || []).map(row => String(row?.event || "")));
  expect(events.has("desktop_site_ready")).toBeTruthy();
  expect(events.has("desktop_window_load_url")).toBeTruthy();
});

test("Auth continuity", async ({ page }) => {
  await signInOnJobs(page);
  await page.click("#saved-jobs-btn");
  await page.waitForURL(/saved\.html/);
  await waitForDesktopAdapter(page);
  await expect(page.locator("#saved-auth-status")).not.toContainText(/Guest/i);
  await expect(page.locator("#saved-auth-status")).toContainText(DESKTOP_USER_NAME);
  await expect(page.locator("#add-custom-job-btn")).toBeEnabled();
});

test("Saved core", async ({ page }) => {
  await signInOnSaved(page);
  await page.click("#add-custom-job-btn");
  await page.fill("#custom-job-title", "Packaged Smoke QA");
  await page.fill("#custom-job-company", "Baluffo QA");
  await page.click("#custom-job-save-btn");

  const expandToggle = page.locator(".details-toggle-btn").first();
  await expect(expandToggle).toBeVisible();
  await expandToggle.click();

  const notesInput = page.locator(".job-notes-input").first();
  await notesInput.fill("Packaged smoke note persistence");
  await expect(notesInput).toHaveValue("Packaged smoke note persistence");
  await page.waitForTimeout(1800);

  await page.reload();
  await waitForDesktopAdapter(page);
  await expect(page.locator("#saved-auth-status")).not.toContainText(/Guest/i);
  await page.locator(".details-toggle-btn").first().click();
  await expect(page.locator(".job-notes-input").first()).toHaveValue("Packaged smoke note persistence");
});

test("Admin access", async ({ page }) => {
  await gotoDesktop(page, "admin.html");
  await expect(page.locator("#admin-pin-gate")).toBeVisible();
  await expect(page.locator("#admin-unlock-btn")).toBeEnabled();
  await page.fill("#admin-pin-input", "1234");
  await page.click("#admin-unlock-btn");
  await expect(page.locator("#admin-content")).toBeVisible();
  await expect(page.locator("#admin-source-status")).toContainText(/Admin access granted|Stored Profiles Overview|Loading|Loaded \d+ user profiles/i);
});
