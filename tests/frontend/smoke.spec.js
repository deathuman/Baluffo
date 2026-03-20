import { test, expect } from "@playwright/test";

async function expectJobsPageReady(page) {
  await page.waitForFunction(() => {
    const state = document.body?.getAttribute("data-jobs-startup-state") || "loading";
    return state === "interactive" || state === "error";
  }, null, { timeout: 15000 });
  await expect(page.locator("body")).not.toHaveAttribute("data-jobs-startup-state", "loading");
  await expect(page.locator("#refresh-jobs-btn")).toBeEnabled();
  await expect(page.locator("#auth-sign-in-btn")).toBeEnabled();
  await expect(page.locator("#jobs-list")).not.toContainText(/Loading jobs/i);
}

async function signInWithProfile(page, buttonSelector, profileName, expectedFocusSelector) {
  await page.click(buttonSelector);
  await page.waitForTimeout(1000);

  const profileInput = page.locator("#local-auth-name-input");
  await expect(profileInput).toBeVisible();
  await profileInput.fill(profileName);
  await profileInput.press("Enter");
  await expect(profileInput).toBeHidden();
  if (expectedFocusSelector) {
    await expect(page.locator(expectedFocusSelector)).toBeFocused();
  }
}

async function cancelSignIn(page) {
  const cancelBtn = page.locator("#local-auth-cancel-btn");
  await expect(cancelBtn).toBeVisible();
  await cancelBtn.click();
  await expect(page.locator("#local-auth-name-input")).toBeHidden();
}

test("index compatibility entry redirects to jobs", async ({ page }) => {
  await page.goto("/index.html");
  await page.waitForURL("**/jobs.html");
  await expect(page.locator("#jobs-list")).toBeVisible();
});

test("jobs smoke: filters + refresh + pagination + save/unsave + guest warning", async ({ page }) => {
  const pageErrors = [];
  const consoleErrors = [];
  page.on("pageerror", error => pageErrors.push(String(error?.message || error)));
  page.on("console", msg => {
    if (msg.type() === "error") {
      consoleErrors.push(msg.text());
    }
  });

  await page.goto("/jobs.html");

  await expect(page.locator("#jobs-list")).toBeVisible();
  await expectJobsPageReady(page);
  await expect(pageErrors).toEqual([]);
  expect(consoleErrors.join("\n")).not.toMatch(/Error initializing jobs|Jobs page boot failed|\[jobs\].*failed/i);
  await page.selectOption("#work-type-filter", "Remote");
  await page.click("#refresh-jobs-btn");
  await expect(page.locator("#source-status")).toHaveText(/Fetching|Loaded|Could not/i);

  const pageButtons = page.locator("#pagination .page-btn");
  const count = await pageButtons.count();
  if (count > 1) {
    await pageButtons.nth(1).click();
  }

  await signInWithProfile(page, "#auth-sign-in-btn", "Smoke User", "#saved-jobs-btn");
  await expect(page.locator("#saved-jobs-btn")).toBeVisible();

  const saveBtn = page.locator(".save-job-btn").first();
  await expect(saveBtn).toBeVisible();
  await saveBtn.click();
  await expect(saveBtn).toHaveClass(/saved/);
  await saveBtn.click();
  await expect(saveBtn).not.toHaveClass(/saved/);

  await page.click("#auth-sign-out-btn");
  await saveBtn.click();
  await expect(page.locator(".toast").last()).toContainText("Sign in to save jobs");
  await cancelSignIn(page);

  await page.locator(".jobs-sources summary").click();
  await expect(page.locator("#data-sources-list")).toContainText("Google Sheets");
});

test("saved smoke: export stays available for signed-in browser users and guest state restores", async ({ page }) => {
  await page.goto("/saved.html");

  await signInWithProfile(page, "#saved-auth-sign-in-btn", "Smoke User", "#add-custom-job-btn");
  await expect(page.locator("#saved-auth-status")).not.toContainText(/Guest/i);
  await page.locator("#saved-utilities summary").click();
  await expect(page.locator("#export-backup-btn")).toBeEnabled();

  await expect(page.locator("#export-backup-btn")).toBeVisible();
  const downloadPromise = page.waitForEvent("download");
  await page.click("#export-backup-btn");
  const download = await downloadPromise;
  await expect(download.suggestedFilename()).toContain("baluffo-backup-");

  await page.click("#saved-auth-sign-out-btn");
  await expect(page.locator("#saved-source-status")).toContainText("Sign in to view your saved jobs");
});

test("admin smoke: loads directly without a PIN gate", async ({ page }) => {
  await page.goto("/admin.html");
  await expect(page.locator("#admin-content")).toBeVisible();
  await expect(page.locator("#admin-source-status")).toContainText(/Loading|Loaded|users|Could not|Admin overview/i);
});

test("admin smoke: direct admin load shows bucketed fetch failure summary", async ({ page }) => {
  await page.goto("/admin.html");
  await expect(page.locator("#admin-content")).toBeVisible();

  const metrics = page.locator("#admin-ops-fetcher-metrics");
  await page.waitForFunction(() => {
    const text = document.querySelector("#admin-ops-fetcher-metrics")?.textContent || "";
    return /Source Failures/i.test(text);
  }, null, { timeout: 15000 });
  await expect(metrics).toContainText(/Source Failures/i);

  const metricsText = String(await metrics.textContent() || "");
  if (!/Source Failures\s*0\s*\//i.test(metricsText)) {
    await expect(metrics).toContainText(/Top-level failed sources/i);
    await expect(metrics).toContainText(/Grouped detail failures/i);
    await expect(metrics).toContainText(/Failure buckets/i);
    await expect(metrics).toContainText(/Extract Zero|Blocked\/Challenge|Timeout|Provider Rate Limited|Provider Bad Config|Uncategorized/i);
  }
});

