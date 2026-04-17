import { test, expect } from "@playwright/test";

const DESKTOP_RUNTIME_QUERY = "?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1";

async function expectJobsPageReady(page, timeout = 90000) {
  await page.waitForFunction(() => {
    const state = document.body?.getAttribute("data-jobs-startup-state") || "loading";
    return state === "interactive" || state === "error";
  }, null, { timeout });
  await expect(page.locator("body")).not.toHaveAttribute("data-jobs-startup-state", "loading");
  await expect(page.locator("#refresh-jobs-btn")).toBeEnabled();
  await expect(page.locator("#auth-sign-in-btn")).toBeEnabled();
  await expect(page.locator("#jobs-list")).not.toContainText(/Loading jobs/i);
  await expect(page.locator("#source-status")).toContainText(/^Loaded \d[\d,]* jobs/i, { timeout });
}

async function expectDesktopUpdateToggleUsable(page, timeout = 15000) {
  const updateToggle = page.locator("#desktop-update-toggle-btn");
  await expect(updateToggle).toBeVisible({ timeout });
  await expect(updateToggle).toBeEnabled({ timeout });
  await updateToggle.click();
  await expect(page.locator("#desktop-update-panel")).toBeVisible({ timeout });
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

test("index entry redirects to jobs", async ({ page }) => {
  await page.goto("/index.html");
  await page.waitForURL("**/jobs.html");
  await expect(page.locator("#jobs-list")).toBeVisible();
});

test("jobs smoke: filters + refresh + pagination + save/unsave + guest warning", async ({ page }) => {
  test.setTimeout(120000);
  const pageErrors = [];
  page.on("pageerror", error => {
    const msg = String(error?.stack || error?.message || error);
    pageErrors.push(msg);
    console.error("[pageerror]", msg);
  });
  page.on("console", msg => {
    if (msg.type() === "error") {
      const location = msg.location();
      const where = location?.url
        ? `${location.url}:${location.lineNumber || 0}:${location.columnNumber || 0}`
        : "unknown";
      console.error(`[page:error] ${where} ${msg.text()}`);
    }
  });
  await page.addInitScript(() => {
    window.__baluffoSmokeErrors = [];
    window.onerror = (message, source, line, column, error) => {
      window.__baluffoSmokeErrors.push({
        message: String(message || ""),
        source: String(source || ""),
        line: Number(line || 0),
        column: Number(column || 0),
        stack: String(error?.stack || ""),
      });
    };
  });

  await page.goto("/jobs.html");

  // Diagnostic: poll startup state and log progress until resolved or timeout
  const startupState = await page.waitForFunction(() => {
    const state = document.body?.getAttribute("data-jobs-startup-state") || "loading";
    if (state !== "loading") return state;
    return false;
  }, null, { timeout: 90000 }).catch(async () => {
    const state = await page.evaluate(() => document.body?.getAttribute("data-jobs-startup-state") || "missing");
    const smokeErrors = await page.evaluate(() => window.__baluffoSmokeErrors || []);
    console.error(`[startup] state at timeout: ${state}`);
    throw new Error(
      `Jobs startup state never left 'loading'. Final state: ${state}. ` +
      `Page errors: ${JSON.stringify(pageErrors)}. ` +
      `Window errors: ${JSON.stringify(smokeErrors)}`
    );
  });
  console.log(`[startup] resolved to: ${startupState}`);

  await expectJobsPageReady(page);

  // Verify admin bridge is running by checking the admin button state
  const adminBtn = page.locator("#admin-page-btn");
  await expect(adminBtn).toHaveAttribute("data-bridge-state", "online", { timeout: 10000 });
  await expect(adminBtn).not.toBeDisabled();
  await expect(adminBtn).not.toContainText("Admin Checking...");

  await expect(pageErrors).toEqual([]);
  await page.selectOption("#work-type-filter", "Remote");
  await expect(page.locator(".save-job-btn").first()).toBeVisible({ timeout: 20000 });

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

  await page.click("#refresh-jobs-btn");
  await expect(page.locator("#refresh-jobs-btn")).toBeDisabled({ timeout: 10000 });
  await expect(page.locator("#source-status")).toContainText(/Fetching/i, { timeout: 10000 });

  await page.locator(".jobs-sources summary").click();
  await expect(page.locator("#data-sources-list")).toContainText("Google Sheets");
});

test("jobs filter popups close on outside click and Escape", async ({ page }) => {
  await page.goto("/jobs.html");
  await expectJobsPageReady(page);

  const countryPickerBtn = page.locator("#country-picker-btn");
  const countryPickerPanel = page.locator("#country-picker-panel");
  const countryPickerSearch = page.locator("#country-picker-search");
  const pageHeading = page.locator("h1");
  const quickFiltersBtn = page.locator("#customize-quick-filters-btn");
  const quickFiltersPanel = page.locator("#quick-filters-panel");

  await countryPickerBtn.click();
  await expect(countryPickerPanel).toBeVisible();
  await pageHeading.click();
  await expect(countryPickerPanel).toBeHidden();

  await countryPickerBtn.click();
  await expect(countryPickerPanel).toBeVisible();
  await expect(countryPickerSearch).toBeFocused();
  await countryPickerSearch.press("Escape");
  await expect(countryPickerPanel).toBeHidden();

  await quickFiltersBtn.click();
  await expect(quickFiltersPanel).toBeVisible();
  await pageHeading.click();
  await expect(quickFiltersPanel).toBeHidden();

  await quickFiltersBtn.click();
  await expect(quickFiltersPanel).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(quickFiltersPanel).toBeHidden();
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

test("jobs admin badge reaches online state after navigating back from saved", async ({ page }) => {
  await page.goto("/saved.html");
  await expect(page.locator("#jobs-page-btn")).toBeVisible();

  await page.click("#jobs-page-btn");
  await page.waitForURL("**/jobs.html");
  await expectJobsPageReady(page);

  const adminBtn = page.locator("#admin-page-btn");
  await expect(adminBtn).toHaveAttribute("data-bridge-state", "online", { timeout: 10000 });
  await expect(adminBtn).not.toBeDisabled();
  await expect(adminBtn).not.toContainText("Admin Checking...");
});

test("desktop jobs update toggle stays usable after Jobs to Saved to Jobs navigation", async ({ page }) => {
  await page.goto(`/jobs.html${DESKTOP_RUNTIME_QUERY}`);
  await expectJobsPageReady(page);
  await expectDesktopUpdateToggleUsable(page);

  await signInWithProfile(page, "#auth-sign-in-btn", "Desktop Smoke User", "#saved-jobs-btn");
  await page.click("#saved-jobs-btn");
  await page.waitForURL("**/saved.html**");
  await expect(page.locator("#jobs-page-btn")).toBeVisible();

  await page.click("#jobs-page-btn");
  await page.waitForURL("**/jobs.html**");
  await expectJobsPageReady(page);
  await expectDesktopUpdateToggleUsable(page);
});

test("desktop jobs update toggle stays usable after Jobs to Admin to Jobs navigation", async ({ page }) => {
  await page.goto(`/jobs.html${DESKTOP_RUNTIME_QUERY}`);
  await expectJobsPageReady(page);
  await expectDesktopUpdateToggleUsable(page);

  const adminBtn = page.locator("#admin-page-btn");
  await expect(adminBtn).toHaveAttribute("data-bridge-state", "online", { timeout: 10000 });
  await adminBtn.click();
  await page.waitForURL("**/admin.html**");
  await expect(page.locator("#admin-jobs-btn")).toBeVisible();

  await page.click("#admin-jobs-btn");
  await page.waitForURL("**/jobs.html**");
  await expectJobsPageReady(page);
  await expectDesktopUpdateToggleUsable(page);
});

test("admin smoke: loads directly without a PIN gate", async ({ page }) => {
  await page.goto("/admin.html");
  await expect(page.locator("#admin-content")).toBeVisible();
  await expect(page.locator("#admin-source-status")).toContainText(/Loading|Loaded|users|Could not|Admin overview/i);
});

test("admin smoke: direct admin load shows bucketed fetch failure summary", async ({ page }) => {
  await page.goto("/admin.html");
  await expect(page.locator("#admin-content")).toBeVisible();
  await expect(page.locator("h1")).toContainText(/Administration/i);

  // Load the fetch report - requires bridge to be running
  await page.click("#admin-refresh-report-btn");
  await expect(page.locator("#admin-ops-fetcher-metrics")).not.toContainText(/Loading/i, { timeout: 15000 });
});
