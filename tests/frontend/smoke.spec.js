import { test, expect } from "@playwright/test";

async function signInWithProfile(page, buttonSelector, profileName, expectedFocusSelector) {
  await page.click(buttonSelector);
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
  await page.goto("/jobs.html");

  await expect(page.locator("#jobs-list")).toBeVisible();
  await expect(page.locator("#source-status")).toHaveText(/./);
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

test("admin smoke: invalid pin keeps browser gate locked", async ({ page }) => {
  await page.goto("/admin.html");

  await page.fill("#admin-pin-input", "9999");
  await page.click("#admin-unlock-btn");
  await expect(page.locator(".toast").last()).toContainText("Invalid admin PIN");
  await expect(page.locator("#admin-pin-gate")).toBeVisible();
  await expect(page.locator("#admin-content")).toBeHidden();
});

