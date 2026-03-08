import { test, expect } from "@playwright/test";

async function stubPrompt(page, value = "Smoke User") {
  await page.addInitScript(name => {
    window.prompt = () => name;
  }, value);
}

test("jobs smoke: filters + refresh + pagination + save/unsave + guest warning", async ({ page }) => {
  await stubPrompt(page);
  await page.goto("/jobs.html");

  await expect(page.locator("#jobs-list")).toBeVisible();
  await page.selectOption("#work-type-filter", "Remote");
  await page.click("#refresh-jobs-btn");

  const pageButtons = page.locator("#pagination .page-btn");
  const count = await pageButtons.count();
  if (count > 1) {
    await pageButtons.nth(1).click();
  }

  await page.click("#auth-sign-in-btn");
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
});

test("saved smoke: sign-in + custom job + notes autosave + export + guest warning", async ({ page }) => {
  await stubPrompt(page);
  await page.goto("/saved.html");

  await page.click("#saved-auth-sign-in-btn");
  await expect(page.locator("#export-backup-btn")).toBeEnabled();

  await page.click("#add-custom-job-btn");
  await page.fill("#custom-job-title", "Smoke QA Engineer");
  await page.fill("#custom-job-company", "Baluffo Labs");
  await page.click("#custom-job-save-btn");

  const expandToggle = page.locator(".details-toggle-btn").first();
  await expect(expandToggle).toBeVisible();
  await expandToggle.click();

  const notesInput = page.locator(".job-notes-input").first();
  await notesInput.fill("Smoke autosave note");
  await expect(notesInput).toHaveValue("Smoke autosave note");

  const downloadPromise = page.waitForEvent("download");
  await page.click("#export-backup-btn");
  const download = await downloadPromise;
  await expect(download.suggestedFilename()).toContain("baluffo-backup-");

  await page.click("#saved-auth-sign-out-btn");
  await expect(page.locator("#add-custom-job-btn")).toBeDisabled();
  await expect(page.locator("#saved-source-status")).toContainText("Sign in to view your saved jobs");
});

test("admin smoke: unlock/lock + refresh + discovery unavailable negative", async ({ page }) => {
  await page.goto("/admin.html");

  await page.fill("#admin-pin-input", "9999");
  await page.click("#admin-unlock-btn");
  await expect(page.locator(".toast").last()).toContainText("Invalid admin PIN");

  await page.fill("#admin-pin-input", "1234");
  await page.click("#admin-unlock-btn");
  await expect(page.locator("#admin-content")).toBeVisible();

  await page.click("#admin-refresh-btn");
  await page.click("#admin-load-discovery-btn");
  await expect(page.locator("#admin-discovery-summary")).toContainText(/Found|bridge unavailable/i);

  await page.click("#admin-lock-btn");
  await expect(page.locator("#admin-pin-gate")).toBeVisible();
});

