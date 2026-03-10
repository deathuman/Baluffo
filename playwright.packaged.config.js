import { defineConfig, devices } from "@playwright/test";

const artifactsDir = process.env.PACKAGED_SMOKE_ARTIFACTS_DIR || ".codex-tmp/packaged-desktop-smoke/playwright-output";
const reportPath = process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT || ".codex-tmp/packaged-desktop-smoke/playwright-report.json";
const headed = process.env.PACKAGED_SMOKE_HEADED === "1";

export default defineConfig({
  testDir: "./tests/frontend",
  testMatch: ["**/packaged-desktop.spec.js"],
  timeout: 45_000,
  expect: {
    timeout: 12_000
  },
  fullyParallel: false,
  retries: 0,
  workers: 1,
  reporter: [
    ["list"],
    ["json", { outputFile: reportPath }]
  ],
  outputDir: artifactsDir,
  use: {
    baseURL: process.env.PACKAGED_DESKTOP_BASE_URL || "http://127.0.0.1:8080",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    headless: !headed
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] }
    }
  ]
});
