import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/frontend",
  testMatch: ["**/smoke.spec.js"],
  testIgnore: ["**/unit/**"],
  timeout: 30_000,
  expect: {
    timeout: 7_500
  },
  fullyParallel: false,
  retries: 0,
  workers: 1,
  reporter: [["list"]],
  use: {
    baseURL: "http://127.0.0.1:4173",
    trace: "on-first-retry",
    headless: true
  },
  webServer: {
    command: "py -3.13 -m http.server 4173 --directory .",
    url: "http://127.0.0.1:4173/jobs.html",
    timeout: 20_000,
    reuseExistingServer: !process.env.CI
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] }
    }
  ]
});
