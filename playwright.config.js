import { defineConfig, devices } from "@playwright/test";

function resolvePlaywrightPythonCommand() {
  if (process.env.PLAYWRIGHT_PYTHON) {
    return process.env.PLAYWRIGHT_PYTHON;
  }
  if (process.platform === "win32") {
    return "python";
  }
  return "python3";
}

const playwrightPython = resolvePlaywrightPythonCommand();

export default defineConfig({
  testDir: "./tests/frontend",
  testMatch: ["**/smoke.spec.js"],
  testIgnore: ["**/unit/**"],
  outputDir: ".tmp/playwright/test-results",
  timeout: 30_000,
  expect: {
    timeout: 7_500
  },
  fullyParallel: false,
  retries: 0,
  workers: 1,
  reporter: [["line"]],
  use: {
    baseURL: "http://127.0.0.1:4173",
    trace: process.env.BALUFFO_PLAYWRIGHT_TRACE === "1" ? "retain-on-failure" : "off",
    headless: true
  },
  webServer: {
    command: `${playwrightPython} scripts/serve_static_site.py --port 4173 --directory .`,
    url: "http://127.0.0.1:4173/jobs.html",
    timeout: 20_000,
    reuseExistingServer: !process.env.CI
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] }
    }
  ],
  globalSetup: "./tests/frontend/global-setup.js",
  globalTeardown: "./tests/frontend/global-teardown.js"
});
