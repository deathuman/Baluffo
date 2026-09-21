/**
 * Guard: browser-backed unit tests must not leak a listening server on launch failure.
 *
 * The two hydration smoke tests start an HTTP server and then launch Playwright
 * Chromium. When the launch sits outside the `try`/`finally`, a launch error (for
 * example a missing Playwright browser cache entry in CI) leaves the server listening,
 * which keeps the Node test runner alive after the assertion fails. That turned a fast
 * red test into a 58-minute hang during the v0.2.152 release recovery.
 */

import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const BROWSER_BACKED_SMOKE_TESTS = [
  "tests/frontend/unit/admin-authoritative-hydration-smoke.test.mjs",
  "tests/frontend/unit/admin-schedule-partial-hydration-smoke.test.mjs"
];

function readTest(relativePath) {
  return fs.readFileSync(path.join(REPO_ROOT, relativePath), "utf8");
}

function indexOfLaunch(source) {
  return source.indexOf("chromium.launch");
}

function enclosingTryIndex(source, launchIndex) {
  // The `try {` that guards the launch is the nearest one before it.
  return source.lastIndexOf("try {", launchIndex);
}

for (const relativePath of BROWSER_BACKED_SMOKE_TESTS) {
  test(`${path.basename(relativePath)} launches Chromium inside try/finally`, () => {
    const source = readTest(relativePath);
    const launchIndex = indexOfLaunch(source);
    assert.notEqual(launchIndex, -1, `${relativePath} should launch Chromium`);

    const tryIndex = enclosingTryIndex(source, launchIndex);
    assert.notEqual(tryIndex, -1, `${relativePath} should guard Chromium launch with try/finally`);

    assert.ok(
      launchIndex > tryIndex,
      `${relativePath} must launch Chromium inside the try block. A launch failure outside ` +
        "try/finally leaks the listening smoke server and hangs the test runner instead of failing fast."
    );

    const guardedRegion = source.slice(tryIndex, launchIndex);
    assert.doesNotMatch(
      guardedRegion,
      /finally\s*\{/,
      `${relativePath} should not close the try block before launching Chromium`
    );
  });

  test(`${path.basename(relativePath)} closes a possibly-unlaunched browser`, () => {
    const source = readTest(relativePath);
    assert.match(
      source,
      /if \(browser\)[\s\S]{0,30}?await browser\.close\(\)/,
      `${relativePath} must null-check the browser in finally so a failed launch still ` +
        "closes the smoke server"
    );
  });
}
