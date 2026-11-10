import assert from "node:assert/strict";
import test from "node:test";

import {
  prepareWarmState,
  resolveWarmSeedFeedUrl
} from "../../../scripts/measure_container_jobs_boot.mjs";

test("warm seed resolves the fixture feed against the server origin", () => {
  assert.equal(
    resolveWarmSeedFeedUrl("http://127.0.0.1:8877/"),
    "http://127.0.0.1:8877/data/jobs-unified-light.json"
  );
  assert.equal(
    resolveWarmSeedFeedUrl("http://127.0.0.1:8877"),
    "http://127.0.0.1:8877/data/jobs-unified-light.json"
  );
});

test("warm state primes one boot, then seeds a page that runs no Jobs code", async () => {
  const calls = [];
  const page = {
    async goto(url, options) {
      calls.push({ goto: url, waitUntil: options?.waitUntil });
    },
    async waitForFunction() {
      calls.push({ waitForFunction: true });
      return 40;
    }
  };

  const seededRowCount = await prepareWarmState(page, {
    baseUrl: "http://127.0.0.1:8877/",
    seed: async (targetPage, options) => {
      calls.push({ seed: options.feedUrl, samePage: targetPage === page });
      return 5000;
    }
  });

  assert.equal(seededRowCount, 5000);
  assert.deepEqual(calls, [
    { goto: "http://127.0.0.1:8877/jobs.html", waitUntil: "domcontentloaded" },
    { waitForFunction: true },
    { goto: "http://127.0.0.1:8877/styles/base.css", waitUntil: undefined },
    {
      seed: "http://127.0.0.1:8877/data/jobs-unified-light.json",
      samePage: true
    }
  ]);
});
