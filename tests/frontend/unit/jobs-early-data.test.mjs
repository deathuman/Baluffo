import test from "node:test";
import assert from "node:assert/strict";
import { importFresh } from "./helpers/browser-test-helpers.mjs";

const EARLY_DATA_URL = "../../../frontend/jobs/early-data.js";
const DESKTOP_HREF = "http://127.0.0.1:8080/jobs.html?desktop=1&bridgePort=8877";
const BROWSER_HREF = "http://127.0.0.1:8080/jobs.html";

function installGlobals({ href, fetchImpl, dataset }) {
  const previous = {
    document: globalThis.document,
    location: globalThis.location,
    fetch: globalThis.fetch,
    early: globalThis.__baluffoJobsEarlyData
  };
  globalThis.document = { currentScript: { dataset: { ...dataset } } };
  globalThis.location = { href };
  globalThis.fetch = fetchImpl;
  delete globalThis.__baluffoJobsEarlyData;
  return () => {
    globalThis.document = previous.document;
    globalThis.location = previous.location;
    globalThis.fetch = previous.fetch;
    if (previous.early === undefined) delete globalThis.__baluffoJobsEarlyData;
    else globalThis.__baluffoJobsEarlyData = previous.early;
  };
}

const DATASET = {
  startupSnapshotUrl: "data/jobs-unified-startup.json",
  fetchReportUrl: "data/jobs-fetch-report.json"
};

test("jobs early data hoists both first-render requests on a desktop page and hands each out once", async () => {
  const calls = [];
  const restore = installGlobals({
    href: DESKTOP_HREF,
    dataset: DATASET,
    fetchImpl: (url) => {
      calls.push(url);
      return Promise.resolve({ ok: true, status: 200, url });
    }
  });
  try {
    await importFresh(EARLY_DATA_URL, { relativeTo: import.meta.url });
    const hoisted = globalThis.__baluffoJobsEarlyData;
    assert.deepEqual([...hoisted.urls], ["data/jobs-unified-startup.json", "data/jobs-fetch-report.json"]);
    assert.deepEqual(calls, ["data/jobs-unified-startup.json", "data/jobs-fetch-report.json"]);

    const snapshot = await hoisted.take("data/jobs-unified-startup.json");
    assert.equal(snapshot.status, 200);
    assert.equal(hoisted.take("data/jobs-unified-startup.json"), null);
    assert.ok(hoisted.take("data/jobs-fetch-report.json"));
    assert.equal(hoisted.take("data/jobs-availability-history.json"), null);
  } finally {
    restore();
  }
});

test("jobs early data skips the report probe outside the desktop app", async () => {
  const calls = [];
  const restore = installGlobals({
    href: BROWSER_HREF,
    dataset: DATASET,
    fetchImpl: (url) => {
      calls.push(url);
      return Promise.resolve({ ok: true, status: 200 });
    }
  });
  try {
    await importFresh(EARLY_DATA_URL, { relativeTo: import.meta.url });
    assert.deepEqual([...globalThis.__baluffoJobsEarlyData.urls], ["data/jobs-unified-startup.json"]);
    assert.deepEqual(calls, ["data/jobs-unified-startup.json"]);
  } finally {
    restore();
  }
});

test("jobs early data resolves a failed hoist to null so the caller refetches", async () => {
  const restore = installGlobals({
    href: DESKTOP_HREF,
    dataset: DATASET,
    fetchImpl: () => Promise.reject(new Error("site not up yet"))
  });
  try {
    await importFresh(EARLY_DATA_URL, { relativeTo: import.meta.url });
    const hoisted = globalThis.__baluffoJobsEarlyData;
    assert.equal(await hoisted.take("data/jobs-fetch-report.json"), null);
    assert.equal(await hoisted.take("data/jobs-unified-startup.json"), null);
  } finally {
    restore();
  }
});
