import test from "node:test";
import assert from "node:assert/strict";
import { readLastJobsUrlFromSession } from "../../../frontend/shared/last-jobs-url.js";
import {
  loadSavedListPreferences,
  persistSavedListPreferences,
  readSavedLastJobsUrl
} from "../../../frontend/saved/state-sync/index.js";
import { readAdminLastJobsUrl } from "../../../frontend/admin/state-sync/index.js";
import { createStorageMock } from "./helpers/browser-test-helpers.mjs";

function makeReader(value) {
  return () => value;
}

test("last jobs URL reader accepts only Jobs page paths", () => {
  assert.equal(
    readLastJobsUrlFromSession(makeReader("jobs.html?page=2&search=engineer"), "last", "jobs.html"),
    "jobs.html?page=2&search=engineer"
  );
  assert.equal(
    readLastJobsUrlFromSession(makeReader("/jobs.html?desktop=1"), "last", "jobs.html"),
    "/jobs.html?desktop=1"
  );
  assert.equal(
    readLastJobsUrlFromSession(makeReader("/saved.html?filter=all"), "last", "jobs.html"),
    "jobs.html"
  );
  assert.equal(
    readLastJobsUrlFromSession(makeReader("admin.html?panel=sources"), "last", "jobs.html"),
    "jobs.html"
  );
  assert.equal(
    readLastJobsUrlFromSession(makeReader(""), "last", "jobs.html"),
    "jobs.html"
  );
});

test("Saved and Admin last-Jobs URL readers reject non-Jobs URLs", () => {
  const previousSessionStorage = global.sessionStorage;
  global.sessionStorage = createStorageMock({
    last_jobs: "/saved.html?filter=all"
  });
  try {
    assert.equal(readSavedLastJobsUrl("last_jobs", "jobs.html"), "jobs.html");
    assert.equal(readAdminLastJobsUrl("last_jobs", "jobs.html"), "jobs.html");
  } finally {
    global.sessionStorage = previousSessionStorage;
  }
});

test("saved list preferences persist group mode per profile and normalize stale values", () => {
  const previousLocalStorage = global.localStorage;
  global.localStorage = createStorageMock();
  const normalizeGroup = value => (value === "stage" ? "stage" : "none");

  try {
    assert.deepEqual(
      loadSavedListPreferences("saved_list", "u1", normalizeGroup, "none"),
      { group: "none" }
    );
    assert.equal(
      persistSavedListPreferences("saved_list", "u1", normalizeGroup, { group: "stage" }),
      true
    );
    assert.deepEqual(
      loadSavedListPreferences("saved_list", "u1", normalizeGroup, "none"),
      { group: "stage" }
    );

    global.localStorage.setItem("saved_list:u1", JSON.stringify({ group: "stale" }));
    assert.deepEqual(
      loadSavedListPreferences("saved_list", "u1", normalizeGroup, "none"),
      { group: "none" }
    );
    assert.deepEqual(
      loadSavedListPreferences("saved_list", "u2", normalizeGroup, "none"),
      { group: "none" }
    );
  } finally {
    global.localStorage = previousLocalStorage;
  }
});
