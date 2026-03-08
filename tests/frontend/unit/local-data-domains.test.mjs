import test from "node:test";
import assert from "node:assert/strict";
import { createAuthDomain } from "../../../frontend/local-data/auth.js";
import { createSavedJobsDomain } from "../../../frontend/local-data/saved-jobs.js";

function createStorageMock() {
  const map = new Map();
  return {
    getItem(key) {
      return map.has(key) ? map.get(key) : null;
    },
    setItem(key, value) {
      map.set(String(key), String(value));
    },
    removeItem(key) {
      map.delete(String(key));
    }
  };
}

test("auth domain signIn/signOut updates session and emits auth changes", async () => {
  const listeners = new Set();
  const profiles = [];
  let currentUser = null;
  const localStorage = createStorageMock();
  let promptCalls = 0;
  global.localStorage = localStorage;
  global.window = {
    prompt: () => {
      promptCalls += 1;
      return "Andrea";
    },
    addEventListener: () => {}
  };

  const authDomain = createAuthDomain({
    listeners,
    getCurrentUser: () => currentUser,
    setCurrentUser: value => {
      currentUser = value;
    },
    makeUser: profile => ({ uid: profile.id, displayName: profile.name, email: profile.email || "" }),
    readProfiles: () => profiles.slice(),
    writeProfiles: next => {
      profiles.length = 0;
      profiles.push(...next);
    },
    hashFNV1a: () => "abcd1234",
    sessionKey: "session_key"
  });

  const observed = [];
  const unsub = authDomain.onAuthStateChanged(user => {
    observed.push(user ? user.uid : "");
  });

  const result = await authDomain.signIn();
  assert.equal(promptCalls, 1);
  assert.equal(result.user.uid, "local_andrea");
  assert.equal(localStorage.getItem("session_key"), "local_andrea");
  assert.equal(profiles.length, 1);
  assert.equal(observed.at(-1), "local_andrea");

  await authDomain.signOut();
  assert.equal(localStorage.getItem("session_key"), null);
  assert.equal(observed.at(-1), "");
  unsub();
});

test("saved-jobs domain normalizes bookmark timestamp and merge keeps richer existing row", () => {
  const savedJobsDomain = createSavedJobsDomain({
    withStore: async () => {
      throw new Error("withStore not expected");
    },
    listSavedJobs: async () => [],
    ensureCurrentUser: () => ({ uid: "u1" }),
    notifySavedJobsChanged: async () => {},
    addActivityLog: async () => {},
    generateJobKey: input => String(input?.jobKey || "job_x"),
    normalizeApplicationStatus: status => String(status || "bookmark"),
    canTransitionPhase: () => true,
    normalizeSectorValue: value => String(value || "Tech"),
    normalizeCustomSourceLabel: value => String(value || "Personal"),
    sanitizeJobUrl: value => String(value || ""),
    nowIso: () => "2026-03-08T12:00:00.000Z",
    normalizeIsoOrNow: (value, fallback = "") => String(value || fallback),
    toPlainObject: value => (value && typeof value === "object" && !Array.isArray(value) ? value : {}),
    isClearlyLowerQualityImported: () => true
  });

  const normalized = savedJobsDomain.normalizeSavedJobRecord("u1", {
    jobKey: "job_1",
    title: "Role",
    company: "Studio"
  });
  assert.equal(normalized.phaseTimestamps.bookmark, "2026-03-08T12:00:00.000Z");
  assert.equal(normalized.jobKey, "job_1");
  assert.equal(normalized.profileId, "u1");

  const existing = savedJobsDomain.normalizeSavedJobRecord("u1", {
    jobKey: "job_1",
    title: "Senior Role",
    company: "Studio"
  });
  const merged = savedJobsDomain.mergeSavedJobRows("u1", existing, {
    jobKey: "job_1",
    title: "",
    company: ""
  });
  assert.equal(merged.title, existing.title);
  assert.equal(merged.company, existing.company);
});
