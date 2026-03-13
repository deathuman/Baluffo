import test from "node:test";
import assert from "node:assert/strict";
import {
  detectWorkType,
  detectContractType,
  classifyCompanyType,
  mapProfession,
  normalizeJobs,
  getJobKeyForJob,
  deriveFreshness,
  mapFreshnessAgeToScore
} from "../../../frontend/jobs/domain.js";

test("jobs domain detects work type and contract", () => {
  assert.equal(detectWorkType("fully remote"), "Remote");
  assert.equal(detectWorkType("hybrid"), "Hybrid");
  assert.equal(detectWorkType("office"), "Onsite");
  assert.equal(detectContractType("internship"), "Internship");
  assert.equal(detectContractType("full time"), "Full-time");
  assert.equal(detectContractType("fixed term"), "Temporary");
});

test("jobs domain classifies company and normalizes jobs", () => {
  assert.equal(classifyCompanyType("Some Game Studio", ""), "Game");
  const rows = normalizeJobs([{ title: "Gameplay Engineer", company: "Foo", workType: "remote" }], {
    professionLabels: {},
    sanitizeUrl: value => value
  });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].workType, "Remote");
  assert.equal(rows[0].companyType, "Game");
});

test("jobs domain maps technical director title synonyms", () => {
  assert.equal(mapProfession("Technical Director"), "technical-director");
  assert.equal(mapProfession("Associate Technical Director"), "technical-director");
  assert.equal(mapProfession("Senior Animation TD"), "technical-director");
  assert.equal(mapProfession("Pipeline TD"), "technical-director");
  assert.equal(mapProfession("TDengine Programmer"), "engine");

  const rows = normalizeJobs([{ title: "Technical Director - Tools", company: "Studio" }], {
    professionLabels: { "technical-director": "Technical Director" },
    sanitizeUrl: value => value
  });
  assert.equal(rows[0].profession, "technical-director");
});

test("jobs domain generates fallback key", () => {
  const key = getJobKeyForJob({ title: "A", company: "B", city: "C", country: "D" }, {});
  assert.match(key, /^job_/);
});

test("jobs domain maps freshness ages to expected score bands", () => {
  assert.ok(mapFreshnessAgeToScore(0) >= 0 && mapFreshnessAgeToScore(0) <= 15);
  assert.ok(mapFreshnessAgeToScore(5) >= 16 && mapFreshnessAgeToScore(5) <= 40);
  assert.ok(mapFreshnessAgeToScore(12) >= 41 && mapFreshnessAgeToScore(12) <= 70);
  assert.ok(mapFreshnessAgeToScore(45) >= 71 && mapFreshnessAgeToScore(45) <= 100);
});

test("jobs domain derives freshness from postedAt first, then fetchedAt fallback", () => {
  const nowMs = Date.parse("2026-03-08T00:00:00.000Z");
  const posted = deriveFreshness({ postedAt: "2026-03-06T00:00:00.000Z" }, { nowMs });
  assert.equal(posted.freshnessSource, "postedAt");
  assert.equal(posted.freshnessAgeDays, 2);
  assert.ok(posted.freshnessScore >= 16 && posted.freshnessScore <= 40);

  const fetched = deriveFreshness({
    postedAt: "",
    fetchedAt: "2026-03-01T00:00:00.000Z"
  }, { nowMs });
  assert.equal(fetched.freshnessSource, "fetchedAt");
  assert.equal(fetched.freshnessAgeDays, 7);
  assert.ok(fetched.freshnessScore >= 16 && fetched.freshnessScore <= 40);
});

test("jobs domain returns null freshness when timestamps are missing/invalid", () => {
  const freshness = deriveFreshness({ postedAt: "bad", fetchedAt: "" });
  assert.equal(freshness.freshnessScore, null);
  assert.equal(freshness.freshnessAgeDays, null);
  assert.equal(freshness.freshnessSource, "");

  const rows = normalizeJobs([{ title: "Artist", company: "Studio", postedAt: "bad" }], {
    professionLabels: {},
    sanitizeUrl: value => value,
    nowMs: Date.parse("2026-03-08T00:00:00.000Z")
  });
  assert.equal(rows[0].freshnessScore, null);
  assert.equal(rows[0].freshnessAgeDays, null);
  assert.equal(rows[0].freshnessSource, "");
});

test("jobs domain normalizes lifecycle status and timestamps", () => {
  const rows = normalizeJobs([{
    title: "Animator",
    company: "Studio",
    status: "LIKELY_REMOVED",
    firstSeenAt: "2026-03-01T00:00:00.000Z",
    lastSeenAt: "2026-03-05T00:00:00.000Z",
    removedAt: "2026-03-06T00:00:00.000Z"
  }], {
    professionLabels: {},
    sanitizeUrl: value => value
  });
  assert.equal(rows[0].status, "likely_removed");
  assert.equal(rows[0].firstSeenAt, "2026-03-01T00:00:00.000Z");
  assert.equal(rows[0].lastSeenAt, "2026-03-05T00:00:00.000Z");
  assert.equal(rows[0].removedAt, "2026-03-06T00:00:00.000Z");
});
