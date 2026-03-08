import test from "node:test";
import assert from "node:assert/strict";
import {
  detectWorkType,
  detectContractType,
  classifyCompanyType,
  normalizeJobs,
  getJobKeyForJob
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

test("jobs domain generates fallback key", () => {
  const key = getJobKeyForJob({ title: "A", company: "B", city: "C", country: "D" }, {});
  assert.match(key, /^job_/);
});
