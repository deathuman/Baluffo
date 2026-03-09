import test from "node:test";
import assert from "node:assert/strict";
import { renderJobRowHtml } from "../../../frontend/jobs/render.js";

function render(job, options = {}) {
  return renderJobRowHtml(job, {
    fullCountryName: value => value,
    sanitizeUrl: value => value || "",
    getJobKeyForJob: () => "job_key",
    savedJobKeys: new Set(),
    isJobsApiReady: () => true,
    toContractClass: () => "unknown",
    capitalizeFirst: value => String(value || ""),
    ...options
  });
}

test("jobs render outputs freshness ping with correct class and tooltip", () => {
  const postedHtml = render({
    id: "1",
    title: "Gameplay Engineer",
    company: "Studio",
    sector: "Game",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time",
    postedAt: "2026-02-03T00:00:00.000Z",
    freshnessScore: 82,
    freshnessAgeDays: 33,
    freshnessSource: "postedAt"
  });
  assert.match(postedHtml, /job-freshness-ping stale/);
  assert.match(postedHtml, /title="Posted 33d ago \(Feb 3, 2026\)"/);
  assert.doesNotMatch(postedHtml, /job-lifecycle-badge/);

  const fetchedHtml = render({
    id: "2",
    title: "Tech Artist",
    company: "Studio",
    sector: "Game",
    city: "Milan",
    country: "Italy",
    workType: "Hybrid",
    contractType: "Temporary",
    fetchedAt: "2026-03-04T00:00:00.000Z",
    freshnessScore: 24,
    freshnessAgeDays: 4,
    freshnessSource: "fetchedAt"
  });
  assert.match(fetchedHtml, /job-freshness-ping fresh/);
  assert.match(fetchedHtml, /title="Fetched 4d ago \(best guess\) \(Mar 4, 2026\)"/);
});

test("jobs render omits freshness ping when score is unavailable", () => {
  const html = render({
    id: "3",
    title: "Animator",
    company: "Studio",
    sector: "Game",
    city: "Turin",
    country: "Italy",
    workType: "Onsite",
    contractType: "Unknown",
    freshnessScore: null,
    freshnessAgeDays: null,
    freshnessSource: ""
  });
  assert.match(html, /class="col-freshness" aria-hidden="true"><\/div>/);
  assert.doesNotMatch(html, /job-freshness-ping/);
});

test("jobs render shows lifecycle badge with removed date tooltip", () => {
  const html = render({
    id: "4",
    title: "Engine Programmer",
    company: "Studio",
    sector: "Game",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time",
    status: "likely_removed",
    removedAt: "2026-03-07T00:00:00.000Z",
    freshnessScore: 90,
    freshnessAgeDays: 35,
    freshnessSource: "postedAt"
  });
  assert.match(html, /job-lifecycle-badge likely-removed/);
  assert.match(html, /title="Likely removed since Mar 7, 2026"/);
});

test("jobs render marks unseen rows with New badge and seen rows with class", () => {
  const newHtml = render({
    id: "5",
    title: "UI Artist",
    company: "Studio",
    sector: "Game",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time"
  }, { isNew: true, isSeen: false });
  assert.match(newHtml, /class="job-row[^"]*job-row-new/);
  assert.match(newHtml, />New<\/span>/);
  assert.match(newHtml, /data-job-key="job_key"/);

  const seenHtml = render({
    id: "6",
    title: "Producer",
    company: "Studio",
    sector: "Game",
    city: "Rome",
    country: "Italy",
    workType: "Hybrid",
    contractType: "Full-time"
  }, { isSeen: true, isNew: false });
  assert.match(seenHtml, /class="job-row[^"]*job-row-seen/);
  assert.doesNotMatch(seenHtml, />New<\/span>/);
});
