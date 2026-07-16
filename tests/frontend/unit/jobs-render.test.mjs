import test from "node:test";
import assert from "node:assert/strict";
import { renderJobRowHtml } from "../../../frontend/jobs/render.js";
import { sanitizeUrl } from "../../../frontend/shared/data/index.js";

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
  assert.match(postedHtml, /data-tooltip="Posted 33d ago \(Feb 3, 2026\)"/);
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
  assert.match(fetchedHtml, /data-tooltip="Fetched 4d ago \(best guess\) \(Mar 4, 2026\)"/);
});

test("jobs render shows empty freshness ring when score is unavailable", () => {
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
  assert.match(html, /job-freshness-ping unknown/);
  assert.match(html, /data-tooltip="Freshness unknown"/);
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
  assert.match(html, /Recently removed/);
  assert.match(html, /data-tooltip="Recently removed since Mar 7, 2026"/);
});

test("jobs render shows reappeared and preserved lifecycle badges", () => {
  const reappearedHtml = render({
    id: "4b",
    title: "Gameplay Engineer",
    company: "Studio",
    sector: "Game",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time",
    status: "active",
    lifecycleEvent: "reappeared"
  });
  assert.match(reappearedHtml, /job-lifecycle-badge reappeared/);
  assert.match(reappearedHtml, />Reappeared<\/span>/);

  const preservedHtml = render({
    id: "4c",
    title: "Build Engineer",
    company: "Studio",
    sector: "Game",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time",
    status: "active",
    lifecycleEvent: "preserved",
    lifecycleReason: "source_failed"
  });
  assert.match(preservedHtml, /job-lifecycle-badge preserved/);
  assert.match(preservedHtml, />Preserved because source failed<\/span>/);

  const skippedHtml = render({
    id: "4d",
    title: "Producer",
    company: "Studio",
    sector: "Game",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time",
    status: "active",
    lifecycleEvent: "preserved",
    lifecycleReason: "source_skipped"
  });
  assert.doesNotMatch(skippedHtml, /Preserved because source skipped/);
});

test("jobs render rewrites remoteok detail links to the listing page", () => {
  const html = render({
    id: "4a",
    title: "Gameplay Programmer",
    company: "Nebula Games",
    sector: "Game",
    city: "Remote",
    country: "Remote",
    workType: "Remote",
    contractType: "Full-time",
    jobLink: "https://remoteok.com/remote-jobs/remote-gameplay-programmer-nebula-1234567"
  }, { sanitizeUrl });

  assert.match(html, /data-job-link="https:\/\/remoteok\.com\/jobs"/);
  assert.doesNotMatch(html, /remote-jobs\/remote-gameplay-programmer-nebula-1234567/);
});

test("jobs render uses separated location hierarchy", () => {
  const html = render({
    id: "4b",
    title: "Systems & Tools Engineer",
    company: "Stellar Entertainment",
    sector: "Game",
    city: "Guildford",
    country: "UK",
    locationSummary: "Guildford, UK | Utrecht, NL",
    workType: "Remote",
    contractType: "Full-time",
    jobLink: "https://jobs.example.com/stellar"
  });
  assert.match(html, /class="job-company-compact">Stellar Entertainment<\/span>/);
  assert.match(html, /class="job-country-main">UK<\/span>/);
  assert.match(html, /class="job-city-sub">Guildford<\/span>/);
  assert.doesNotMatch(html, /data-tooltip="Stellar Entertainment"/);
  assert.doesNotMatch(html, /data-tooltip="UK"/);
  assert.doesNotMatch(html, /data-tooltip="Guildford"/);
});

test("jobs render suppresses duplicated location and unknown country display", () => {
  const remoteHtml = render({
    id: "4c",
    title: "Analytics Manager",
    company: "Atari",
    sector: "Game",
    city: "Remote",
    country: "Remote",
    locationSummary: "Remote, Remote",
    workType: "Remote",
    contractType: "Unknown"
  });
  assert.doesNotMatch(remoteHtml, /Remote, Remote/);
  assert.match(remoteHtml, /class="job-country-main"><\/span>/);
  assert.match(remoteHtml, /class="job-city-sub"><\/span>/);

  const countryHtml = render({
    id: "4d",
    title: "Revenue Specialist",
    company: "Easygo",
    sector: "Tech",
    city: "Melbourne, Australia",
    country: "Australia",
    locationSummary: "Melbourne, Australia",
    workType: "Onsite",
    contractType: "Unknown"
  });
  assert.match(countryHtml, /class="job-country-main">Australia<\/span>/);
  assert.match(countryHtml, /class="job-city-sub">Melbourne<\/span>/);
  assert.doesNotMatch(countryHtml, /Melbourne, Australia/);

  const unknownHtml = render({
    id: "4e",
    title: "Security Analyst",
    company: "Scopely",
    sector: "Tech",
    city: "US - United States, Unknown",
    country: "Unknown",
    locationSummary: "US - United States, Unknown",
    workType: "Onsite",
    contractType: "Unknown"
  });
  assert.doesNotMatch(unknownHtml, /Unknown Unknown/);
});

test("jobs render uses accepted default row hierarchy and save icon", () => {
  const html = render({
    id: "7",
    title: "Technical Artist",
    company: "Studio",
    sector: "Game",
    city: "Boston, United States",
    country: "United States",
    workType: "Hybrid",
    contractType: "Full-time"
  });

  assert.match(html, /<div class="col-title job-cell" data-label="Position">/);
  assert.match(html, /<div class="job-title-line">/);
  assert.match(html, /<div class="job-sector-line">Game<\/div>/);
  assert.match(html, /<div class="col-company job-cell" data-label="Company">/);
  assert.match(html, /<div class="col-location job-cell" data-label="Location">/);
  assert.match(html, /class="job-company-compact">Studio<\/span>/);
  assert.match(html, /class="job-country-main">United States<\/span>/);
  assert.match(html, /class="job-city-sub">Boston<\/span>/);
  assert.doesNotMatch(html, /data-tooltip="Studio"/);
  assert.doesNotMatch(html, /data-tooltip="United States"/);
  assert.doesNotMatch(html, /data-tooltip="Boston"/);
  assert.match(html, /<div class="col-contract job-cell" data-label="Contract">/);
  assert.match(html, /<div class="col-type job-cell" data-label="Type">/);
  assert.match(html, /<div class="col-save job-cell" data-label="Save" aria-label="Job actions">/);
  assert.match(html, /aria-label="Save job"/);
  assert.match(html, />＋<\/span>/);

  const savedHtml = render({
    id: "8",
    title: "Producer",
    company: "Studio",
    sector: "Game",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time"
  }, {
    savedJobKeys: new Set(["job_key"])
  });
  assert.match(savedHtml, /aria-label="Job saved"/);
  assert.match(savedHtml, />✓<\/span>/);
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

test("confirmed unavailable rows require the explicit warned original-link action", () => {
  const html = render({
    id: "9",
    title: "Rendering Engineer",
    company: "Studio",
    sector: "Game",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time",
    jobLink: "https://example.com/jobs/9",
    availabilityStatus: "unavailable"
  });

  assert.match(html, /data-job-link=""/);
  assert.doesNotMatch(html, /class="job-row[^\"]*job-row-link/);
  assert.match(html, /data-ui="job-original-link-btn"/);
  assert.match(html, />Open original link<\/button>/);
  assert.match(html, /availability-warning/);
});

test("availability check action is rendered only for bridge-capable runtimes", () => {
  const job = {
    id: "availability-runtime",
    title: "Engine Programmer",
    company: "Studio",
    sector: "Game",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time",
    jobLink: "https://example.com/jobs/runtime",
    availabilityId: "availability_runtime"
  };

  assert.doesNotMatch(render(job), /data-ui="job-availability-check-btn"/);
  assert.match(
    render(job, { canManageAvailability: true }),
    /data-ui="job-availability-check-btn"/
  );
});
