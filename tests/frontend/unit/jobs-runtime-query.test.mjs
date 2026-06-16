import test from "node:test";
import assert from "node:assert/strict";

import {
  buildFilterOptions,
  filterJobs,
  isCleanFilterOptionValue,
  jobMatchesLifecycleFilter,
  sortJobs
} from "../../../frontend/jobs/app/runtime/query.js";
import {
  getJobLocationCities,
  getJobLocationCountries,
  isCityFilterEligible,
  sanitizeLocationField
} from "../../../frontend/jobs/domain.js";

test("jobs runtime query helpers derive filter options from the full job set", () => {
  const options = buildFilterOptions([
    {
      profession: "engineer",
      sector: "Games",
      city: "Amsterdam",
      country: "NL"
    },
    {
      profession: "artist",
      sector: "Art",
      city: "Berlin / Hamburg",
      country: "Japan"
    },
    {
      profession: "writer",
      sector: "Narrative",
      city: "2026",
      country: "US"
    },
    {
      profession: "producer",
      sector: "Games",
      city: "A bachelor's degree in digital communications",
      country: "CA"
    },
    {
      profession: "designer",
      sector: "Art",
      city: "????",
      country: "GB"
    },
    {
      profession: "producer",
      sector: "Games",
      city: "Rotterdam",
      country: "Onsite"
    },
    {
      profession: "engineer",
      sector: "Tech",
      city: "Development",
      country: "NL"
    },
    {
      profession: "artist",
      sector: "Art",
      city: "sqs",
      country: "US"
    }
  ], {
    getJobLocationCities: job => [job.city].filter(Boolean),
    getJobLocationCountries: job => [job.country].filter(Boolean),
    isCityFilterEligible,
    isValidCountry: value => Boolean(sanitizeLocationField(value, "country")),
    isSemanticallyValidLocationValue: value => Boolean(sanitizeLocationField(value, "city")),
    getAvailableRegionOptions: countries => countries.map(country => ({
      value: `region:${country}`,
      label: country
    })),
    fullCountryName: value => value
  });

  assert.deepEqual(options.availableProfessions, ["artist", "designer", "engineer", "producer", "writer"]);
  assert.deepEqual(options.availableCities, ["Amsterdam", "Rotterdam"]);
  assert.deepEqual(options.availableCountries, ["CA", "GB", "Japan", "NL", "US"]);
  assert.deepEqual(options.availableSectors, ["Art", "Games", "Narrative", "Tech"]);
});

test("jobs runtime query builds city options from same-country compound locations only", () => {
  const options = buildFilterOptions([
    {
      profession: "engineer",
      sector: "Games",
      locations: [{ city: "Tokyo or Fukuoka", country: "Japan" }]
    },
    {
      profession: "artist",
      sector: "Games",
      locations: [{ city: "New York or London", country: "US" }]
    },
    {
      profession: "producer",
      sector: "Games",
      locations: [{ city: "S.F. or North America", country: "Unknown" }]
    }
  ], {
    getJobLocationCities,
    getJobLocationCountries,
    isCityFilterEligible,
    isValidCountry: value => Boolean(sanitizeLocationField(value, "country")),
    isSemanticallyValidLocationValue: value => Boolean(sanitizeLocationField(value, "city")),
    fullCountryName: value => value
  });

  assert.deepEqual(options.availableCities, ["Fukuoka", "Tokyo"]);
});

test("jobs runtime query helpers filter jobs by search, new-only, and country selection", () => {
  const seenJobKeys = new Set(["job-2"]);
  const jobs = [
    {
      id: "1",
      title: "Gameplay Engineer",
      company: "Studio",
      sector: "Games",
      city: "Amsterdam",
      country: "NL",
      workType: "Remote",
      status: "active"
    },
    {
      id: "2",
      title: "UI Artist",
      company: "Studio",
      sector: "Art",
      city: "Utrecht",
      country: "NL",
      workType: "Hybrid",
      status: "active"
    }
  ];

  const filtered = filterJobs(jobs, {
    search: "engineer",
    countries: ["NL"],
    newOnly: true,
    excludeInternship: false
  }, {
    currentUser: { uid: "user" },
    seenJobKeys,
    getJobKeyForJob: job => `job-${job.id}`,
    getJobLocationCities: job => [job.city].filter(Boolean),
    getJobLocationCountries: job => [job.country].filter(Boolean),
    isInternshipJob: () => false,
    matchesCountrySelection: (country, selections) => selections.includes(country)
  });

  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].title, "Gameplay Engineer");
});

test("jobs runtime query helpers match multi-term search across job fields", () => {
  const jobs = [
    {
      id: "qloc-240",
      title: "Technical Artist",
      company: "Qloc careers",
      sector: "Game",
      city: "",
      country: "",
      source: "static_source::static:listing_url:https://qloc.elevato.net/en/",
      jobLink: "https://qloc.elevato.net/en/technical-artist,j,240",
      status: "active"
    },
    {
      id: "other-technical",
      title: "Technical Artist",
      company: "Other Studio",
      sector: "Game",
      city: "",
      country: "",
      status: "active"
    },
    {
      id: "qloc-tester",
      title: "Software Tester",
      company: "QLOC",
      sector: "Tech",
      city: "",
      country: "",
      status: "active"
    }
  ];

  const baseOptions = {
    getJobLocationCities: job => [job.city].filter(Boolean),
    getJobLocationCountries: job => [job.country].filter(Boolean),
    matchesCountrySelection: (country, selections) => selections.includes(country)
  };

  assert.deepEqual(
    filterJobs(jobs, { search: "QLOC", countries: [] }, baseOptions).map(job => job.id),
    ["qloc-240", "qloc-tester"]
  );
  assert.deepEqual(
    filterJobs(jobs, { search: "Technical Artist", countries: [] }, baseOptions).map(job => job.id),
    ["qloc-240", "other-technical"]
  );
  assert.deepEqual(
    filterJobs(jobs, { search: "QLOC Technical Artist", countries: [] }, baseOptions).map(job => job.id),
    ["qloc-240"]
  );
  assert.deepEqual(
    filterJobs(jobs, { search: "technical-artist,j,240", countries: [] }, baseOptions).map(job => job.id),
    ["qloc-240"]
  );
});

test("jobs runtime query helpers filter by read-only lifecycle evidence", () => {
  const jobs = [
    { id: "active", title: "Active", status: "active" },
    { id: "removed", title: "Removed", status: "likely_removed" },
    { id: "reappeared", title: "Reappeared", status: "active", lifecycleEvent: "reappeared" },
    {
      id: "preserved-failed",
      title: "Preserved Failed",
      status: "active",
      lifecycleEvent: "preserved",
      lifecycleReason: "source_failed"
    },
    {
      id: "preserved-skipped",
      title: "Preserved Skipped",
      status: "active",
      lifecycleEvent: "preserved",
      lifecycleReason: "source_skipped"
    }
  ];

  assert.deepEqual(
    filterJobs(jobs, { lifecycleStatus: "likely_removed" }).map(job => job.id),
    ["removed"]
  );
  assert.deepEqual(
    filterJobs(jobs, { lifecycleStatus: "reappeared" }).map(job => job.id),
    ["reappeared"]
  );
  assert.deepEqual(
    filterJobs(jobs, { lifecycleStatus: "preserved_source_failed" }).map(job => job.id),
    ["preserved-failed"]
  );
  assert.deepEqual(
    filterJobs(jobs, { lifecycleStatus: "all" }).map(job => job.id),
    ["active", "removed", "reappeared", "preserved-failed", "preserved-skipped"]
  );
  assert.equal(jobMatchesLifecycleFilter(jobs[4], "preserved_source_failed"), false);
});

test("jobs runtime query helpers sort jobs by relevance and title", () => {
  const jobs = [
    { title: "Bravo", freshnessScore: 4, country: "NL", company: "Studio", workType: "Remote" },
    { title: "Alpha", freshnessScore: 4, country: "NL", company: "Studio", workType: "Remote" },
    { title: "Zulu", freshnessScore: 1, country: "NL", company: "Studio", workType: "Remote" }
  ];

  const relevance = sortJobs(jobs, "relevance", {
    fullCountryName: value => value
  });
  assert.deepEqual(relevance.map(job => job.title), ["Zulu", "Alpha", "Bravo"]);

  const titleAsc = sortJobs(jobs, "title-asc", {
    fullCountryName: value => value
  });
  assert.deepEqual(titleAsc.map(job => job.title), ["Alpha", "Bravo", "Zulu"]);
});

test("jobs runtime query helpers reject contaminated filter option values", () => {
  assert.equal(isCleanFilterOptionValue("Amsterdam"), true);
  assert.equal(isCleanFilterOptionValue("<script>alert(1)</script>"), false);
  const semanticGuard = value => Boolean(sanitizeLocationField(value, "city"));
  assert.equal(isCleanFilterOptionValue("Remote", { isSemanticallyValidLocationValue: semanticGuard }), false);
  assert.equal(isCleanFilterOptionValue("Apr. 06", { isSemanticallyValidLocationValue: semanticGuard }), false);
  assert.equal(
    isCleanFilterOptionValue("CA - Canada; US - United States", { isSemanticallyValidLocationValue: semanticGuard }),
    false
  );
  assert.equal(isCleanFilterOptionValue("6th of October City", { isSemanticallyValidLocationValue: semanticGuard }), true);
  assert.equal(isCleanFilterOptionValue("St. Louis", { isSemanticallyValidLocationValue: semanticGuard }), true);
});
