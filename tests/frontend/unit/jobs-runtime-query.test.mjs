import test from "node:test";
import assert from "node:assert/strict";

import {
  buildFilterOptions,
  filterJobs,
  isCleanFilterOptionValue,
  sortJobs
} from "../../../frontend/jobs/app/runtime/query.js";
import { sanitizeLocationField } from "../../../frontend/jobs/domain.js";

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
    }
  ], {
    getJobLocationCities: job => [job.city].filter(Boolean),
    getJobLocationCountries: job => [job.country].filter(Boolean),
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
  assert.deepEqual(options.availableSectors, ["Art", "Games", "Narrative"]);
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
});
