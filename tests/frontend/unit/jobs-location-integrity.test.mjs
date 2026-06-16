import test from "node:test";
import assert from "node:assert/strict";
import {
  buildJobLocationSummary,
  getJobLocationCities,
  isValidCityFilterOption,
  sanitizeLocationField
} from "../../../frontend/jobs/domain.js";

test("jobs domain rejects high-confidence city filter noise but preserves edge-case cities", () => {
  [
    "BLANK",
    "(none) Vancouver",
    "Apr. 06",
    "Art Team Lead",
    "As a Product Associate",
    "CA - Canada; US - United States",
    "Fully Remote",
    "Hybrid",
    "On-site",
    "Remote"
  ].forEach(value => {
    assert.equal(isValidCityFilterOption(value), false, value);
    assert.equal(sanitizeLocationField(value, "city"), "", value);
  });

  assert.equal(isValidCityFilterOption("6th of October City"), true);
  assert.equal(isValidCityFilterOption("St. Louis"), true);
});

test("jobs domain hides AppData city dropdown pollutants while preserving real cities", () => {
  [
    "00:00",
    "1fr);",
    "sqs",
    "box",
    "Accounting",
    "Android",
    "Announcement",
    "Development",
    "Operations",
    "Everything",
    "For all applicants",
    "Europe",
    "S.F. or North America",
    "UK or GMT ± 2"
  ].forEach(value => {
    assert.equal(isValidCityFilterOption(value), false, value);
  });

  [
    "McLean",
    "Newport News",
    "Ciudad Juárez",
    "Thành phố Thủ Dầu Một",
    "Tweed Heads",
    "McKinney"
  ].forEach(value => {
    assert.equal(isValidCityFilterOption(value), true, value);
  });
});

test("jobs domain splits eligible or-joined city options only when every part is valid", () => {
  assert.equal(isValidCityFilterOption("Tokyo or Fukuoka"), false);
  assert.deepEqual(
    getJobLocationCities({ locations: [{ city: "Tokyo or Fukuoka", country: "Japan" }] }),
    ["Tokyo", "Fukuoka"]
  );
  assert.deepEqual(
    getJobLocationCities({ locations: [{ city: "S.F. or North America", country: "Unknown" }] }),
    []
  );
  assert.deepEqual(
    getJobLocationCities({ locations: [{ city: "New York or London", country: "US" }] }),
    []
  );
});

test("jobs domain builds compact location summaries without repeated countries or work modes", () => {
  assert.equal(
    buildJobLocationSummary({
      locations: [
        { city: "Melbourne, Australia", country: "Australia" },
        { city: "Melbourne, Australia", country: "Australia" }
      ]
    }),
    "Melbourne, Australia"
  );
  assert.equal(
    buildJobLocationSummary({
      locations: [{ city: "Remote", country: "Remote" }],
      city: "Remote",
      country: "Remote"
    }),
    ""
  );
});
