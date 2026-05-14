import test from "node:test";
import assert from "node:assert/strict";
import {
  buildJobLocationSummary,
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
