import test from "node:test";
import assert from "node:assert/strict";
import { matchesCountrySelection } from "../../../frontend/jobs/app/countries.js";

// A region has to be findable by every spelling its members can arrive as.
//
// The v9 feed stores the UK three ways -- `GB` (1,770 rows), `UK` (20) and `England` (356) --
// and the region index canonicalised only its own list, so `region:europe` held the token
// `uk` while a stored `GB` tokenises to `unitedkingdom`. Every UK row was invisible to a
// Europe selection: 2,146 rows, on a filter that exists and is offered.
const UK_SPELLINGS = ["GB", "UK", "United Kingdom", "England"];

test("a stored GB row is inside the Europe region", () => {
  assert.equal(matchesCountrySelection("GB", ["region:europe"]), true);
});

test("a stored UK row is inside the Europe region", () => {
  assert.equal(matchesCountrySelection("UK", ["region:europe"]), true);
});

test("a stored 'United Kingdom' row is inside the Europe region", () => {
  assert.equal(matchesCountrySelection("United Kingdom", ["region:europe"]), true);
});

test("a stored England row is inside the Europe region", () => {
  // England is the case the contract keeps as its own label, so it needs saying explicitly.
  assert.equal(matchesCountrySelection("England", ["region:europe"]), true);
});

test("the England country filter still matches England rows", () => {
  // Adding England to the region must not take it away from its own filter.
  assert.equal(matchesCountrySelection("England", ["England"]), true);
});

test("the United Kingdom country filter matches both its code and its alias", () => {
  for (const stored of ["GB", "UK"]) {
    assert.equal(matchesCountrySelection(stored, ["GB"]), true, stored);
  }
});

test("every UK spelling stays out of the North America region", () => {
  for (const stored of UK_SPELLINGS) {
    assert.equal(matchesCountrySelection(stored, ["region:north-america"]), false, stored);
  }
});

test("a genuinely non-European row is still excluded from Europe", () => {
  for (const stored of ["US", "CA", "JP", "Remote"]) {
    assert.equal(matchesCountrySelection(stored, ["region:europe"]), false, stored);
  }
});

test("a European row is still included in Europe", () => {
  for (const stored of ["DE", "FR", "NL", "PL", "SE", "ES"]) {
    assert.equal(matchesCountrySelection(stored, ["region:europe"]), true, stored);
  }
});
