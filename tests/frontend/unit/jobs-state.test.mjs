import test from "node:test";
import assert from "node:assert/strict";
import { DEFAULT_FILTERS, PROFESSION_LABELS, QUICK_FILTERS } from "../../../jobs-state.js";

test("jobs state includes newOnly default flag", () => {
  assert.equal(DEFAULT_FILTERS.newOnly, false);
});

test("jobs quick filters expose New Only quick toggle", () => {
  const newOnlyFilter = QUICK_FILTERS.find(item => item.key === "new-only");
  assert.ok(newOnlyFilter);
  assert.equal(newOnlyFilter.type, "flag");
  assert.equal(newOnlyFilter.value, "newOnly");
});

test("jobs state exposes Technical Director profession filter", () => {
  assert.equal(PROFESSION_LABELS["technical-director"], "Technical Director");
  const technicalDirectorFilter = QUICK_FILTERS.find(item => item.key === "technical-director");
  assert.ok(technicalDirectorFilter);
  assert.equal(technicalDirectorFilter.type, "profession");
  assert.equal(technicalDirectorFilter.value, "technical-director");
  assert.equal(technicalDirectorFilter.defaultVisible, false);
});
