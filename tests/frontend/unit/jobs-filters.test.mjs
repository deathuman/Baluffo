import test from "node:test";
import assert from "node:assert/strict";

import {
  applyQuickFilterToState,
  getActiveFilterSummaryItems,
  normalizeLifecycleStatus
} from "../../../frontend/jobs/app/filters.js";
import { QUICK_FILTERS } from "../../../frontend/jobs/state.js";

test("jobs filters normalize first-slice lifecycle filter values", () => {
  assert.equal(normalizeLifecycleStatus("all"), "all");
  assert.equal(normalizeLifecycleStatus("reappeared"), "reappeared");
  assert.equal(normalizeLifecycleStatus("preserved_source_failed"), "preserved_source_failed");
  assert.equal(normalizeLifecycleStatus("preserved_source_skipped"), "active");
});

test("jobs lifecycle quick filters update status without changing lifecycle semantics", () => {
  const filters = {
    lifecycleStatus: "active"
  };

  applyQuickFilterToState("reappeared", filters, QUICK_FILTERS, {
    toggleCountrySelection: () => {}
  });
  assert.equal(filters.lifecycleStatus, "reappeared");

  applyQuickFilterToState("reappeared", filters, QUICK_FILTERS, {
    toggleCountrySelection: () => {}
  });
  assert.equal(filters.lifecycleStatus, "active");
});

test("jobs active filter summary uses user-facing lifecycle labels", () => {
  assert.deepEqual(
    getActiveFilterSummaryItems({
      workType: "",
      lifecycleStatus: "preserved_source_failed",
      countries: [],
      city: "",
      sector: "",
      profession: "",
      newOnly: false,
      excludeInternship: false,
      search: ""
    }, {
      professionLabels: {}
    }),
    ["Status: Preserved because source failed"]
  );
});
