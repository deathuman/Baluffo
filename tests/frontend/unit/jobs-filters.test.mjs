import test from "node:test";
import assert from "node:assert/strict";

import {
  applyQuickFilterToState,
  getActiveFilterSummaryItems,
  normalizeLifecycleStatus,
  renderQuickFiltersHtml
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

test("jobs quick filters use preset labels and separate clear", () => {
  const html = renderQuickFiltersHtml([
    "remote",
    "new-only",
    "exclude-internship",
    "netherlands",
    "clear"
  ], QUICK_FILTERS);

  assert.match(html, /data-quick="remote"[^>]*>Remote<\/button>/);
  assert.match(html, /data-quick="new-only"[^>]*>New<\/button>/);
  assert.match(html, /data-quick="exclude-internship"[^>]*>No internships<\/button>/);
  assert.match(html, /data-quick="netherlands"[^>]*>Netherlands<\/button>/);
  assert.match(html, /class="btn quick-btn quick-clear" data-quick="clear">Clear filters<\/button>/);
  assert.doesNotMatch(html, />Remote Only<\/button>/);
  assert.doesNotMatch(html, />Clear Filters<\/button>/);
});
