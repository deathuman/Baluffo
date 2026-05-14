import test from "node:test";
import assert from "node:assert/strict";

import { QUICK_FILTERS, DEFAULT_FILTERS } from "../../../frontend/jobs/state.js";
import { createJobsFiltersController } from "../../../frontend/jobs/app/runtime/filters-ui.js";
import { createElement } from "./helpers/jobs-runtime-helpers.mjs";

function createFiltersControllerHarness() {
  const writes = [];
  const refs = {
    quickActionsEl: createElement(),
    quickFiltersOptionsEl: createElement(),
    quickFiltersResetBtn: createElement()
  };
  const controller = createJobsFiltersController({
    refs,
    state: { filters: { ...DEFAULT_FILTERS, countries: [] } },
    defaultFilters: DEFAULT_FILTERS,
    quickFilters: QUICK_FILTERS,
    professionLabels: {},
    jobsDispatch: { dispatch: () => {} },
    JOBS_ACTIONS: { FILTERS_CHANGED: "jobs/filtersChanged" },
    applyFiltersAndRender: () => {},
    buildFilterOptions: () => ({}),
    getJobLocationCities: () => [],
    getJobLocationCountries: () => [],
    isValidCountry: () => true,
    isSemanticallyValidLocationValue: () => true,
    readQuickFilterPreferences: () => null,
    writeQuickFilterPreferences: (key, value) => {
      writes.push({ key, value });
    },
    QUICK_FILTER_PREFS_KEY: "quick-filter-prefs",
    escapeHtml: value => String(value || ""),
    normalizeLifecycleStatus: value => value || "active"
  });
  return { controller, refs, writes };
}

test("jobs quick filter reset button reflects whether default presets are already shown", () => {
  const { controller, refs, writes } = createFiltersControllerHarness();

  controller.initializeQuickFilters();

  assert.equal(refs.quickFiltersResetBtn.disabled, true);
  assert.equal(refs.quickFiltersResetBtn.getAttribute("aria-disabled"), "true");
  assert.match(refs.quickFiltersResetBtn.getAttribute("data-tooltip"), /already shown/);

  controller.setQuickFilterVisibility("netherlands", false);

  assert.equal(refs.quickFiltersResetBtn.disabled, false);
  assert.equal(refs.quickFiltersResetBtn.getAttribute("aria-disabled"), "false");
  assert.match(refs.quickFiltersResetBtn.getAttribute("data-tooltip"), /Restore the default/);
  assert.doesNotMatch(refs.quickActionsEl.innerHTML, /data-quick="netherlands"/);

  controller.resetQuickFilterPreferences();

  assert.equal(refs.quickFiltersResetBtn.disabled, true);
  assert.equal(refs.quickFiltersResetBtn.getAttribute("aria-disabled"), "true");
  assert.match(refs.quickActionsEl.innerHTML, /data-quick="netherlands"/);
  assert.deepEqual(writes.at(-1), {
    key: "quick-filter-prefs",
    value: ["remote", "new-only", "exclude-internship", "netherlands", "technical-artist", "clear"]
  });
});
