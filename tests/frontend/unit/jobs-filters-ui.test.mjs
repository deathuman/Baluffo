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

function createOptionElement() {
  return {
    value: "",
    textContent: "",
    selected: false
  };
}

function createSelectElement() {
  const element = createElement({
    options: [],
    appendChild(option) {
      this.options.push(option);
    }
  });
  let html = "";
  Object.defineProperty(element, "innerHTML", {
    get() {
      return html;
    },
    set(value) {
      html = String(value || "");
      element.options = [];
    }
  });
  return element;
}

function withFakeDocument(callback) {
  const previousDocument = globalThis.document;
  globalThis.document = {
    createElement: () => createOptionElement()
  };
  try {
    return callback();
  } finally {
    if (previousDocument === undefined) {
      delete globalThis.document;
    } else {
      globalThis.document = previousDocument;
    }
  }
}

function createFullFiltersControllerHarness({ selectedCity = "" } = {}) {
  const refs = {
    workTypeFilter: createSelectElement(),
    countryFilter: createSelectElement(),
    professionFilter: createSelectElement(),
    cityFilter: createSelectElement(),
    sectorFilter: createSelectElement(),
    sortFilter: createSelectElement(),
    searchFilter: createElement(),
    countryPickerOptions: createElement(),
    quickActionsEl: createElement(),
    quickFiltersOptionsEl: createElement(),
    quickFiltersResetBtn: createElement()
  };
  const state = { filters: { ...DEFAULT_FILTERS, countries: [], city: selectedCity } };
  const controller = createJobsFiltersController({
    refs,
    state,
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
    writeQuickFilterPreferences: () => {},
    QUICK_FILTER_PREFS_KEY: "quick-filter-prefs",
    escapeHtml: value => String(value || ""),
    normalizeLifecycleStatus: value => value || "active"
  });
  return { controller, refs, state };
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

test("jobs city filter defers thousands of options until the selector is used", () => withFakeDocument(() => {
  const { controller, refs } = createFullFiltersControllerHarness();
  const cities = ["Amsterdam", "Berlin", "Copenhagen", "Dublin", "Edinburgh"];

  controller.updateFilterOptions([], {
    precomputed: {
      availableCountries: [],
      availableCountryFilterValues: [],
      availableProfessions: [],
      availableCities: cities,
      availableSectors: []
    }
  });

  assert.deepEqual(refs.cityFilter.options.map(option => option.value), [""]);

  controller.materializeCityOptions();

  assert.deepEqual(refs.cityFilter.options.map(option => option.value), ["", ...cities]);
}));

test("jobs city filter preserves a selected city before materializing all options", () => withFakeDocument(() => {
  const { controller, refs, state } = createFullFiltersControllerHarness({ selectedCity: "Berlin" });
  const cities = ["Amsterdam", "Berlin", "Copenhagen"];

  controller.updateFilterOptions([], {
    precomputed: {
      availableCountries: [],
      availableCountryFilterValues: [],
      availableProfessions: [],
      availableCities: cities,
      availableSectors: []
    }
  });
  controller.applyStateToFilters();

  assert.equal(state.filters.city, "Berlin");
  assert.equal(refs.cityFilter.value, "Berlin");
  assert.deepEqual(refs.cityFilter.options.map(option => option.value), ["", "Berlin"]);

  controller.materializeCityOptions();

  assert.equal(refs.cityFilter.value, "Berlin");
  assert.deepEqual(refs.cityFilter.options.map(option => option.value), ["", ...cities]);
}));
