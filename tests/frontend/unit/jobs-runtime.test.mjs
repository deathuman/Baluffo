import test from "node:test";
import assert from "node:assert/strict";

import { DEFAULT_FILTERS } from "../../../frontend/jobs/state.js";
import { createJobsEventsController } from "../../../frontend/jobs/app/runtime/events.js";
import {
  createJobsPageState,
  createJobsPipelineUiState,
  createJobsRuntimeState,
  createJobsUserState
} from "../../../frontend/jobs/app/runtime/state.js";
import { createElement } from "./helpers/jobs-runtime-helpers.mjs";

function createWindowStub({ innerHeight = 900, innerWidth = 1200 } = {}) {
  const listeners = new Map();
  return {
    innerHeight,
    innerWidth,
    listeners,
    addEventListener(type, handler) {
      const handlers = listeners.get(type) || [];
      handlers.push(handler);
      listeners.set(type, handlers);
    }
  };
}

function createDocumentStub() {
  const listeners = new Map();
  return {
    listeners,
    addEventListener(type, handler) {
      const handlers = listeners.get(type) || [];
      handlers.push(handler);
      listeners.set(type, handlers);
    }
  };
}

function dispatchAll(target, type, event) {
  for (const handler of target.listeners.get(type) || []) {
    handler(event);
  }
}

test("jobs events controller wires core actions to the existing runtime callbacks", async () => {
  const dom = {
    savedJobsBtn: createElement(),
    countryPickerClearBtn: createElement(),
    quickFiltersResetBtn: createElement(),
    authSignInBtn: createElement(),
    authSignOutBtn: createElement(),
    adminPageBtn: createElement(),
    refreshJobsBtn: createElement(),
    jobsPipelineRunBtn: createElement()
  };
  const pageState = { filters: { countries: ["IT"] } };
  const runtimeState = { coreEventsBound: false, filteredJobs: [], allJobs: [] };
  const calls = {
    remember: 0,
    navigate: [],
    applyFilters: [],
    resetQuick: 0,
    signIn: 0,
    signOut: 0,
    admin: 0,
    refresh: [],
    pipeline: 0
  };
  const asyncClicks = [];
  const controller = createJobsEventsController({
    dom,
    pageState,
    runtimeState,
    filtersController: {
      applyStateToFilters: () => {
        calls.applyFilters.push("state");
      },
      resetQuickFilterPreferences: () => {
        calls.resetQuick += 1;
      }
    },
    authController: {
      signInUser: async () => {
        calls.signIn += 1;
      },
      signOutUser: async () => {
        calls.signOut += 1;
      }
    },
    rememberCurrentJobsUrl: () => {
      calls.remember += 1;
    },
    navigateDesktopPage: page => {
      calls.navigate.push(page);
    },
    openAdminPageFromJobs: async () => {
      calls.admin += 1;
    },
    refreshJobsNow: async options => {
      calls.refresh.push(options);
    },
    triggerJobsPipelineRun: async () => {
      calls.pipeline += 1;
    },
    handleAutoRefreshSignalValue: () => {},
    applyFiltersAndRender: options => {
      calls.applyFilters.push(options);
    },
    bindUi: () => {},
    bindAsyncClick: (element, handler) => {
      asyncClicks.push(element);
      element.boundAsyncClick = handler;
    },
    bindHandlersMap: map => {
      for (const [element, handler] of map) {
        if (element) element.boundHandler = handler;
      }
    },
    debounce: handler => handler,
    jobsAutoRefreshSignalKey: "jobs-auto-refresh",
    jobsListDelegation: () => {},
    goToPage: () => {},
    windowObject: createWindowStub(),
    documentObject: createDocumentStub()
  });

  controller.bindCoreEvents();
  controller.bindCoreEvents();

  dom.savedJobsBtn.boundHandler();
  dom.countryPickerClearBtn.boundHandler();
  dom.quickFiltersResetBtn.boundHandler();
  await dom.authSignInBtn.boundAsyncClick();
  await dom.authSignOutBtn.boundAsyncClick();
  await dom.adminPageBtn.boundAsyncClick();
  await dom.refreshJobsBtn.boundAsyncClick();
  await dom.jobsPipelineRunBtn.boundAsyncClick();

  assert.equal(runtimeState.coreEventsBound, true);
  assert.equal(asyncClicks.length, 5);
  assert.equal(calls.remember, 1);
  assert.deepEqual(calls.navigate, ["saved.html"]);
  assert.deepEqual(pageState.filters.countries, []);
  assert.deepEqual(calls.applyFilters, ["state", { resetPage: true }]);
  assert.equal(calls.resetQuick, 1);
  assert.equal(calls.signIn, 1);
  assert.equal(calls.signOut, 1);
  assert.equal(calls.admin, 1);
  assert.deepEqual(calls.refresh, [{ manual: true }]);
  assert.equal(calls.pipeline, 1);
});

test("jobs events controller handles filter UI, resize, storage, and keyboard navigation", () => {
  class FakeInputElement {}

  const dom = {
    jobsList: createElement({
      getBoundingClientRect: () => ({ top: 100 })
    }),
    workTypeFilter: createElement(),
    lifecycleStatusFilter: createElement(),
    countryFilter: createElement(),
    cityFilter: createElement(),
    sectorFilter: createElement(),
    professionFilter: createElement(),
    sortFilter: createElement(),
    professionSearchFilter: createElement(),
    countryPickerBtn: createElement(),
    countryPickerSearch: createElement(),
    countryPickerOptions: createElement(),
    countryPickerPanel: createElement({
      classList: {
        contains: () => false
      }
    }),
    searchFilter: createElement(),
    quickActionsEl: createElement(),
    customizeQuickFiltersBtn: createElement(),
    quickFiltersOptionsEl: createElement(),
    quickFiltersPanel: createElement({
      classList: {
        contains: () => false
      }
    })
  };
  const windowObject = createWindowStub({ innerHeight: 1000, innerWidth: 1000 });
  windowObject.HTMLInputElement = FakeInputElement;
  const documentObject = createDocumentStub();
  const pageState = {
    currentPage: 1,
    itemsPerPage: 5,
    filters: { countries: [] }
  };
  const runtimeState = {
    allJobs: Array.from({ length: 30 }, (_, index) => ({ id: String(index + 1) })),
    filteredJobs: Array.from({ length: 30 }, (_, index) => ({ id: String(index + 1) })),
    secondaryEventsBound: false
  };
  const calls = {
    applyFilters: [],
    search: 0,
    countrySearch: [],
    countryToggle: 0,
    quick: [],
    quickVisibility: [],
    storage: [],
    goToPage: []
  };
  const controller = createJobsEventsController({
    dom,
    pageState,
    runtimeState,
    filtersController: {
      onFilterChange: () => {
        calls.search += 1;
      },
      renderProfessionOptions: value => {
        calls.profession = value;
      },
      toggleCountryPickerPanel: () => {
        calls.countryToggle += 1;
      },
      renderCountryPickerOptions: value => {
        calls.countrySearch.push(value);
      },
      applyStateToFilters: () => {
        calls.applyStateToFilters = (calls.applyStateToFilters || 0) + 1;
      },
      closeCountryPickerPanel: () => {},
      closeQuickFiltersPanel: () => {},
      applyQuickFilter: quick => {
        calls.quick.push(quick);
      },
      setQuickFilterVisibility: (quick, visible) => {
        calls.quickVisibility.push([quick, visible]);
      }
    },
    authController: {
      signInUser: async () => {},
      signOutUser: async () => {}
    },
    rememberCurrentJobsUrl: () => {},
    navigateDesktopPage: () => {},
    openAdminPageFromJobs: async () => {},
    refreshJobsNow: async () => {},
    triggerJobsPipelineRun: async () => {},
    handleAutoRefreshSignalValue: value => {
      calls.storage.push(value);
    },
    applyFiltersAndRender: options => {
      calls.applyFilters.push(options);
    },
    bindUi: (element, type, handler) => {
      element?.addEventListener(type, handler);
    },
    bindAsyncClick: () => {},
    bindHandlersMap: () => {},
    debounce: handler => handler,
    jobsAutoRefreshSignalKey: "jobs-auto-refresh",
    jobsListDelegation: () => {},
    goToPage: page => {
      calls.goToPage.push(page);
    },
    windowObject,
    documentObject
  });

  controller.bindEvents();

  const checkbox = new FakeInputElement();
  checkbox.type = "checkbox";
  checkbox.checked = true;
  checkbox.value = "IT";
  dom.countryPickerOptions.dispatch("change", { target: checkbox });
  dom.searchFilter.dispatch("input", { target: dom.searchFilter });
  dom.quickActionsEl.dispatch("click", {
    target: {
      closest: () => ({ dataset: { quick: "remote-only" } })
    }
  });
  const quickCheckbox = new FakeInputElement();
  quickCheckbox.type = "checkbox";
  quickCheckbox.checked = true;
  quickCheckbox.dataset = { quick: "new-only" };
  dom.quickFiltersOptionsEl.dispatch("change", { target: quickCheckbox });
  dispatchAll(windowObject, "storage", { key: "jobs-auto-refresh", newValue: "signal-1" });
  dispatchAll(windowObject, "resize", {});
  dispatchAll(documentObject, "keydown", {
    key: "ArrowRight",
    target: { tagName: "DIV", isContentEditable: false }
  });

  assert.equal(runtimeState.secondaryEventsBound, true);
  assert.deepEqual(pageState.filters.countries, ["IT"]);
  assert.equal(calls.applyStateToFilters, 2);
  assert.deepEqual(calls.applyFilters, [
    { resetPage: true },
    { resetPage: true },
    { resetPage: false }
  ]);
  assert.equal(calls.search, 1);
  assert.deepEqual(calls.quick, ["remote-only"]);
  assert.deepEqual(calls.quickVisibility, [["new-only", true]]);
  assert.deepEqual(calls.storage, ["signal-1"]);
  assert.deepEqual(calls.goToPage, [2]);
  assert.equal(pageState.itemsPerPage, 14);
});

test("jobs runtime state builder groups mutable runtime state and clones filter arrays", () => {
  const defaultFilters = {
    ...DEFAULT_FILTERS,
    countries: ["NL"]
  };

  const runtime = createJobsRuntimeState(defaultFilters, {
    lastHandledAutoRefreshSignalId: "12"
  });

  assert.notStrictEqual(runtime.pageState.filters.countries, defaultFilters.countries);
  assert.deepEqual(runtime.pageState.filters.countries, ["NL"]);

  runtime.pageState.filters.countries.push("BE");
  assert.deepEqual(defaultFilters.countries, ["NL"]);
  assert.equal(runtime.runtimeState.lastHandledAutoRefreshSignalId, 12);
  assert.deepEqual(runtime.runtimeState.allJobs, []);
  assert.deepEqual(runtime.runtimeState.filteredJobs, []);
  assert.equal(runtime.runtimeState.desktopUrlStateReady, false);
  assert.equal(runtime.runtimeState.nonCriticalStartupScheduled, false);
  assert.equal(runtime.runtimeState.desktopUpdateController, null);
  assert.ok(runtime.userState.savedJobKeys instanceof Set);
  assert.ok(runtime.userState.seenJobKeys instanceof Set);
  assert.equal(runtime.pipelineUiState.bridgeOnline, false);
});

test("jobs runtime state factories return isolated default containers", () => {
  const pageState = createJobsPageState({ countries: ["FR"] });
  const pipelineState = createJobsPipelineUiState();
  const userState = createJobsUserState();

  assert.equal(pageState.currentPage, 1);
  assert.deepEqual(pageState.filters.countries, ["FR"]);
  assert.equal(pipelineState.pollingTimer, null);
  assert.equal(pipelineState.active, false);
  assert.equal(userState.currentUser, null);
  assert.equal(userState.authStateListenerBound, false);
});
