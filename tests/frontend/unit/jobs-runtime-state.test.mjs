import test from "node:test";
import assert from "node:assert/strict";

import { DEFAULT_FILTERS } from "../../../frontend/jobs/state.js";
import {
  createJobsPageState,
  createJobsPipelineUiState,
  createJobsRuntimeState,
  createJobsUserState
} from "../../../frontend/jobs/app/runtime/state.js";

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
