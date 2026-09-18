import test from "node:test";
import assert from "node:assert/strict";

import { createJobsEventsController } from "../../../frontend/jobs/app/runtime/events.js";

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
    },
    querySelector: () => null,
    querySelectorAll: () => []
  };
}

function itemsPerPageFor(jobsList, innerWidth, innerHeight = 1000) {
  const pageState = { currentPage: 1, itemsPerPage: 5, filters: { countries: [] } };
  const runtimeState = {
    allJobs: Array.from({ length: 30 }, (_, index) => ({ id: String(index + 1) })),
    filteredJobs: Array.from({ length: 30 }, (_, index) => ({ id: String(index + 1) }))
  };
  const controller = createJobsEventsController({
    dom: { jobsList },
    pageState,
    runtimeState,
    filtersController: {
      onFilterChange: () => {},
      applyStateToFilters: () => {},
      closeCountryPickerPanel: () => {},
      closeQuickFiltersPanel: () => {}
    },
    windowObject: createWindowStub({ innerHeight, innerWidth }),
    documentObject: createDocumentStub()
  });
  controller.recalculateItemsPerPage();
  return pageState.itemsPerPage;
}

const LIST_TOP = 100;
const RESERVED = 140;

function listWithRowHeight(height) {
  return {
    getBoundingClientRect: () => ({ top: LIST_TOP }),
    querySelector: () => ({ getBoundingClientRect: () => ({ height }) })
  };
}

test("jobs rows-per-page measures a rendered row instead of trusting the breakpoint constant", () => {
  const available = 1000 - LIST_TOP - RESERVED;

  // A measured 55px desktop row drives the count, not the 52px guess.
  assert.equal(itemsPerPageFor(listWithRowHeight(55), 1200), Math.floor(available / 56));

  // The stacked-card layout is ~246px tall, not the 136px the old constant
  // assumed, so the measured value must win below the breakpoint too. The
  // viewport is taller here so the count stays above the floor of 4.
  const tall = 2000 - LIST_TOP - RESERVED;
  const cardRows = itemsPerPageFor(listWithRowHeight(246), 700, 2000);
  assert.equal(cardRows, Math.floor(tall / 247));
  assert.ok(cardRows < Math.floor(tall / 136), "measured card rows must beat the stale constant");
});

test("jobs rows-per-page falls back to the breakpoint guess when no row is measurable", () => {
  // The unit-test DOM stubs expose no querySelector, so the fallback is what
  // keeps the pre-existing desktop contract (14 rows) intact.
  const stubList = { getBoundingClientRect: () => ({ top: LIST_TOP }) };
  assert.equal(itemsPerPageFor(stubList, 1200), 14);

  const nullRowList = { getBoundingClientRect: () => ({ top: LIST_TOP }), querySelector: () => null };
  assert.equal(itemsPerPageFor(nullRowList, 1200), 14);

  // A zero-height row (hidden or not yet laid out) must not divide by zero.
  assert.equal(itemsPerPageFor(listWithRowHeight(0), 1200), 14);

  // A throwing row lookup must not break pagination.
  const throwingList = {
    getBoundingClientRect: () => ({ top: LIST_TOP }),
    querySelector: () => {
      throw new Error("detached");
    }
  };
  assert.equal(itemsPerPageFor(throwingList, 1200), 14);
});
