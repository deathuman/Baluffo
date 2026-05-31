import test from "node:test";
import assert from "node:assert/strict";

import * as savedViews from "../../../frontend/jobs/app/saved-views.js";

function createStorage(seed = {}) {
  const map = new Map(Object.entries(seed).map(([key, value]) => [String(key), String(value)]));
  return {
    getItem(key) {
      return map.has(String(key)) ? map.get(String(key)) : null;
    },
    setItem(key, value) {
      map.set(String(key), String(value));
    },
    removeItem(key) {
      map.delete(String(key));
    }
  };
}

function withGlobals(storage, fn) {
  const originalStorage = globalThis.localStorage;
  const originalNow = Date.now;
  globalThis.localStorage = storage;
  try {
    return fn({
      setNow(value) {
        Date.now = () => value;
      }
    });
  } finally {
    Date.now = originalNow;
    if (originalStorage === undefined) {
      delete globalThis.localStorage;
    } else {
      globalThis.localStorage = originalStorage;
    }
  }
}

const DEFAULT_FILTERS = Object.freeze({
  workType: "",
  lifecycleStatus: "active",
  countries: [],
  city: "",
  sector: "",
  profession: "",
  newOnly: false,
  excludeInternship: false,
  search: "",
  sort: "relevance"
});

test("saved filter presets strip defaults, apply filters, and delete cleanly", () => {
  const storage = createStorage({
    baluffo_jobs_recent_views: JSON.stringify([{ url: "/jobs.html?page=2", label: "Old current page" }])
  });

  withGlobals(storage, ({ setNow }) => {
    setNow(1000);
    assert.equal(savedViews.saveFilterPreset(" Remote Tech ", {
      currentPage: 4,
      filters: {
        ...DEFAULT_FILTERS,
        workType: "Remote",
        countries: ["Netherlands"],
        city: "Amsterdam",
        profession: "Engineering",
        newOnly: true,
        search: "graphics",
        sort: "most_recent"
      }
    }, DEFAULT_FILTERS), true);

    const [preset] = savedViews.listFilterPresets();
    assert.equal(preset.name, "Remote Tech");
    assert.equal(preset.label, "Remote Tech");
    assert.deepEqual(preset.filters, {
      workType: "Remote",
      countries: ["Netherlands"],
      city: "Amsterdam",
      profession: "Engineering",
      newOnly: true,
      search: "graphics",
      sort: "most_recent"
    });

    const applied = savedViews.applyFilterPreset("Remote Tech", {
      currentPage: 9,
      filters: { ...DEFAULT_FILTERS }
    });
    assert.equal(applied.currentPage, 1);
    assert.deepEqual(applied.filters, {
      ...DEFAULT_FILTERS,
      workType: "Remote",
      countries: ["Netherlands"],
      city: "Amsterdam",
      profession: "Engineering",
      newOnly: true,
      search: "graphics",
      sort: "most_recent"
    });

    assert.equal(
      storage.getItem("baluffo_jobs_recent_views"),
      JSON.stringify([{ url: "/jobs.html?page=2", label: "Old current page" }])
    );
    assert.equal(savedViews.deleteFilterPreset("Remote Tech"), true);
    assert.deepEqual(savedViews.listFilterPresets(), []);
  });
});

test("saved filter presets are capped and no recent-view API remains exported", () => {
  const storage = createStorage();

  withGlobals(storage, ({ setNow }) => {
    for (let index = 0; index < 11; index += 1) {
      setNow(2000 + index);
      assert.equal(savedViews.saveFilterPreset(`Preset ${index}`, {
        filters: { ...DEFAULT_FILTERS, search: `term-${index}` }
      }, DEFAULT_FILTERS), true);
    }

    const presets = savedViews.listFilterPresets();
    assert.equal(presets.length, 10);
    assert.equal(presets[0].name, "Preset 10");
    assert.equal(presets.at(-1).name, "Preset 1");
    assert.equal(savedViews.loadFilterPreset("Preset 0"), null);
    assert.equal(savedViews.saveFilterPreset("   ", { filters: DEFAULT_FILTERS }, DEFAULT_FILTERS), false);
  });

  assert.equal("recordRecentView" in savedViews, false);
  assert.equal("getRecentViews" in savedViews, false);
  assert.equal("clearRecentViews" in savedViews, false);
});
