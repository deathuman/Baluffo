/**
 * Jobs feed — filter-state comparison and startup-preview fast-path gating.
 *
 * Split out of ``feed.js``; that module stays the thin coordinator owning
 * the public entrypoints.
 *
 * @module feed-filter-match
 */

function cloneFilterState(filters = {}) {
  return {
    ...filters,
    countries: Array.from(filters?.countries || [])
  };
}

function filtersMatchDefault(filters = {}, defaultFilters = {}) {
  const current = cloneFilterState(filters);
  const defaults = cloneFilterState(defaultFilters);
  return JSON.stringify(current) === JSON.stringify(defaults);
}

function jobsFirstRunBootstrapNumberOverride(windowObject, key, fallback) {
  try {
    const search = String(windowObject?.location?.search || "");
    const params = new URLSearchParams(search);
    if (params.get("desktop") !== "1" || params.get("jobsColdStart") !== "1") {
      return fallback;
    }
    const value = params.get(key);
    const numeric = Number(value);
    return Number.isFinite(numeric) && numeric > 0 ? numeric : fallback;
  } catch {
    return fallback;
  }
}

function canUseStartupPreviewFastPath(pageState = {}, defaultFilters = {}) {
  return Number(pageState?.currentPage || 1) === 1
    && filtersMatchDefault(pageState?.filters || {}, defaultFilters);
}

export {
  jobsFirstRunBootstrapNumberOverride,
  canUseStartupPreviewFastPath
};
