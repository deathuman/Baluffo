export function createJobsStartupPreviewController({
  runtimeState,
  pageState,
  displayJobs,
  windowObject = globalThis.window,
  createFilterOptionsAccumulator,
  addJobToFilterOptions,
  finalizeFilterOptions,
  compareJobsForSort,
  sortJobs,
  getJobLocationCities,
  getJobLocationCountries,
  isSemanticallyValidLocationValue,
  isValidCountry,
  getAvailableRegionOptions,
  fullCountryName
}) {
  function clearPendingStartupPreviewMaterialization() {
    if (runtimeState.startupPreviewMaterializeTimer) {
      windowObject.clearTimeout(runtimeState.startupPreviewMaterializeTimer);
    }
    runtimeState.startupPreviewMaterialize = null;
    runtimeState.startupPreviewMaterializeTimer = null;
    runtimeState.startupPreviewFilteredCount = 0;
  }

  function materializePendingStartupPreview({ render = false } = {}) {
    if (typeof runtimeState.startupPreviewMaterialize !== "function") return runtimeState.filteredJobs;
    const materialize = runtimeState.startupPreviewMaterialize;
    clearPendingStartupPreviewMaterialization();
    runtimeState.filteredJobs = materialize();
    if (render) {
      displayJobs(runtimeState.filteredJobs);
    }
    return runtimeState.filteredJobs;
  }

  function scheduleStartupPreviewMaterialization(materializeFilteredJobs) {
    if (typeof materializeFilteredJobs !== "function") return;
    clearPendingStartupPreviewMaterialization();
    runtimeState.startupPreviewMaterialize = materializeFilteredJobs;
    runtimeState.startupPreviewMaterializeTimer = windowObject.setTimeout(() => {
      materializePendingStartupPreview();
    }, 0);
  }

  function insertTopStartupPreviewJob(topJobs, job, limit) {
    if (!Number.isFinite(limit) || limit <= 0) return;
    let insertIndex = topJobs.findIndex(existing =>
      compareJobsForSort(job, existing, "relevance", {
        fullCountryName
      }) < 0
    );
    if (insertIndex < 0) insertIndex = topJobs.length;
    if (insertIndex >= limit && topJobs.length >= limit) return;
    topJobs.splice(insertIndex, 0, job);
    if (topJobs.length > limit) {
      topJobs.pop();
    }
  }

  function buildStartupPreviewFastPathPlan(allJobs) {
    const filterOptionsAccumulator = createFilterOptionsAccumulator();
    const activeJobs = [];
    const firstPageJobs = [];
    const firstPageLimit = Math.max(1, Number(pageState.itemsPerPage) || 1);

    (allJobs || []).forEach(job => {
      addJobToFilterOptions(filterOptionsAccumulator, job, {
        getJobLocationCities,
        getJobLocationCountries,
        isSemanticallyValidLocationValue,
        isValidCountry
      });
      if (String(job?.status || "active").toLowerCase() !== "active") return;
      activeJobs.push(job);
      insertTopStartupPreviewJob(firstPageJobs, job, firstPageLimit);
    });

    return {
      filterOptions: finalizeFilterOptions(filterOptionsAccumulator, {
        getAvailableRegionOptions,
        fullCountryName
      }),
      filteredCount: activeJobs.length,
      pageJobs: firstPageJobs,
      materializeFilteredJobs: () => sortJobs(activeJobs, "relevance", {
        fullCountryName
      })
    };
  }

  function renderStartupPreviewFastPath(plan = {}) {
    const pageJobs = Array.isArray(plan?.pageJobs) ? plan.pageJobs : [];
    const filteredCount = Number.isFinite(Number(plan?.filteredCount))
      ? Number(plan.filteredCount)
      : pageJobs.length;
    runtimeState.filteredJobs = pageJobs;
    runtimeState.startupPreviewFilteredCount = filteredCount;
    displayJobs(runtimeState.filteredJobs, {
      pageJobsOverride: pageJobs,
      totalCountOverride: filteredCount
    });
  }

  return {
    clearPendingStartupPreviewMaterialization,
    materializePendingStartupPreview,
    scheduleStartupPreviewMaterialization,
    buildStartupPreviewFastPathPlan,
    renderStartupPreviewFastPath
  };
}
