export function createJobsPageState(defaultFilters) {
  return {
    currentPage: 1,
    itemsPerPage: 10,
    filters: { ...defaultFilters, countries: Array.from(defaultFilters.countries || []) }
  };
}

export function createJobsPipelineUiState() {
  return {
    pollingTimer: null,
    runId: "",
    active: false,
    bridgeOnline: false,
    startedAt: ""
  };
}
