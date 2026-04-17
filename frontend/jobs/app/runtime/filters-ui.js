import {
  applyQuickFilterToState,
  getActiveFilterSummaryItems,
  getCountryPickerTriggerText,
  getCountrySelectionBadgeText,
  getDefaultQuickFilterKeys,
  getNextQuickFilterKeys,
  isQuickFilterActive,
  normalizeSelectedCountries,
  optionExists,
  renderCountryPickerOptionsHtml,
  renderQuickFilterOptionsHtml,
  renderQuickFiltersHtml,
  sanitizeQuickFilterKeys
} from "../filters.js";
import { fullCountryName as fullCountryNameForJobs, getAvailableRegionOptions as getAvailableRegionOptionsForJobs, getCountryFilterOptionLabel as getCountryFilterOptionLabelForJobs, resolveCountryCode as resolveCountryCodeForJobs } from "../countries.js";
import { capitalizeFirst } from "../runtime-utils.js";

export function createJobsFiltersController({
  refs,
  state,
  defaultFilters,
  quickFilters,
  professionLabels,
  jobsDispatch,
  JOBS_ACTIONS,
  applyFiltersAndRender,
  buildFilterOptions,
  getJobLocationCities,
  getJobLocationCountries,
  isValidCountry,
  isSemanticallyValidLocationValue,
  readQuickFilterPreferences,
  writeQuickFilterPreferences,
  QUICK_FILTER_PREFS_KEY,
  escapeHtml,
  normalizeLifecycleStatus
}) {
  let availableProfessions = [];
  let availableCountries = [];
  let availableCountryFilterValues = [];
  let visibleQuickFilterKeys = [];

  function updateCountrySelectionBadge() {
    if (!refs.countrySelectionBadge) return;
    refs.countrySelectionBadge.textContent = getCountrySelectionBadgeText(state.filters.countries);
  }

  function updateCountryPickerTrigger() {
    if (!refs.countryPickerBtn) return;
    refs.countryPickerBtn.textContent = getCountryPickerTriggerText(state.filters.countries);
  }

  function resolveCountryCode(countryCode) {
    return resolveCountryCodeForJobs(countryCode, {
      availableCountries,
      availableCountryFilterValues
    });
  }

  function toggleCountrySelection(countryCode) {
    const mapped = resolveCountryCode(countryCode);
    if (!mapped) return;
    const selected = new Set(state.filters.countries || []);
    if (selected.has(mapped)) selected.delete(mapped);
    else selected.add(mapped);
    state.filters.countries = Array.from(selected);
  }

  function syncCountryPickerChecks() {
    if (!refs.countryPickerOptions) return;
    const selected = new Set(state.filters.countries || []);
    refs.countryPickerOptions.querySelectorAll('input[type="checkbox"]').forEach(input => {
      input.checked = selected.has(input.value);
    });
  }

  function renderCountryPickerOptions(query = "") {
    if (!refs.countryPickerOptions) return;
    refs.countryPickerOptions.innerHTML = renderCountryPickerOptionsHtml({
      availableCountryFilterValues,
      selectedCountries: state.filters.countries,
      query,
      getCountryFilterOptionLabel: getCountryFilterOptionLabelForJobs,
      escapeHtml
    });
  }

  function updateQuickChipStates() {
    if (!refs.quickActionsEl) return;
    refs.quickActionsEl.querySelectorAll(".quick-chip").forEach(chip => {
      const key = chip.dataset.quick;
      const item = quickFilters.find(filter => filter.key === key);
      if (!item) return;
      const active = isQuickFilterActive(item, state.filters, { resolveCountryCode });
      chip.classList.toggle("active", active);
      chip.setAttribute("aria-pressed", active ? "true" : "false");
    });
  }

  function updateActiveFiltersSummary() {
    if (!refs.activeFiltersSummaryEl) return;
    const active = getActiveFilterSummaryItems(state.filters, {
      professionLabels
    });
    refs.activeFiltersSummaryEl.textContent = active.length ? `Active filters: ${active.join(" • ")}` : "No active filters";
  }

  function applyStateToStaticFilters() {
    if (refs.workTypeFilter) refs.workTypeFilter.value = state.filters.workType;
    if (refs.lifecycleStatusFilter) refs.lifecycleStatusFilter.value = state.filters.lifecycleStatus || "active";
    if (refs.searchFilter) refs.searchFilter.value = state.filters.search;
    if (refs.sortFilter) refs.sortFilter.value = state.filters.sort;
  }

  function renderProfessionOptions(query = "") {
    if (!refs.professionFilter) return;
    const normalized = String(query || "").trim().toLowerCase();
    const current = state.filters.profession;

    refs.professionFilter.innerHTML = '<option value="">All Roles</option>';
    availableProfessions.forEach(profession => {
      const label = professionLabels[profession] || capitalizeFirst(profession);
      if (normalized && !label.toLowerCase().includes(normalized) && profession !== current) {
        return;
      }
      const opt = document.createElement("option");
      opt.value = profession;
      opt.textContent = label;
      refs.professionFilter.appendChild(opt);
    });

    if (optionExists(refs.professionFilter, current)) {
      refs.professionFilter.value = current;
    } else if (current) {
      state.filters.profession = "";
      refs.professionFilter.value = "";
    }
  }

  function applyStateToFilters() {
    applyStateToStaticFilters();
    state.filters.countries = normalizeSelectedCountries(state.filters.countries, {
      resolveCountryCode,
      availableCountryFilterValues
    });

    if (refs.countryFilter) {
      const selected = new Set(state.filters.countries || []);
      Array.from(refs.countryFilter.options).forEach(option => {
        option.selected = selected.has(option.value);
      });
    }
    syncCountryPickerChecks();

    if (refs.cityFilter && optionExists(refs.cityFilter, state.filters.city)) {
      refs.cityFilter.value = state.filters.city;
    } else {
      state.filters.city = "";
    }

    if (refs.sectorFilter && optionExists(refs.sectorFilter, state.filters.sector)) {
      refs.sectorFilter.value = state.filters.sector;
    } else {
      state.filters.sector = "";
    }

    if (refs.professionFilter && optionExists(refs.professionFilter, state.filters.profession)) {
      refs.professionFilter.value = state.filters.profession;
    } else if (state.filters.profession && state.filters.profession !== "") {
      state.filters.profession = "";
    }

    updateCountrySelectionBadge();
    updateCountryPickerTrigger();
    updateQuickChipStates();
    updateActiveFiltersSummary();
  }

  function syncStateFromFilters() {
    state.filters.workType = refs.workTypeFilter ? refs.workTypeFilter.value : "";
    state.filters.lifecycleStatus = normalizeLifecycleStatus(refs.lifecycleStatusFilter ? refs.lifecycleStatusFilter.value : "active", "active");
    state.filters.countries = refs.countryFilter
      ? Array.from(refs.countryFilter.selectedOptions).map(option => option.value)
      : [];
    state.filters.city = refs.cityFilter ? refs.cityFilter.value : "";
    state.filters.sector = refs.sectorFilter ? refs.sectorFilter.value : "";
    state.filters.profession = refs.professionFilter ? refs.professionFilter.value : "";
    state.filters.newOnly = Boolean(state.filters.newOnly);
    state.filters.excludeInternship = Boolean(state.filters.excludeInternship);
    state.filters.search = refs.searchFilter ? refs.searchFilter.value.trim() : "";
    state.filters.sort = refs.sortFilter ? refs.sortFilter.value : "relevance";
    updateCountrySelectionBadge();
    updateCountryPickerTrigger();
    updateQuickChipStates();
    updateActiveFiltersSummary();
  }

  function onFilterChange() {
    jobsDispatch.dispatch({
      type: JOBS_ACTIONS.FILTERS_CHANGED,
      payload: { signature: JSON.stringify(state.filters || {}) }
    });
    syncStateFromFilters();
    applyFiltersAndRender({ resetPage: true });
  }

  function resetFilters() {
    state.filters = { ...defaultFilters, countries: Array.from(defaultFilters.countries || []) };
    if (refs.professionSearchFilter) refs.professionSearchFilter.value = "";
    renderProfessionOptions("");
    applyStateToFilters();
  }

  function updateFilterOptions(allJobs, { precomputed = null } = {}) {
    if (!refs.workTypeFilter || !refs.countryFilter || !refs.professionFilter || !refs.cityFilter || !refs.sectorFilter) return;
    const {
      availableCountries: nextAvailableCountries,
      availableCountryFilterValues: nextAvailableCountryFilterValues,
      availableProfessions: nextAvailableProfessions,
      availableCities,
      availableSectors
    } = precomputed || buildFilterOptions(allJobs, {
      getJobLocationCities,
      getJobLocationCountries,
      isValidCountry,
      getAvailableRegionOptions: getAvailableRegionOptionsForJobs,
      fullCountryName: fullCountryNameForJobs,
      isSemanticallyValidLocationValue
    });

    availableCountries = nextAvailableCountries;
    availableCountryFilterValues = nextAvailableCountryFilterValues;

    refs.countryFilter.innerHTML = "";
    availableCountryFilterValues.forEach(country => {
      const opt = document.createElement("option");
      opt.value = country;
      opt.textContent = getCountryFilterOptionLabelForJobs(country);
      refs.countryFilter.appendChild(opt);
    });
    renderCountryPickerOptions(refs.countryPickerSearch ? refs.countryPickerSearch.value : "");

    refs.cityFilter.innerHTML = '<option value="">All Cities</option>';
    availableCities.forEach(city => {
      const opt = document.createElement("option");
      opt.value = city;
      opt.textContent = city;
      refs.cityFilter.appendChild(opt);
    });

    refs.sectorFilter.innerHTML = '<option value="">All Sectors</option>';
    availableSectors.forEach(sector => {
      const opt = document.createElement("option");
      opt.value = sector;
      opt.textContent = sector;
      refs.sectorFilter.appendChild(opt);
    });

    availableProfessions = nextAvailableProfessions;
    renderProfessionOptions(refs.professionSearchFilter ? refs.professionSearchFilter.value : "");
    updateCountrySelectionBadge();
    updateCountryPickerTrigger();
  }

  function loadQuickFilterPreferences() {
    const defaults = getDefaultQuickFilterKeys(quickFilters);
    const parsed = readQuickFilterPreferences(QUICK_FILTER_PREFS_KEY, defaults);
    return sanitizeQuickFilterKeys(parsed, quickFilters);
  }

  function saveQuickFilterPreferences() {
    writeQuickFilterPreferences(QUICK_FILTER_PREFS_KEY, visibleQuickFilterKeys);
  }

  function renderQuickFilters() {
    if (!refs.quickActionsEl) return;
    refs.quickActionsEl.innerHTML = renderQuickFiltersHtml(visibleQuickFilterKeys, quickFilters);
    updateQuickChipStates();
  }

  function renderQuickFilterOptions() {
    if (!refs.quickFiltersOptionsEl) return;
    refs.quickFiltersOptionsEl.innerHTML = renderQuickFilterOptionsHtml(visibleQuickFilterKeys, quickFilters);
  }

  function initializeQuickFilters() {
    visibleQuickFilterKeys = loadQuickFilterPreferences();
    renderQuickFilters();
    renderQuickFilterOptions();
  }

  function setQuickFilterVisibility(key, visible) {
    visibleQuickFilterKeys = getNextQuickFilterKeys(visibleQuickFilterKeys, key, visible, quickFilters);
    saveQuickFilterPreferences();
    renderQuickFilters();
    renderQuickFilterOptions();
  }

  function resetQuickFilterPreferences() {
    visibleQuickFilterKeys = getDefaultQuickFilterKeys(quickFilters);
    saveQuickFilterPreferences();
    renderQuickFilters();
    renderQuickFilterOptions();
  }

  function toggleQuickFiltersPanel() {
    if (!refs.quickFiltersPanel) return;
    const hidden = refs.quickFiltersPanel.classList.contains("hidden");
    if (hidden) {
      renderQuickFilterOptions();
      refs.quickFiltersPanel.classList.remove("hidden");
      if (refs.customizeQuickFiltersBtn) refs.customizeQuickFiltersBtn.setAttribute("aria-expanded", "true");
      return;
    }
    closeQuickFiltersPanel();
  }

  function closeQuickFiltersPanel() {
    if (!refs.quickFiltersPanel) return;
    refs.quickFiltersPanel.classList.add("hidden");
    if (refs.customizeQuickFiltersBtn) refs.customizeQuickFiltersBtn.setAttribute("aria-expanded", "false");
  }

  function applyQuickFilter(quick) {
    const item = quickFilters.find(filter => filter.key === quick);
    if (!item) return;
    if (item.type === "clear") {
      resetFilters();
      return;
    }
    applyQuickFilterToState(quick, state.filters, quickFilters, { toggleCountrySelection });
  }

  function toggleCountryPickerPanel() {
    if (!refs.countryPickerPanel) return;
    const isHidden = refs.countryPickerPanel.classList.contains("hidden");
    if (isHidden) {
      refs.countryPickerPanel.classList.remove("hidden");
      if (refs.countryPickerBtn) refs.countryPickerBtn.setAttribute("aria-expanded", "true");
      if (refs.countryPickerSearch) refs.countryPickerSearch.focus();
      return;
    }
    closeCountryPickerPanel();
  }

  function closeCountryPickerPanel() {
    if (!refs.countryPickerPanel) return;
    refs.countryPickerPanel.classList.add("hidden");
    if (refs.countryPickerBtn) refs.countryPickerBtn.setAttribute("aria-expanded", "false");
  }

  return {
    updateFilterOptions,
    applyStateToStaticFilters,
    applyStateToFilters,
    syncStateFromFilters,
    resetFilters,
    onFilterChange,
    updateCountrySelectionBadge,
    toggleCountrySelection,
    resolveCountryCode,
    syncCountryPickerChecks,
    renderCountryPickerOptions,
    toggleCountryPickerPanel,
    closeCountryPickerPanel,
    initializeQuickFilters,
    loadQuickFilterPreferences,
    saveQuickFilterPreferences,
    renderQuickFilters,
    renderQuickFilterOptions,
    setQuickFilterVisibility,
    resetQuickFilterPreferences,
    toggleQuickFiltersPanel,
    closeQuickFiltersPanel,
    applyQuickFilter,
    updateCountryPickerTrigger,
    updateQuickChipStates,
    updateActiveFiltersSummary,
    renderProfessionOptions
  };
}
