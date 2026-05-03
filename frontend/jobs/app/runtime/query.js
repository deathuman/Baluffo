export function isCleanFilterOptionValue(value, {
  isSemanticallyValidLocationValue = () => true
} = {}) {
  const text = String(value || "").trim();
  if (!text) return false;
  if (text.includes("<") || text.includes(">")) return false;
  return Boolean(isSemanticallyValidLocationValue(text, "city"));
}

export function createFilterOptionsAccumulator() {
  return {
    countries: new Set(),
    professions: new Set(),
    cities: new Set(),
    sectors: new Set()
  };
}

export function addJobToFilterOptions(accumulator, job, {
  getJobLocationCities = () => [],
  getJobLocationCountries = () => [],
  isSemanticallyValidLocationValue = () => true,
  isValidCountry = () => true
} = {}) {
  getJobLocationCountries(job).forEach(country => {
    if (isValidCountry(country)) accumulator.countries.add(country);
  });
  if (job.profession) accumulator.professions.add(job.profession);
  getJobLocationCities(job).forEach(city => {
    if (city && isCleanFilterOptionValue(city, { isSemanticallyValidLocationValue })) {
      accumulator.cities.add(city);
    }
  });
  if (job.sector) accumulator.sectors.add(job.sector);
}

export function finalizeFilterOptions(accumulator, {
  getAvailableRegionOptions = () => [],
  fullCountryName = value => String(value || "")
} = {}) {
  const availableCountries = Array.from(accumulator?.countries || []).sort((a, b) =>
    fullCountryName(a).localeCompare(fullCountryName(b))
  );
  const availableRegions = getAvailableRegionOptions(availableCountries);
  const availableCountryFilterValues = [
    ...availableRegions.map(region => region.value),
    ...availableCountries
  ];

  return {
    availableCountries,
    availableRegions,
    availableCountryFilterValues,
    availableProfessions: Array.from(accumulator?.professions || []).sort(),
    availableCities: Array.from(accumulator?.cities || []).sort(),
    availableSectors: Array.from(accumulator?.sectors || []).sort((a, b) => a.localeCompare(b))
  };
}

export function buildFilterOptions(allJobs, {
  getJobLocationCities = () => [],
  getJobLocationCountries = () => [],
  isSemanticallyValidLocationValue = () => true,
  isValidCountry = () => true,
  getAvailableRegionOptions = () => [],
  fullCountryName = value => String(value || "")
} = {}) {
  const accumulator = createFilterOptionsAccumulator();
  (allJobs || []).forEach(job => {
    addJobToFilterOptions(accumulator, job, {
      getJobLocationCities,
      getJobLocationCountries,
      isSemanticallyValidLocationValue,
      isValidCountry
    });
  });
  return finalizeFilterOptions(accumulator, {
    getAvailableRegionOptions,
    fullCountryName
  });
}

export function jobMatchesLifecycleFilter(job, lifecycleFilter) {
  if (lifecycleFilter === undefined || lifecycleFilter === null || lifecycleFilter === "") return true;
  const filterValue = String(lifecycleFilter || "active").toLowerCase();
  const status = String(job?.status || "active").toLowerCase() || "active";
  const lifecycleEvent = String(job?.lifecycleEvent || "").toLowerCase();
  const lifecycleReason = String(job?.lifecycleReason || "").toLowerCase();

  if (filterValue === "all") return true;
  if (filterValue === "reappeared") return lifecycleEvent === "reappeared";
  if (filterValue === "preserved_source_failed") {
    return lifecycleEvent === "preserved" && lifecycleReason === "source_failed";
  }
  return status === filterValue;
}

export function filterJobs(allJobs, filters, {
  currentUser = null,
  seenJobKeys = new Set(),
  getJobKeyForJob = job => String(job?.id || ""),
  getJobLocationCities = () => [],
  getJobLocationCountries = () => [],
  isInternshipJob = () => false,
  matchesCountrySelection = () => false
} = {}) {
  const searchTerm = String(filters?.search || "").toLowerCase();
  const filterCountries = Array.from(filters?.countries || []);

  return (allJobs || []).filter(job => {
    const matchesWorkType = !filters?.workType || job.workType === filters.workType;
    const matchesLifecycle = jobMatchesLifecycleFilter(job, filters?.lifecycleStatus);
    const locationCities = getJobLocationCities(job);
    const locationCountries = getJobLocationCountries(job);
    const matchesCountry = filterCountries.length === 0
      || locationCountries.some(country => matchesCountrySelection(country, filterCountries));
    const matchesCity = !filters?.city || locationCities.includes(filters.city);
    const matchesSector = !filters?.sector || job.sector === filters.sector;
    const matchesProfession = !filters?.profession || job.profession === filters.profession;
    const jobKey = getJobKeyForJob(job);
    const matchesNewOnly = !filters?.newOnly || !currentUser || !seenJobKeys.has(jobKey);
    const matchesInternship = !filters?.excludeInternship || !isInternshipJob(job);
    const matchesSearch =
      !searchTerm ||
      String(job.title || "").toLowerCase().includes(searchTerm) ||
      String(job.company || "").toLowerCase().includes(searchTerm) ||
      String(job.city || "").toLowerCase().includes(searchTerm) ||
      String(job.sector || "").toLowerCase().includes(searchTerm) ||
      String(job.locationSummary || "").toLowerCase().includes(searchTerm) ||
      locationCities.some(value => String(value || "").toLowerCase().includes(searchTerm)) ||
      locationCountries.some(value => String(value || "").toLowerCase().includes(searchTerm));

    return matchesWorkType
      && matchesLifecycle
      && matchesCountry
      && matchesCity
      && matchesSector
      && matchesProfession
      && matchesNewOnly
      && matchesInternship
      && matchesSearch;
  });
}

export function sortJobs(jobs, sortMode, {
  fullCountryName = value => String(value || "")
} = {}) {
  const sorted = Array.from(jobs || []);
  sorted.sort((a, b) => compareJobsForSort(a, b, sortMode, { fullCountryName }));
  return sorted;
}

export function compareJobsForSort(a, b, sortMode, {
  fullCountryName = value => String(value || "")
} = {}) {
  if (sortMode === "relevance") {
    const aScore = a.freshnessScore ?? 101;
    const bScore = b.freshnessScore ?? 101;
    if (aScore !== bScore) return aScore - bScore;
    return String(a.title || "").localeCompare(String(b.title || ""));
  }
  if (sortMode === "title-asc") {
    return String(a.title || "").localeCompare(String(b.title || ""));
  }
  if (sortMode === "company-asc") {
    return String(a.company || "").localeCompare(String(b.company || ""));
  }
  if (sortMode === "country-asc") {
    return fullCountryName(a.country).localeCompare(fullCountryName(b.country));
  }
  if (sortMode === "remote-first") {
    const order = { Remote: 0, Hybrid: 1, Onsite: 2 };
    const diff = (order[a.workType] ?? 99) - (order[b.workType] ?? 99);
    if (diff !== 0) return diff;
    return String(a.title || "").localeCompare(String(b.title || ""));
  }
  return 0;
}
