export function isCleanFilterOptionValue(value, {
  isSemanticallyValidLocationValue = () => true
} = {}) {
  const text = String(value || "").trim();
  if (!text) return false;
  if (text.includes("<") || text.includes(">")) return false;
  return Boolean(isSemanticallyValidLocationValue(text, "city"));
}

export function buildFilterOptions(allJobs, {
  getJobLocationCities = () => [],
  getJobLocationCountries = () => [],
  isValidCountry = () => true,
  getAvailableRegionOptions = () => [],
  fullCountryName = value => String(value || "")
} = {}) {
  const countries = new Set();
  const professions = new Set();
  const cities = new Set();
  const sectors = new Set();

  (allJobs || []).forEach(job => {
    getJobLocationCountries(job).forEach(country => {
      if (isValidCountry(country)) countries.add(country);
    });
    if (job.profession) professions.add(job.profession);
    getJobLocationCities(job).forEach(city => {
      if (city && isCleanFilterOptionValue(city, { isSemanticallyValidLocationValue: () => true })) {
        cities.add(city);
      }
    });
    if (job.sector) sectors.add(job.sector);
  });

  const availableCountries = Array.from(countries).sort((a, b) =>
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
    availableProfessions: Array.from(professions).sort(),
    availableCities: Array.from(cities).sort(),
    availableSectors: Array.from(sectors).sort((a, b) => a.localeCompare(b))
  };
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
    const lifecycleStatus = String(job.status || "active").toLowerCase() || "active";
    const matchesLifecycle = !filters?.lifecycleStatus || lifecycleStatus === filters.lifecycleStatus;
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
  if (sortMode === "relevance") {
    sorted.sort((a, b) => {
      const aScore = a.freshnessScore ?? 101;
      const bScore = b.freshnessScore ?? 101;
      if (aScore !== bScore) return aScore - bScore;
      return String(a.title || "").localeCompare(String(b.title || ""));
    });
    return sorted;
  }
  if (sortMode === "title-asc") {
    sorted.sort((a, b) => String(a.title || "").localeCompare(String(b.title || "")));
    return sorted;
  }
  if (sortMode === "company-asc") {
    sorted.sort((a, b) => String(a.company || "").localeCompare(String(b.company || "")));
    return sorted;
  }
  if (sortMode === "country-asc") {
    sorted.sort((a, b) => fullCountryName(a.country).localeCompare(fullCountryName(b.country)));
    return sorted;
  }
  if (sortMode === "remote-first") {
    const order = { Remote: 0, Hybrid: 1, Onsite: 2 };
    sorted.sort((a, b) => {
      const diff = (order[a.workType] ?? 99) - (order[b.workType] ?? 99);
      if (diff !== 0) return diff;
      return String(a.title || "").localeCompare(String(b.title || ""));
    });
  }
  return sorted;
}
