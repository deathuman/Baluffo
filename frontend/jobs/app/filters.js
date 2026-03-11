export function normalizeLifecycleStatus(value, fallback = "active") {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) return fallback;
  if (normalized === "active" || normalized === "likely_removed" || normalized === "archived") return normalized;
  return fallback;
}

export function optionExists(select, value) {
  if (!value) return true;
  return Array.from(select?.options || []).some(option => option.value === value);
}

export function isRegionSelection(value) {
  return String(value || "").startsWith("region:");
}

export function getCountryFilterOptionLabel(value, {
  regionDefinitions,
  fullCountryName,
  countryNameOptions
}) {
  const region = regionDefinitions.find(item => item.value === value);
  if (region) return region.label;
  return fullCountryName(value, countryNameOptions);
}

export function resolveRegionSelection(value, {
  normalizeCountryToken,
  regionDefinitions
}) {
  const normalized = normalizeCountryToken(value);
  if (!normalized) return "";
  const match = regionDefinitions.find(region =>
    normalizeCountryToken(region.value) === normalized || normalizeCountryToken(region.label) === normalized
  );
  return match ? match.value : "";
}

export function getAvailableRegionOptions(countries, {
  canonicalizeCountryName,
  normalizeCountryToken,
  regionDefinitions,
  regionCountryTokenLookup,
  countryNameOptions
}) {
  const countryTokens = new Set(
    (countries || [])
      .map(item => normalizeCountryToken(canonicalizeCountryName(item, countryNameOptions)))
      .filter(Boolean)
  );

  return regionDefinitions.filter(region => {
    const regionTokens = regionCountryTokenLookup[region.value];
    if (!regionTokens || regionTokens.size === 0) return false;

    for (const token of regionTokens) {
      if (countryTokens.has(token)) return true;
    }
    return false;
  });
}

export function countryMatchesRegion(countryToken, regionValue, {
  regionCountryTokenLookup,
  remoteWorldwideTokens
}) {
  const regionCountries = regionCountryTokenLookup[regionValue];
  if (!regionCountries || regionCountries.size === 0) return false;
  if (regionValue === "region:remote-worldwide") {
    return remoteWorldwideTokens.has(countryToken);
  }
  return regionCountries.has(countryToken);
}

export function matchesCountrySelection(jobCountry, selections, {
  canonicalizeCountryName,
  normalizeCountryToken,
  countryNameOptions,
  regionCountryMatcher
}) {
  const countryToken = normalizeCountryToken(canonicalizeCountryName(jobCountry, countryNameOptions));
  if (!countryToken) return false;

  for (const selection of selections || []) {
    if (isRegionSelection(selection)) {
      if (regionCountryMatcher(countryToken, selection)) return true;
      continue;
    }

    const selectionToken = normalizeCountryToken(canonicalizeCountryName(selection, countryNameOptions));
    if (selectionToken && selectionToken === countryToken) return true;
  }
  return false;
}

export function resolveCountryCode(countryCode, {
  availableCountries,
  availableCountryFilterValues,
  resolveRegionValue,
  canonicalizeCountryName,
  normalizeCountryToken,
  countryNameOptions
}) {
  const raw = String(countryCode || "").trim();
  if (!raw) return "";

  const regionValue = resolveRegionValue(raw);
  if (regionValue && availableCountryFilterValues.includes(regionValue)) {
    return regionValue;
  }

  if (availableCountryFilterValues.includes(raw)) return raw;

  const normalized = normalizeCountryToken(canonicalizeCountryName(raw, countryNameOptions));
  const byName = availableCountries.find(
    code => normalizeCountryToken(canonicalizeCountryName(code, countryNameOptions)) === normalized
  );
  return byName || "";
}

export function normalizeSelectedCountries(countries, {
  resolveCountryCode,
  availableCountryFilterValues
}) {
  return Array.from(
    new Set(
      (countries || [])
        .map(resolveCountryCode)
        .filter(code => availableCountryFilterValues.includes(code))
    )
  );
}

export function getCountrySelectionBadgeText(countries) {
  const count = Array.isArray(countries) ? countries.length : 0;
  if (count === 0) return "All countries";
  return count === 1 ? "1 country selected" : `${count} countries selected`;
}

export function renderCountryPickerOptionsHtml({
  availableCountryFilterValues,
  selectedCountries,
  query,
  getCountryFilterOptionLabel,
  escapeHtml
}) {
  const normalized = String(query || "").trim().toLowerCase();
  const selected = new Set(selectedCountries || []);
  const rows = availableCountryFilterValues.filter(code => {
    if (!normalized) return true;
    const label = getCountryFilterOptionLabel(code).toLowerCase();
    return label.includes(normalized);
  });

  if (rows.length === 0) {
    return '<div class="country-empty">No matches.</div>';
  }

  return rows.map(code => `
    <label class="country-option">
      <input type="checkbox" value="${escapeHtml(code)}" ${selected.has(code) ? "checked" : ""}>
      <span>${escapeHtml(getCountryFilterOptionLabel(code))}</span>
    </label>
  `).join("");
}

export function getCountryPickerTriggerText(countries) {
  const count = Array.isArray(countries) ? countries.length : 0;
  return count > 0 ? `Country: ${count} selected` : "Country: All";
}

export function getDefaultQuickFilterKeys(quickFilters) {
  return quickFilters.filter(item => item.defaultVisible).map(item => item.key);
}

export function orderQuickFilterKeys(keys, quickFilters) {
  const set = new Set(keys);
  return quickFilters.filter(item => set.has(item.key)).map(item => item.key);
}

export function sanitizeQuickFilterKeys(parsed, quickFilters) {
  const defaults = getDefaultQuickFilterKeys(quickFilters);
  if (!Array.isArray(parsed)) return defaults;
  const valid = parsed.filter(key => quickFilters.some(item => item.key === key));
  const keepClear = valid.includes("clear") ? valid : [...valid, "clear"];
  return orderQuickFilterKeys(keepClear, quickFilters);
}

export function renderQuickFiltersHtml(visibleQuickFilterKeys, quickFilters) {
  return visibleQuickFilterKeys
    .map(key => {
      const item = quickFilters.find(filter => filter.key === key);
      if (!item) return "";
      const isClear = item.type === "clear";
      const classes = isClear ? "btn quick-btn quick-clear" : "btn quick-btn quick-chip";
      const ariaPressed = isClear ? "" : ' aria-pressed="false"';
      return `<button class="${classes}" data-quick="${item.key}"${ariaPressed}>${item.label}</button>`;
    })
    .join("");
}

export function renderQuickFilterOptionsHtml(visibleQuickFilterKeys, quickFilters) {
  const current = new Set(visibleQuickFilterKeys);
  return quickFilters
    .filter(item => item.type !== "clear")
    .map(item => `
      <label class="quick-filter-option">
        <input type="checkbox" data-quick="${item.key}" ${current.has(item.key) ? "checked" : ""}>
        <span>${item.label}</span>
      </label>
    `)
    .join("");
}

export function getNextQuickFilterKeys(visibleQuickFilterKeys, key, visible, quickFilters) {
  const item = quickFilters.find(filter => filter.key === key && filter.type !== "clear");
  if (!item) return visibleQuickFilterKeys;
  const next = new Set(visibleQuickFilterKeys);
  if (visible) next.add(key);
  else next.delete(key);
  next.add("clear");
  return orderQuickFilterKeys(Array.from(next), quickFilters);
}

export function applyQuickFilterToState(quick, filters, quickFilters, {
  toggleCountrySelection
}) {
  const item = quickFilters.find(filter => filter.key === quick);
  if (!item) return;

  if (item.type === "clear") {
    filters.workType = "";
    filters.lifecycleStatus = "active";
    filters.countries = [];
    filters.city = "";
    filters.sector = "";
    filters.profession = "";
    filters.newOnly = false;
    filters.excludeInternship = false;
    filters.search = "";
    filters.sort = "relevance";
    return;
  }
  if (item.type === "workType") {
    filters.workType = filters.workType === item.value ? "" : item.value;
    return;
  }
  if (item.type === "profession") {
    filters.profession = filters.profession === item.value ? "" : item.value;
    return;
  }
  if (item.type === "sector") {
    filters.sector = filters.sector === item.value ? "" : item.value;
    return;
  }
  if (item.type === "country") {
    toggleCountrySelection(item.value);
    return;
  }
  if (item.type === "flag" && typeof item.value === "string" && item.value in filters) {
    filters[item.value] = !Boolean(filters[item.value]);
  }
}

export function isQuickFilterActive(item, filters, {
  resolveCountryCode
}) {
  if (!item) return false;
  if (item.type === "workType") return filters.workType === item.value;
  if (item.type === "profession") return filters.profession === item.value;
  if (item.type === "sector") return filters.sector === item.value;
  if (item.type === "flag" && typeof item.value === "string") return Boolean(filters[item.value]);
  if (item.type === "country") {
    const mapped = resolveCountryCode(item.value);
    return Boolean(mapped) && filters.countries.includes(mapped);
  }
  return false;
}

export function getActiveFilterSummaryItems(filters, {
  professionLabels
}) {
  const active = [];
  if (filters.workType) active.push(filters.workType);
  if (filters.lifecycleStatus && filters.lifecycleStatus !== "active") {
    active.push(`Status: ${filters.lifecycleStatus.replace("_", " ")}`);
  }
  if (filters.countries.length > 0) active.push(`Countries: ${filters.countries.length}`);
  if (filters.city) active.push(`City: ${filters.city}`);
  if (filters.sector) active.push(`Sector: ${filters.sector}`);
  if (filters.profession) active.push(professionLabels[filters.profession] || filters.profession);
  if (filters.newOnly) active.push("New Only");
  if (filters.excludeInternship) active.push("Exclude Internship");
  if (filters.search) active.push(`Search: "${filters.search}"`);
  return active;
}
