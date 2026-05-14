const UNKNOWN_LOCATION_TOKENS = new Set(["", "unknown", "n/a", "na", "none", "blank"]);
const WORK_MODE_LOCATION_TOKENS = new Set([
  "remote",
  "fully remote",
  "hybrid",
  "onsite",
  "on-site",
  "on site",
  "office",
  "worldwide"
]);
const COUNTRY_CODE_LOCATION_RE = /^[a-z]{2}\s*-\s*/i;
const COUNTRY_CODE_BUNDLE_RE = /^[a-z]{2}\s*-\s*[^;]+(?:\s*;\s*[a-z]{2}\s*-\s*[^;]+)+$/i;

function cleanToken(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function lowerToken(value) {
  return cleanToken(value).toLowerCase();
}

function isUnknownToken(value) {
  return UNKNOWN_LOCATION_TOKENS.has(lowerToken(value));
}

function isWorkModeToken(value, workType = "") {
  const lowered = lowerToken(value);
  if (!lowered) return false;
  if (WORK_MODE_LOCATION_TOKENS.has(lowered)) return true;
  return Boolean(workType && lowered === lowerToken(workType));
}

function cleanLocationPart(value, { workType = "" } = {}) {
  const text = cleanToken(value).replace(/,\s*unknown$/i, "").trim();
  if (!text || isUnknownToken(text) || isWorkModeToken(text, workType)) return "";
  if (text.startsWith("(none)") || COUNTRY_CODE_LOCATION_RE.test(text) || COUNTRY_CODE_BUNDLE_RE.test(text)) return "";
  return text;
}

function cleanCountryPart(value, { fullCountryName, workType = "" } = {}) {
  const raw = cleanToken(value);
  if (!raw || isUnknownToken(raw) || isWorkModeToken(raw, workType)) return "";
  const label = cleanToken(typeof fullCountryName === "function" ? fullCountryName(raw) : raw);
  if (!label || isUnknownToken(label) || isWorkModeToken(label, workType)) return "";
  return label;
}

function includesCountrySuffix(city, country) {
  if (!city || !country) return false;
  return lowerToken(city).endsWith(`, ${lowerToken(country)}`);
}

function makeLabel(city, country) {
  if (city && country && includesCountrySuffix(city, country)) return city;
  return [city, country].filter(Boolean).join(", ");
}

function collectLocationLabels(job, options) {
  const locations = Array.isArray(job?.locations) ? job.locations : [];
  const labels = [];
  const seen = new Set();
  for (const location of locations) {
    const city = cleanLocationPart(location?.city, options);
    const country = cleanCountryPart(location?.country, options);
    const label = makeLabel(city, country);
    const key = lowerToken(label);
    if (!label || seen.has(key)) continue;
    seen.add(key);
    labels.push(label);
  }
  return labels;
}

function cleanSummary(summary, options) {
  const text = cleanToken(summary);
  if (!text || isUnknownToken(text) || isWorkModeToken(text, options.workType)) return "";
  return text
    .split("|")
    .map(part => cleanToken(part))
    .map(part => part.replace(/,\s*unknown$/i, "").trim())
    .filter(part =>
      part &&
      !isUnknownToken(part) &&
      !isWorkModeToken(part, options.workType) &&
      !COUNTRY_CODE_LOCATION_RE.test(part) &&
      !COUNTRY_CODE_BUNDLE_RE.test(part) &&
      lowerToken(part) !== "remote, remote"
    )
    .filter((part, index, parts) => parts.findIndex(candidate => lowerToken(candidate) === lowerToken(part)) === index)
    .join(" | ");
}

export function formatCompactJobLocation(job, {
  fullCountryName = value => String(value || "")
} = {}) {
  const options = {
    fullCountryName,
    workType: job?.workType || ""
  };
  const locationLabels = collectLocationLabels(job, options);
  if (locationLabels.length > 0) return locationLabels.join(" | ");

  const summary = cleanSummary(job?.locationSummary, options);
  if (summary) return summary;

  const city = cleanLocationPart(job?.city, options);
  const country = cleanCountryPart(job?.country, options);
  return makeLabel(city, country);
}

export function formatJobLocationColumns(job, {
  fullCountryName = value => String(value || "")
} = {}) {
  const options = {
    fullCountryName,
    workType: job?.workType || ""
  };
  const locations = Array.isArray(job?.locations) ? job.locations : [];
  const firstLocation = locations.length === 1 ? locations[0] : null;
  const singleCity = cleanLocationPart(firstLocation?.city || job?.city, options);
  const singleCountry = cleanCountryPart(firstLocation?.country || job?.country, options);
  const summary = cleanSummary(job?.locationSummary, options);
  const singleLabel = makeLabel(singleCity, singleCountry);
  if (
    locations.length <= 1 &&
    singleCity &&
    singleCountry &&
    !includesCountrySuffix(singleCity, singleCountry) &&
    (!summary || lowerToken(summary) === lowerToken(singleLabel))
  ) {
    return {
      cityLabel: singleCity,
      countryLabel: singleCountry
    };
  }

  const compact = formatCompactJobLocation(job, { fullCountryName });
  if (!compact) return { cityLabel: "", countryLabel: "" };

  const country = cleanCountryPart(job?.country, options);
  const canShowCountrySeparately =
    country &&
    !compact.includes("|") &&
    !includesCountrySuffix(compact, country) &&
    lowerToken(compact) !== lowerToken(country);

  return {
    cityLabel: compact,
    countryLabel: canShowCountrySeparately ? country : ""
  };
}
