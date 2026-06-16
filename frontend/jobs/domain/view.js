import {
  parseTimestampMs,
  getCityFilterOptionValues,
  sanitizeLocationField
} from "./query.js?v=1";

const DAY_MS = 24 * 60 * 60 * 1000;

export function getJobLocationCities(job) {
  const locations = Array.isArray(job?.locations) ? job.locations : [];
  const cities = [];
  const seen = new Set();
  for (const location of locations) {
    getCityFilterOptionValues(location?.city || "", location?.country || "").forEach(city => {
      if (!city || seen.has(city)) return;
      seen.add(city);
      cities.push(city);
    });
  }
  if (cities.length === 0) {
    getCityFilterOptionValues(job?.city || "", job?.country || "").forEach(city => {
      if (!city || seen.has(city)) return;
      seen.add(city);
      cities.push(city);
    });
  }
  return cities;
}

export function getJobLocationCountries(job) {
  const locations = Array.isArray(job?.locations) ? job.locations : [];
  const countries = [];
  const seen = new Set();
  for (const location of locations) {
    const country = sanitizeLocationField(location?.country || "", "country");
    if (!country || seen.has(country)) continue;
    seen.add(country);
    countries.push(country);
  }
  if (countries.length === 0) {
    const fallbackCountry = sanitizeLocationField(job?.country || "", "country");
    if (fallbackCountry) countries.push(fallbackCountry);
  }
  return countries;
}

export function mapFreshnessAgeToScore(ageDays) {
  if (!Number.isFinite(ageDays)) return null;
  const age = Math.max(0, ageDays);
  if (age < 2) {
    return Math.round((age / 2) * 15);
  }
  if (age <= 7) {
    return Math.round(16 + ((age - 2) / 5) * 24);
  }
  if (age <= 21) {
    return Math.round(41 + ((age - 8) / 13) * 29);
  }
  const staleProgress = Math.min(1, (age - 22) / 68);
  return Math.round(71 + staleProgress * 29);
}

export function deriveFreshness(row, options = {}) {
  const nowMs = Number.isFinite(options.nowMs) ? options.nowMs : Date.now();
  const postedMs = parseTimestampMs(row?.postedAt);
  const fetchedMs = parseTimestampMs(row?.fetchedAt);
  const source = postedMs != null ? "postedAt" : (fetchedMs != null ? "fetchedAt" : "");
  const timestampMs = source === "postedAt" ? postedMs : fetchedMs;
  if (!source || timestampMs == null) {
    return {
      freshnessAgeDays: null,
      freshnessScore: null,
      freshnessSource: ""
    };
  }

  const ageDays = Math.max(0, Math.floor((nowMs - timestampMs) / DAY_MS));
  return {
    freshnessAgeDays: ageDays,
    freshnessScore: mapFreshnessAgeToScore(ageDays),
    freshnessSource: source
  };
}
