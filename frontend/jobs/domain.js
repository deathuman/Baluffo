import {
  COUNTRY_ACCEPTANCE,
  resolveCountryAcceptanceValue
} from "../shared/data/country-acceptance.js";
import {
  CITY_NOISE_CONTRACT,
  normalizeCityNoiseText
} from "../shared/data/city-noise.js";

export function detectWorkType(text) {
  if (!text) return "Onsite";
  const lower = String(text).toLowerCase();
  if (lower.includes("remote")) return "Remote";
  if (lower.includes("hybrid") || lower.includes("mixed")) return "Hybrid";
  return "Onsite";
}

export function detectContractType(text, title = "") {
  const lower = `${text} ${title}`.toLowerCase();
  if (lower.includes("internship") || lower.includes("intern ")) return "Internship";
  if (lower.includes("full-time") || lower.includes("full time") || lower.includes("permanent")) return "Full-time";
  if (
    lower.includes("temporary") ||
    lower.includes("temp ") ||
    lower.includes("contract") ||
    lower.includes("fixed-term") ||
    lower.includes("fixed term") ||
    lower.includes("freelance") ||
    lower.includes("part-time") ||
    lower.includes("part time")
  ) {
    return "Temporary";
  }
  return "Unknown";
}

const GAME_SOURCE_FAMILY_HINTS = [
  "8bitplay",
  "epic_games_careers",
  "gamejobs",
  "gamesindustry",
  "gracklehq",
  "workwithindies"
];

const GAME_ROLE_KEYWORDS = [
  "artist",
  "designer",
  "engineer",
  "programmer",
  "animator",
  "technical artist",
  "concept artist",
  "environment artist",
  "character artist",
  "gameplay",
  "level design"
];

function normalizeBundleList(sourceBundle) {
  return Array.isArray(sourceBundle) ? sourceBundle.filter(item => item && typeof item === "object") : [];
}

function hasGameSourceProvenance(source = "", sourceBundle = []) {
  const sourceText = String(source || "").toLowerCase();
  if (sourceText && GAME_SOURCE_FAMILY_HINTS.some(hint => sourceText.includes(hint))) {
    return true;
  }

  for (const item of normalizeBundleList(sourceBundle)) {
    const bundleSource = String(item.source || "").toLowerCase();
    if (bundleSource && GAME_SOURCE_FAMILY_HINTS.some(hint => bundleSource.includes(hint))) {
      return true;
    }
    const studio = String(item.studio || "").trim().toLowerCase();
    const adapter = String(item.adapter || "").trim().toLowerCase();
    if (studio && adapter && !["csv", "static", "scrapy_static"].includes(adapter)) {
      return true;
    }
  }
  return false;
}

function hasPositiveGameEvidence(company = "", title = "", source = "", jobLink = "", sourceBundle = []) {
  const text = `${company} ${title} ${source} ${jobLink}`.toLowerCase();
  if (hasGameSourceProvenance(source, sourceBundle)) {
    return true;
  }
  if (
    /\b(game|gaming|games|esports|gameplay|gamedev|unity|unreal|technical artist|tech artist|shader|material artist|world artist|terrain artist|environment art|environment artist|character artist|engine programmer|graphics programmer|level design|animator)\b/.test(text) ||
    /\b(studio|studios|interactive|publisher|entertainment)\b/.test(text) ||
    text.includes("game")
  ) {
    return true;
  }
  const titleText = String(title || "").toLowerCase();
  const companyToken = String(company || "").toLowerCase().replace(/\s+/g, "");
  if (companyToken && GAME_ROLE_KEYWORDS.some(keyword => titleText.includes(keyword))) {
    const joined = `${source} ${jobLink}`.toLowerCase().replace(/\s+/g, "");
    if (joined.includes(companyToken)) {
      return true;
    }
  }
  return false;
}

export function classifyCompanyType(company, title = "", source = "", jobLink = "", sourceBundle = []) {
  const text = `${company} ${title} ${source} ${jobLink}`.toLowerCase();
  const isGame =
    hasPositiveGameEvidence(company, title, source, jobLink, sourceBundle) ||
    /\b(game|gaming|games|esports|studio|studios|interactive|publisher|entertainment)\b/.test(text) ||
    /\b(gameplay|level design|character artist|environment artist|technical artist|animator)\b/.test(text);
  return isGame ? "Game" : "Tech";
}

export function normalizeSector(text, company = "", title = "", source = "", jobLink = "", sourceBundle = []) {
  const sectorText = String(text || "").trim();
  if (!sectorText) {
    return hasPositiveGameEvidence(company, title, source, jobLink, sourceBundle) ? "Game" : "Tech";
  }
  return hasPositiveGameEvidence(company, title, source, jobLink, sourceBundle) ? "Game" : "Tech";
}

export function mapProfession(title) {
  const lower = String(title || "").toLowerCase();
  if (lower.includes("technical animator")) return "technical-animator";
  if (lower.includes("technical director") || /\btd\b/.test(lower)) return "technical-director";
  if (lower.includes("technical artist")) return "technical-artist";
  if (lower.includes("environment artist")) return "environment-artist";
  if (lower.includes("character artist")) return "character-artist";
  if (/\brigging\b/.test(lower) || /\brigger\b/.test(lower)) return "rigging";
  if (lower.includes("vfx artist") || lower.includes("visual effects artist") || lower.includes("fx artist")) return "vfx-artist";
  if (lower.includes("ui artist") || lower.includes("ux artist") || lower.includes("ui/ux")) return "ui-ux-artist";
  if (lower.includes("concept artist")) return "concept-artist";
  if (lower.includes("3d artist") || lower.includes("3d modeler") || lower.includes("3d modeller")) return "3d-artist";
  if (lower.includes("art director")) return "art-director";
  if (lower.includes("gameplay") || lower.includes("game mechanics")) return "gameplay";
  if (lower.includes("graphics") || lower.includes("rendering") || lower.includes("shader")) return "graphics";
  if (lower.includes("engine") || lower.includes("architecture") || lower.includes("systems")) return "engine";
  if (lower.includes("ai") || lower.includes("artificial intelligence") || lower.includes("behavior")) return "ai";
  if (lower.includes("animator") || lower.includes("animation") || lower.includes("motion animator")) return "animator";
  if (lower.includes("tool") || lower.includes("pipeline") || lower.includes("editor") || (lower.includes("technical") && !lower.includes("artist"))) return "tools";
  if (lower.includes("designer") || lower.includes("level") || lower.includes("game design")) return "designer";
  if (lower.includes("artist") || lower.includes("animation") || lower.includes("visual")) return "3d-artist";
  return "other";
}

/**
 * @param {import('../shared/types.js').CanonicalJob} job
 * @returns {boolean}
 */
export function isInternshipJob(job) {
  const contract = String(job?.contractType || "").toLowerCase();
  if (contract === "internship") return true;
  const text = `${job?.title || ""} ${job?.description || ""}`.toLowerCase();
  return /\bintern(ship)?\b/.test(text);
}

export function normalizeCountryToken(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/g, "");
}

export function canonicalizeCountryName(value, options = {}) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const countryNamesByCode = options.countryNamesByCode || {};
  const aliasToCanonical = options.countryAliasToCanonical || {};
  const countryDisplayNames = options.countryDisplayNames || null;
  const uppercaseRaw = raw.toUpperCase();
  if (countryNamesByCode[uppercaseRaw]) return countryNamesByCode[uppercaseRaw];
  if (/^[A-Z]{2}$/.test(uppercaseRaw) && countryDisplayNames) {
    try {
      const fromIntl = countryDisplayNames.of(uppercaseRaw);
      if (fromIntl && fromIntl !== uppercaseRaw) return fromIntl;
    } catch {
      // Ignore invalid region codes.
    }
  }
  const normalized = normalizeCountryToken(raw);
  if (aliasToCanonical[normalized]) return aliasToCanonical[normalized];
  return raw;
}

export function fullCountryName(code, options = {}) {
  const fromShared = typeof options.fullCountryNameFromData === "function"
    ? options.fullCountryNameFromData(code)
    : "";
  return fromShared || canonicalizeCountryName(code, options);
}

function sanitizeCountryField(value) {
  const text = sanitizePublicText(value);
  if (!text) return "";
  if (text === "Remote") return "Remote";
  if (text.length === 2 && /^[A-Z]+$/.test(text)) return text;
  const resolved = resolveCountryAcceptanceValue(text);
  if (!resolved) return "";
  const normalized = normalizeCountryToken(text);
  if (COUNTRY_ACCEPTANCE.aliasToCanonical.has(normalized)) {
    for (const [code, name] of Object.entries(COUNTRY_ACCEPTANCE.countryNameByCode || {})) {
      if (name === resolved) return code;
    }
  }
  return resolved;
}

function matchesCitySentencePrefix(text, prefix) {
  if (!text || !prefix || !text.startsWith(prefix)) return false;
  if (text.length === prefix.length) return true;
  return !/[a-z0-9]/i.test(text.charAt(prefix.length));
}

function isCityNoiseFragment(value) {
  const text = normalizeCityNoiseText(value);
  if (!text) return false;
  const contract = CITY_NOISE_CONTRACT || {};
  if (Array.isArray(contract.knownJunkTokens) && contract.knownJunkTokens.includes(text)) return true;
  if (Array.isArray(contract.proseFragments) && contract.proseFragments.some(fragment => text.includes(fragment))) return true;
  if (Array.isArray(contract.placeholderFragments) && contract.placeholderFragments.some(fragment => text.includes(fragment))) return true;
  return Array.isArray(contract.sentencePrefixes) && contract.sentencePrefixes.some(prefix => matchesCitySentencePrefix(text, prefix));
}

export function isValidCountry(country) {
  return Boolean(sanitizeCountryField(country));
}

function normalizeTimestamp(value) {
  if (!value) return "";
  let dt = null;
  if (typeof value === "number" && Number.isFinite(value)) {
    const ms = value > 10_000_000_000 ? value : value * 1000;
    dt = new Date(ms);
  } else {
    const trimmed = String(value).trim();
    if (!trimmed) return "";
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric) && /^\d{10,13}$/.test(trimmed)) {
      const ms = numeric > 10_000_000_000 ? numeric : numeric * 1000;
      dt = new Date(ms);
    } else {
      dt = new Date(trimmed);
    }
  }
  if (!dt || Number.isNaN(dt.getTime())) return "";
  return dt.toISOString();
}

export function sanitizePublicText(value) {
  const decoded = String(value || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, "\"")
    .replace(/&#39;/gi, "'");
  const stripped = decoded.replace(/<[^>]+>/gi, " ");
  const normalized = stripped.replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  if (normalized.includes("<") || normalized.includes(">")) return "";
  const lowered = normalized.toLowerCase();
  if (["div", "/div", "span", "/span", "cb", "location", "title"].includes(lowered)) return "";
  return normalized;
}

const LOCATION_NOISE_PATTERNS = [
  /\b(requirements?|responsibilit(?:y|ies)|qualifications?|experience|register|registration|apply|position|positions)\b/i,
  /\b(business level|job description|preferred|benefits?|contact us)\b/i,
  /\b(open jobs?|followers?|following|connections?|employees?)\b/i,
  /\b(report this post|view all jobs|job postings?|all jobs)\b/i,
  /\b(job|jobs|career|careers|hiring|quiz|game|artist|animator|designer|developer|engineer|programmer|producer|director|writer|specialist|manager|intern|freelanc(?:e|ing)|technical)\b/i,
  /(?:https?:\/\/|www\.)/i
];
const LOCATION_CSS_NOISE_RE = /(?:--|var\(|calc\(|box-shadow|grid-gutter)/i;
const LOCATION_ADDRESS_NOISE_RE = /\b\d[^\n]*\b(?:street|st\.?|avenue|ave\.?|road|rd\.?|boulevard|blvd\.?|drive|dr\.?|lane|ln\.?|way|parkway|pkwy\.?|suite|ste\.?|apt\.?|unit|floor|fl\.?|building|bldg\.?)/i;
const LOCATION_POSTAL_CODE_RE = /\b\d{2,6}(?:-\d{2,4})?\b/;
const LOCATION_SCRIPT_NOISE_RE = /(?:document\.|addEventListener|DOMContentLoaded|querySelector|innerHTML|setTimeout|console\.|function\s*\(|\{\{|\}\})/i;
const LOCATION_ROLE_BLOB_RE = /\b(administratif|administration|assistant|assistante|gestion|human resources|hr|office|operations?|coordination|support)\b/i;

function invalidLocationReason(value, field = "city") {
  const text = sanitizePublicText(value);
  if (!text) return "";
  const lowered = text.toLowerCase();
  if (["unknown", "n/a", "na", "none", "remote", "hybrid", "onsite", "on-site", "worldwide"].includes(lowered)) {
    return "";
  }
  if (field === "city" && resolveCountryAcceptanceValue(text)) {
    return `invalid_${field}_semantic_noise`;
  }
  if (/^\d+$/u.test(text)) return `invalid_${field}_semantic_noise`;
  if (!/[\p{L}\p{N}]/u.test(text)) return `invalid_${field}_semantic_noise`;
  if (text.length > 120) return `invalid_${field}_semantic_overlong`;
  if (text.length > 72 && ((text.match(/,/g) || []).length >= 3 || (text.match(/;/g) || []).length >= 2)) {
    return `invalid_${field}_semantic_multi_location_blob`;
  }
  if (text.length > 48 && (((text.match(/・/g) || []).length >= 2) || text.includes("※"))) {
    return `invalid_${field}_semantic_bullet_noise`;
  }
  if (text.length > 48 && ((text.match(/[.!?。！？]/g) || []).length >= 2)) {
    return `invalid_${field}_semantic_sentence_noise`;
  }
  if (LOCATION_CSS_NOISE_RE.test(text)) {
    return `invalid_${field}_semantic_noise`;
  }
  if (LOCATION_NOISE_PATTERNS.some(pattern => pattern.test(text))) {
    return `invalid_${field}_semantic_noise`;
  }
  if (LOCATION_ADDRESS_NOISE_RE.test(text)) {
    return `invalid_${field}_semantic_noise`;
  }
  if (text.includes(",") && LOCATION_POSTAL_CODE_RE.test(text)) {
    return `invalid_${field}_semantic_noise`;
  }
  if (text.includes("/")) return `invalid_${field}_semantic_noise`;
  if (text.endsWith(",")) return `invalid_${field}_semantic_noise`;
  if (LOCATION_SCRIPT_NOISE_RE.test(text)) return `invalid_${field}_semantic_noise`;
  if ((text.match(/,/g) || []).length >= 3 && LOCATION_ROLE_BLOB_RE.test(text)) {
    return `invalid_${field}_semantic_noise`;
  }
  if (text.startsWith("#")) return `invalid_${field}_semantic_noise`;
  if (text.includes('"') && text.includes(":")) return `invalid_${field}_semantic_noise`;
  if (text.includes("{") || text.includes("}")) return `invalid_${field}_semantic_noise`;
  if (field === "city" && isCityNoiseFragment(text)) return `invalid_${field}_semantic_noise`;
  return "";
}

export function isSemanticallyValidLocationValue(value, field = "city") {
  return !invalidLocationReason(value, field);
}

export function sanitizeLocationField(value, field = "city") {
  const text = sanitizePublicText(value);
  if (!text) return "";
  if (field === "country") {
    return sanitizeCountryField(text);
  }
  return isSemanticallyValidLocationValue(text, field) ? text : "";
}

function normalizeLocationEntry(entry) {
  if (!entry || typeof entry !== "object") return null;
  const rawCity = sanitizePublicText(entry.city || entry.addressLocality || "");
  const city = sanitizeLocationField(rawCity, "city");
  const country = sanitizeLocationField(entry.country || entry.addressCountry || "", "country");
  const promotedCountry = !country ? resolveCountryAcceptanceValue(rawCity) : "";
  const resolvedCountry = country || promotedCountry;
  if (!city && !resolvedCountry) return null;
  if (!city && isUnknownLocationToken(resolvedCountry)) return null;
  return { city, country: resolvedCountry };
}

function isUnknownLocationToken(value) {
  const lowered = sanitizePublicText(value).trim().toLowerCase();
  return lowered === "unknown";
}

function normalizeJobLocations(value, fallbackCity = "", fallbackCountry = "") {
  const entries = Array.isArray(value) ? value : [];
  const normalized = [];
  const seen = new Set();
  for (const item of entries) {
    const normalizedEntry = normalizeLocationEntry(item);
    if (!normalizedEntry) continue;
    const key = `${normalizedEntry.city}|${normalizedEntry.country}`;
    if (seen.has(key)) continue;
    seen.add(key);
    normalized.push(normalizedEntry);
  }
  if (normalized.length === 0) {
    const rawFallbackCity = sanitizePublicText(fallbackCity || "");
    const city = sanitizeLocationField(rawFallbackCity, "city");
    const country = sanitizeLocationField(fallbackCountry || "", "country");
    const promotedCountry = !country ? resolveCountryAcceptanceValue(rawFallbackCity) : "";
    const resolvedCountry = country || promotedCountry;
    if (city || resolvedCountry) normalized.push({ city, country: resolvedCountry });
  }
  return normalized;
}

export function getJobLocationCities(job) {
  const locations = Array.isArray(job?.locations) ? job.locations : [];
  const cities = [];
  const seen = new Set();
  for (const location of locations) {
    const city = sanitizeLocationField(location?.city || "", "city");
    if (!city || seen.has(city)) continue;
    seen.add(city);
    cities.push(city);
  }
  if (cities.length === 0) {
    const fallbackCity = sanitizeLocationField(job?.city || "", "city");
    if (fallbackCity) cities.push(fallbackCity);
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

function buildJobLocationSummary(job) {
  const locations = Array.isArray(job?.locations) ? job.locations : [];
  const locationLabels = locations
    .map(location => {
      const city = sanitizeLocationField(location?.city || "", "city");
      const country = sanitizeLocationField(location?.country || "", "country");
      return [city, country].filter(Boolean).join(", ");
    })
    .filter(Boolean);
  if (locationLabels.length > 0) return locationLabels.join(" | ");
  const city = sanitizeLocationField(job?.city || "", "city");
  const country = sanitizeLocationField(job?.country || "", "country");
  return [city, country].filter(Boolean).join(", ");
}

const DAY_MS = 24 * 60 * 60 * 1000;

function parseTimestampMs(value) {
  if (!value) return null;
  const ms = Date.parse(String(value));
  return Number.isFinite(ms) ? ms : null;
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

/**
 * @param {import('../shared/types.js').CanonicalJob} row
 * @param {Object} [options]
 * @returns {Object}
 */
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

/**
 * @param {Array<import('../shared/types.js').CanonicalJob>} rows
 * @param {Object} [options]
 * @returns {Array<import('../shared/types.js').CanonicalJob>}
 */
export function normalizeJobs(rows, options = {}) {
  if (!Array.isArray(rows)) return [];
  const professionLabels = options.professionLabels || {};
  const sanitizeUrl = options.sanitizeUrl || (value => String(value || ""));
  return rows.map((row, idx) => {
    const job = { ...row };
    job.id = job.id || (1000 + idx);
    job.title = sanitizePublicText(job.title || "");
    job.company = sanitizePublicText(job.company || "");
    const rawCountry = sanitizePublicText(job.country || "");
    job.locations = normalizeJobLocations(job.locations, job.city || "", rawCountry);
    job.locationSummary = sanitizePublicText(job.locationSummary || buildJobLocationSummary(job));
    const meaningfulLocation =
      job.locations.find(location => location?.city || location?.country) || {};
    job.city = sanitizeLocationField(job.city || meaningfulLocation.city || "", "city");
    const locationCountry = meaningfulLocation.country || rawCountry;
    const sanitizedCountry = sanitizeLocationField(locationCountry, "country");
    job.country = sanitizedCountry || (rawCountry ? "" : "Unknown");
    job.workType = detectWorkType(job.workType || "");
    job.contractType = detectContractType(job.contractType || "", job.title || "");
    job.jobLink = sanitizeUrl(job.jobLink || "");
    job.source = String(job.source || "").trim();
    job.sourceJobId = String(job.sourceJobId || "").trim();
    job.fetchedAt = normalizeTimestamp(job.fetchedAt);
    job.postedAt = normalizeTimestamp(job.postedAt);
    job.firstSeenAt = normalizeTimestamp(job.firstSeenAt);
    job.lastSeenAt = normalizeTimestamp(job.lastSeenAt);
    job.removedAt = normalizeTimestamp(job.removedAt);
    job.status = String(job.status || "active").trim().toLowerCase() || "active";
    const freshness = deriveFreshness(job, options);
    job.freshnessAgeDays = freshness.freshnessAgeDays;
    job.freshnessScore = freshness.freshnessScore;
    job.freshnessSource = freshness.freshnessSource;
    job.dedupKey = String(job.dedupKey || "").trim();
    const quality = Number(job.qualityScore);
    job.qualityScore = Number.isFinite(quality) ? Math.max(0, Math.min(100, Math.round(quality))) : 0;
    job.sector = normalizeSector(
      sanitizePublicText(job.sector || ""),
      job.company || "",
      job.title || "",
      job.source || "",
      job.jobLink || "",
      job.sourceBundle || []
    );
    job.profession = professionLabels[job.profession] ? job.profession : mapProfession(String(job.title || ""));
    if (!job.companyType) {
      job.companyType = classifyCompanyType(
        job.company,
        job.title || "",
        job.source || "",
        job.jobLink || "",
        job.sourceBundle || []
      );
    }
    if (!job.description) job.description = `${job.title} at ${job.company}`;
    return job;
  });
}

function simpleHash(input) {
  let hash = 0;
  const value = String(input || "");
  for (let i = 0; i < value.length; i++) {
    hash = ((hash << 5) - hash) + value.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash).toString(16);
}

/**
 * @param {import('../shared/types.js').CanonicalJob} job
 * @param {Object} [options]
 * @returns {string}
 */
export function getJobKeyForJob(job, options = {}) {
  const generated = typeof options.generateJobKey === "function" ? options.generateJobKey(job) : "";
  if (generated) return generated;
  const canonical = `${job?.title || ""}|${job?.company || ""}|${job?.city || ""}|${job?.country || ""}`.toLowerCase();
  return `job_${simpleHash(canonical)}`;
}

/**
 * @param {import('../shared/types.js').CanonicalJob} job
 * @param {Object} [options]
 * @returns {import('../shared/types.js').SavedJobSnapshot}
 */
export function toJobSnapshot(job, options = {}) {
  const sanitizeUrl = options.sanitizeUrl || (value => String(value || ""));
  const companyType = classifyCompanyType(
    job?.company,
    job?.title,
    job?.source,
    job?.jobLink,
    job?.sourceBundle || []
  );
  const locations = normalizeJobLocations(job?.locations, job?.city || "", job?.country || "");
  const city = sanitizeLocationField(job?.city || "", "city");
  const country = sanitizeLocationField(job?.country || "", "country");
  const locationSummary = buildJobLocationSummary({ ...job, city, country, locations });
  return {
    title: job?.title || "",
    company: job?.company || "",
    sector: job?.sector || companyType,
    companyType: job?.companyType || companyType,
    city,
    country,
    locations,
    locationSummary,
    workType: job?.workType || "Onsite",
    contractType: job?.contractType || "Unknown",
    jobLink: sanitizeUrl(job?.jobLink || "")
  };
}
