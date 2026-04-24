import {
  COUNTRY_ACCEPTANCE,
  resolveCountryAcceptanceValue
} from "../../shared/data/country-acceptance.js";
import {
  CITY_NOISE_CONTRACT,
  normalizeCityNoiseText
} from "../../shared/data/city-noise.js";

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

export function normalizeTimestamp(value) {
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
  if (text.length > 48 && (((text.match(/\u30fb/g) || []).length >= 2) || text.includes("\u203b"))) {
    return `invalid_${field}_semantic_bullet_noise`;
  }
  if (text.length > 48 && ((text.match(/[.!?\u3002\uff01\uff1f]/g) || []).length >= 2)) {
    return `invalid_${field}_semantic_sentence_noise`;
  }
  if (LOCATION_CSS_NOISE_RE.test(text)) return `invalid_${field}_semantic_noise`;
  if (LOCATION_NOISE_PATTERNS.some(pattern => pattern.test(text))) return `invalid_${field}_semantic_noise`;
  if (LOCATION_ADDRESS_NOISE_RE.test(text)) return `invalid_${field}_semantic_noise`;
  if (text.includes(",") && LOCATION_POSTAL_CODE_RE.test(text)) return `invalid_${field}_semantic_noise`;
  if (text.includes("/")) return `invalid_${field}_semantic_noise`;
  if (text.endsWith(",")) return `invalid_${field}_semantic_noise`;
  if (LOCATION_SCRIPT_NOISE_RE.test(text)) return `invalid_${field}_semantic_noise`;
  if ((text.match(/,/g) || []).length >= 3 && LOCATION_ROLE_BLOB_RE.test(text)) return `invalid_${field}_semantic_noise`;
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
  if (field === "country") return sanitizeCountryField(text);
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

export function normalizeJobLocations(value, fallbackCity = "", fallbackCountry = "") {
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

export function buildJobLocationSummary(job) {
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

export function parseTimestampMs(value) {
  if (!value) return null;
  const ms = Date.parse(String(value));
  return Number.isFinite(ms) ? ms : null;
}
