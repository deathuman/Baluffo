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

export function classifyCompanyType(company, title = "") {
  const text = `${company} ${title}`.toLowerCase();
  const isGame =
    /\b(game|gaming|games|esports|studio|studios|interactive|publisher|entertainment)\b/.test(text) ||
    /\b(gameplay|level design|character artist|environment artist|technical artist|animator)\b/.test(text);
  return isGame ? "Game" : "Tech";
}

export function normalizeSector(text, company = "", title = "") {
  const value = String(text || "").trim();
  const lower = value.toLowerCase();
  if (/\b(game|gaming|esports|studio|publisher)\b/.test(lower)) return "Game";
  if (/\b(tech|technology|software|it)\b/.test(lower)) return "Tech";
  return classifyCompanyType(company, title) === "Game" ? "Game" : "Tech";
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

export function isValidCountry(country) {
  if (!country || typeof country !== "string") return false;
  const trimmed = country.trim();
  if (!trimmed || trimmed.length < 2) return false;
  if (trimmed.includes(",")) return false;
  return true;
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

export function invalidLocationReason(value, field = "city") {
  const text = sanitizePublicText(value);
  if (!text) return "";
  const lowered = text.toLowerCase();
  if (["unknown", "n/a", "na", "none", "remote", "hybrid", "onsite", "on-site", "worldwide"].includes(lowered)) {
    return "";
  }
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
  if (/(requirements?|responsibilit(?:y|ies)|qualifications?|experience|register|registration|apply|position|positions|business level|job description|preferred|benefits?|contact us)/i.test(text)) {
    return `invalid_${field}_semantic_noise`;
  }
  if (/(キャリア登録|ポジション|ご案内|応募|職務経歴|ビジネスレベルの日本語能力)/.test(text)) {
    return `invalid_${field}_semantic_noise`;
  }
  return "";
}

export function isSemanticallyValidLocationValue(value, field = "city") {
  return !invalidLocationReason(value, field);
}

export function sanitizeLocationField(value, field = "city") {
  const text = sanitizePublicText(value);
  if (!text) return "";
  return isSemanticallyValidLocationValue(text, field) ? text : "";
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
    job.city = sanitizeLocationField(job.city || "", "city");
    const rawCountry = sanitizePublicText(job.country || "");
    const sanitizedCountry = sanitizeLocationField(rawCountry, "country");
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
    job.sector = normalizeSector(sanitizePublicText(job.sector || ""), job.company || "", job.title || "");
    job.profession = professionLabels[job.profession] ? job.profession : mapProfession(String(job.title || ""));
    if (!job.companyType) job.companyType = classifyCompanyType(job.company, job.title || "");
    if (!job.description) job.description = `${job.title} at ${job.company}`;
    return job;
  });
}

export function simpleHash(input) {
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
  const companyType = classifyCompanyType(job?.company, job?.title);
  return {
    title: job?.title || "",
    company: job?.company || "",
    sector: job?.sector || companyType,
    companyType: job?.companyType || companyType,
    city: job?.city || "",
    country: job?.country || "",
    workType: job?.workType || "Onsite",
    contractType: job?.contractType || "Unknown",
    jobLink: sanitizeUrl(job?.jobLink || "")
  };
}
