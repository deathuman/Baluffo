import {
  buildJobLocationSummary,
  detectContractType,
  detectWorkType,
  normalizeJobLocations,
  normalizeTimestamp,
  sanitizeLocationField,
  sanitizePublicText
} from "./query.js";
import { deriveFreshness } from "./view.js";

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

export function isInternshipJob(job) {
  const contract = String(job?.contractType || "").toLowerCase();
  if (contract === "internship") return true;
  const text = `${job?.title || ""} ${job?.description || ""}`.toLowerCase();
  return /\bintern(ship)?\b/.test(text);
}

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
    job.lifecycleEvent = String(job.lifecycleEvent || "").trim().toLowerCase();
    job.lifecycleReason = String(job.lifecycleReason || "").trim().toLowerCase();
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
  }).filter(job => String(job?.title || "").trim());
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

export function getJobKeyForJob(job, options = {}) {
  const generated = typeof options.generateJobKey === "function" ? options.generateJobKey(job) : "";
  if (generated) return generated;
  const canonical = `${job?.title || ""}|${job?.company || ""}|${job?.city || ""}|${job?.country || ""}`.toLowerCase();
  return `job_${simpleHash(canonical)}`;
}

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
