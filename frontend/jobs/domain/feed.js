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

// Curated games-industry employer identities, matched against the row's
// COMPANY field only. These names carry employer-scope evidence the
// keyword/provenance branches cannot (no game token in the name, no dedicated
// board source) and short-name employers use multi-token hints so unrelated
// look-alikes ("EACH1", "Eataly") stay unmatched. Recovers the
// URL-keyword-carried employers lost to the 2026-09-06 keyword scoping:
// docs/snapshots/sector-signal-contamination-2026-09-06.md.
const GAME_EMPLOYER_NAME_HINTS = [
  "playrix",
  "daybreak",
  "metacore",
  "avalanche",
  "square enix",
  "lightbulb crew",
  "electronic arts",
  "ea sports",
  "ea create"
];

function normalizeBundleList(sourceBundle) {
  return Array.isArray(sourceBundle) ? sourceBundle.filter(item => item && typeof item === "object") : [];
}

const STATIC_ADAPTERS = ["csv", "static", "scrapy_static"];

// Tokens too generic to tie two employer names together.
const GENERIC_EMPLOYER_TOKENS = new Set([
  "games", "game", "gaming", "entertainment", "interactive", "studios", "studio",
  "inc", "llc", "ltd", "the", "and", "co", "corp", "company", "group", "holdings"
]);

function employerTokens(value) {
  return String(value || "")
    .toLowerCase()
    .match(/[a-z0-9]+/g)
    ?.filter(token => !GENERIC_EMPLOYER_TOKENS.has(token)) || [];
}

function studioConsistentWithCompany(studio, company) {
  const studioNorm = String(studio || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const companyNorm = String(company || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  if (!studioNorm || !companyNorm) return false;
  if (studioNorm.includes(companyNorm) || companyNorm.includes(studioNorm)) return true;
  const studioSet = new Set(employerTokens(studio));
  return employerTokens(company).some(token => studioSet.has(token));
}

// Provider provenance is scoped for multi-board static rows: when the bundle
// mixes provider-adapter items with static-adapter items, provider provenance
// only counts if every provider item's studio is employer-consistent with the
// row's company — one employer's board must not certify a row belonging to a
// different employer on the same aggregated site.
function hasGameSourceProvenance(source = "", sourceBundle = [], company = "") {
  const sourceText = String(source || "").toLowerCase();
  if (sourceText && GAME_SOURCE_FAMILY_HINTS.some(hint => sourceText.includes(hint))) {
    return true;
  }

  let sawProviderItem = false;
  let sawStaticItem = false;
  let providerInconsistent = false;
  for (const item of normalizeBundleList(sourceBundle)) {
    const bundleSource = String(item.source || "").toLowerCase();
    if (bundleSource && GAME_SOURCE_FAMILY_HINTS.some(hint => bundleSource.includes(hint))) {
      return true;
    }
    const adapter = String(item.adapter || "").trim().toLowerCase();
    if (!adapter) continue;
    if (STATIC_ADAPTERS.includes(adapter)) {
      sawStaticItem = true;
      continue;
    }
    const studio = String(item.studio || "").trim().toLowerCase();
    if (!studio) continue;
    sawProviderItem = true;
    if (!studioConsistentWithCompany(studio, company)) {
      providerInconsistent = true;
    }
  }
  if (!sawProviderItem) return false;
  if (!sawStaticItem) return true;
  return !providerInconsistent;
}

// Company name tokens are deliberately NOT positive game evidence: ATS hosts
// (greenhouse/lever/workable/ashby/bamboohr) and own-domain careers sites embed
// the company slug for every employer, so "company appears in its own job URL"
// carries no employer-specific information (it misclassified Apple/NVIDIA-class
// non-games rows as Game). GAME_KEYWORDS equally do not count in source/jobLink:
// board URLs like careers.wbd.com/.../wb-games-jobs or
// disneycareers.com/en/search-jobs/game/... carry a games-flavored path for a
// whole-company site and misattributed every employer's rows (WBD, CNN, HBO
// Max, Disney, SciGames) as Game. Evidence:
// docs/snapshots/sector-signal-contamination-2026-09-06.md.
// `jobLink` is accepted for positional signature stability but carries no
// evidence (see the comment above: board URLs are not employer-scoped), hence
// the intentionally unused leading-underscore parameter.
function hasPositiveGameEvidence(company = "", title = "", source = "", _jobLink = "", sourceBundle = []) {
  if (hasGameSourceProvenance(source, sourceBundle, company)) {
    return true;
  }
  const companyText = String(company || "").trim().toLowerCase();
  if (companyText && GAME_EMPLOYER_NAME_HINTS.some(hint => companyText.includes(hint))) {
    return true;
  }
  const employerText = `${company} ${title}`.toLowerCase();
  if (
    /\b(game|gaming|games|esports|gameplay|gamedev|unity|unreal|technical artist|tech artist|shader|material artist|world artist|terrain artist|environment art|environment artist|character artist|engine programmer|graphics programmer|level design|animator)\b/.test(employerText) ||
    /\b(studio|studios|interactive|publisher|entertainment)\b/.test(employerText) ||
    employerText.includes("game")
  ) {
    return true;
  }
  return false;
}

// Delegates entirely to hasPositiveGameEvidence: the two keyword regexes this
// function used to carry were a strict subset of that predicate's own
// company/title matching, so they never changed a verdict. Kept as a named
// export because it is the public company/sector entry point.
export function classifyCompanyType(company, title = "", source = "", jobLink = "", sourceBundle = []) {
  return hasPositiveGameEvidence(company, title, source, jobLink, sourceBundle) ? "Game" : "Tech";
}

// The incoming `sector` value carries no evidence: the verdict is decided
// entirely by hasPositiveGameEvidence, which is why the parameter is now
// intentionally unused (leading underscore per eslint argsIgnorePattern). It
// stays in the signature because it is public surface, re-exported through
// frontend/jobs/domain.js and passed positionally by call sites such as
// frontend/jobs/parsing-utils.js.
export function normalizeSector(_text, company = "", title = "", source = "", jobLink = "", sourceBundle = []) {
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
    job.availabilityId = String(job.availabilityId || "").trim();
    job.availabilityStatus = String(job.availabilityStatus || "available").trim().toLowerCase() || "available";
    job.availabilityCheckedAt = normalizeTimestamp(job.availabilityCheckedAt);
    job.availabilityVerifiedAt = normalizeTimestamp(job.availabilityVerifiedAt);
    job.availabilityUnavailableAt = normalizeTimestamp(job.availabilityUnavailableAt);
    job.availabilityEvidence = job.availabilityEvidence && typeof job.availabilityEvidence === "object"
      ? { ...job.availabilityEvidence }
      : {};
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
