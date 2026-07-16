/**
 * Renders a single job row HTML string. Used by jobs page.
 * @param {object} job - Job with title, company, jobLink, freshnessScore, etc.
 * @param {{ fullCountryName: function, sanitizeUrl: function, getJobKeyForJob: function, savedJobKeys: Set, isSeen?: boolean, isNew?: boolean, isJobsApiReady: function, toContractClass: function, capitalizeFirst: function }} options
 * @returns {string} HTML string for the row
 */
import { escapeHtml, tooltipAttrs } from "../ui/index.js?v=5";
import { renderLifecycleBadgeHtml } from "../lifecycle-badges.js";
import { formatJobLocationColumns } from "../location-display.js";

function getFreshnessTier(score) {
  if (!Number.isFinite(score)) return "";
  if (score <= 40) return "fresh";
  if (score <= 70) return "mid";
  return "stale";
}

function getFreshnessTooltip(ageDays, source) {
  if (!Number.isFinite(ageDays)) return "";
  if (source === "postedAt") return `Posted ${ageDays}d ago`;
  if (source === "fetchedAt") return `Fetched ${ageDays}d ago (best guess)`;
  return "";
}

function formatTooltipDate(value) {
  const parsed = new Date(String(value || ""));
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
}

function renderFreshnessCell(job) {
  const rawScore = job?.freshnessScore;
  const rawAgeDays = job?.freshnessAgeDays;
  const score = typeof rawScore === "number" ? rawScore : Number.NaN;
  const ageDays = typeof rawAgeDays === "number" ? rawAgeDays : Number.NaN;
  const source = String(job?.freshnessSource || "");
  if (!Number.isFinite(score) || !Number.isFinite(ageDays)) {
    return `
      <div class="col-freshness" aria-hidden="true">
        <span class="job-freshness-ping unknown"${tooltipAttrs("Freshness unknown")}></span>
      </div>
    `;
  }
  const tier = getFreshnessTier(score);
  const baseTooltip = getFreshnessTooltip(ageDays, source);
  const sourceDateRaw = source === "postedAt" ? job?.postedAt : job?.fetchedAt;
  const guessedDate = formatTooltipDate(sourceDateRaw);
  const tooltip = guessedDate ? `${baseTooltip} (${guessedDate})` : baseTooltip;
  return `
    <div class="col-freshness" aria-hidden="true">
      <span class="job-freshness-ping ${tier}"${tooltipAttrs(tooltip)}></span>
    </div>
  `;
}

function formatWorkType(value, capitalizeFirst) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  return capitalizeFirst(raw);
}

function isUnknownValue(value) {
  const text = String(value || "").trim().toLowerCase();
  return !text || text === "unknown" || text === "n/a";
}

function isWorkModeValue(value, workType = "") {
  const text = String(value || "").trim().toLowerCase();
  const mode = String(workType || "").trim().toLowerCase();
  if (!text) return false;
  return text === mode || ["remote", "hybrid", "onsite", "on-site", "on site"].includes(text);
}

function isEmptyLocationValue(value, workType = "") {
  return isUnknownValue(value) || isWorkModeValue(value, workType);
}

function cleanRowLocationValue(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function sameText(left, right) {
  return cleanRowLocationValue(left).toLowerCase() === cleanRowLocationValue(right).toLowerCase();
}

function stripCountrySuffix(city, country) {
  const cleanCity = cleanRowLocationValue(city);
  const cleanCountry = cleanRowLocationValue(country);
  if (!cleanCity || !cleanCountry) return cleanCity;
  const suffix = `, ${cleanCountry}`;
  return cleanCity.toLowerCase().endsWith(suffix.toLowerCase())
    ? cleanCity.slice(0, -suffix.length).trim()
    : cleanCity;
}

function parseCompactLocationLabel(label, countryHint = "") {
  const text = cleanRowLocationValue(label);
  if (!text || text.includes("|")) return { city: "", country: "" };
  const hint = cleanRowLocationValue(countryHint);
  if (hint && text.toLowerCase().endsWith(`, ${hint.toLowerCase()}`)) {
    return {
      city: stripCountrySuffix(text, hint),
      country: hint
    };
  }
  const commaParts = text.split(",").map(part => cleanRowLocationValue(part)).filter(Boolean);
  if (commaParts.length === 2) {
    return {
      city: commaParts[0],
      country: commaParts[1]
    };
  }
  return { city: text, country: "" };
}

function resolveRowLocation(job, locationColumns, { fullCountryName }) {
  const locations = Array.isArray(job?.locations) ? job.locations : [];
  const firstLocation = locations.length === 1 ? locations[0] : null;
  const workType = String(job?.workType || "");
  const rawCountry = cleanRowLocationValue(firstLocation?.country || job?.country || "");
  const rawCity = cleanRowLocationValue(firstLocation?.city || job?.city || "");
  const countryFromRaw = isEmptyLocationValue(rawCountry, workType)
    ? ""
    : cleanRowLocationValue(typeof fullCountryName === "function" ? fullCountryName(rawCountry) : rawCountry);
  let country = countryFromRaw;
  let city = isEmptyLocationValue(rawCity, workType) ? "" : stripCountrySuffix(rawCity, country);

  if (sameText(city, country)) city = "";

  if (!country || !city) {
    const parsed = parseCompactLocationLabel(locationColumns.cityLabel, country || locationColumns.countryLabel);
    if (!country && parsed.country && !isEmptyLocationValue(parsed.country, workType)) country = parsed.country;
    if (!city && parsed.city && !isEmptyLocationValue(parsed.city, workType) && !sameText(parsed.city, country)) {
      city = stripCountrySuffix(parsed.city, country);
    }
  }

  const fallbackCountry = cleanRowLocationValue(locationColumns.countryLabel);
  if (!country && fallbackCountry && !isEmptyLocationValue(fallbackCountry, workType)) country = fallbackCountry;
  return { city, country };
}

function renderJobRowContent(job, {
  safeTitle,
  safeCompany,
  locationColumns,
  safeJobLink,
  jobKey,
  isSaved,
  isJobsApiReady,
  canManageAvailability,
  isSeen,
  isNew,
  toContractClass,
  fullCountryName,
  capitalizeFirst
}) {
  const workTypeLabel = formatWorkType(job.workType, capitalizeFirst);
  const newBadge = isNew ? '<span class="job-new-badge">New</span>' : "";
  const lifecycleBadge = renderLifecycleBadgeHtml(job);
  const isUnavailable = String(job.availabilityStatus || "").toLowerCase() === "unavailable";
  const sectorLine = isUnknownValue(job.sector)
    ? ""
    : `<div class="job-sector-line">${escapeHtml(job.sector)}</div>`;
  const rowLocation = resolveRowLocation(job, locationColumns, { fullCountryName });
  const rowCountry = escapeHtml(rowLocation.country);
  const rowCity = escapeHtml(rowLocation.city);
  const rowClasses = [
    "job-row",
    safeJobLink && !isUnavailable ? "job-row-link" : "",
    isSeen ? "job-row-seen" : "",
    isNew ? "job-row-new" : ""
  ].filter(Boolean).join(" ");
  return `
    <div class="${rowClasses}" data-job-link="${isUnavailable ? "" : safeJobLink}" data-job-key="${escapeHtml(jobKey)}" data-availability-status="${escapeHtml(job.availabilityStatus || "available")}" data-ui="job-row">
      <div class="col-title job-cell" data-label="Position">
        <div class="job-title-line">
          ${renderFreshnessCell(job)}
          <span class="job-title-compact">${safeTitle}</span>
          ${newBadge}
          ${lifecycleBadge}
        </div>
        ${sectorLine}
      </div>
      <div class="col-company job-cell" data-label="Company">
        <span class="job-company-compact">${safeCompany}</span>
      </div>
      <div class="col-location job-cell" data-label="Location">
        <div class="job-location-stack">
          <span class="job-country-main">${rowCountry}</span>
          <span class="job-city-sub">${rowCity}</span>
        </div>
      </div>
      <div class="col-contract job-cell" data-label="Contract">
        <span class="job-contract ${toContractClass(job.contractType)}">${escapeHtml(job.contractType || "Unknown")}</span>
      </div>
      <div class="col-type job-cell" data-label="Type">
        <span class="job-tag ${job.workType.toLowerCase()}">${escapeHtml(workTypeLabel || "Unknown")}</span>
      </div>
      <div class="col-save job-cell" data-label="Save" aria-label="Job actions">
        ${isUnavailable && safeJobLink ? `<button class="btn back-btn job-original-link-btn availability-warning" data-ui="job-original-link-btn" data-job-link="${safeJobLink}" title="Confirmed unavailable; open the original posting anyway" aria-label="Open original link for confirmed unavailable job">Open original link</button>` : ""}
        ${canManageAvailability && job.availabilityId ? `<button class="job-availability-check-btn" data-ui="job-availability-check-btn" data-availability-id="${escapeHtml(job.availabilityId)}" title="Check availability now" aria-label="Check availability now">↻</button>` : ""}
        <button
          class="save-job-btn job-inline-save-btn ${isSaved ? "saved" : ""}"
          data-job-id="${job.id}"
          data-job-key="${jobKey}"
          data-ui="save-job-btn"
          ${!isJobsApiReady() ? "disabled" : ""}
          aria-label="${isSaved ? "Job saved" : "Save job"}"
          title="${isSaved ? "Saved" : "Save job"}"
        >
          <span aria-hidden="true">${isSaved ? "✓" : "＋"}</span>
        </button>
      </div>
    </div>
  `;
}

export function renderJobRow(job, options = {}) {
  const {
    fullCountryName,
    sanitizeUrl,
    getJobKeyForJob,
    savedJobKeys,
    isSeen = false,
    isNew = false,
    isJobsApiReady,
    canManageAvailability = false,
    toContractClass,
    capitalizeFirst
  } = options;
  const safeTitle = escapeHtml(job.title);
  const safeCompany = escapeHtml(job.company);
  const locationColumns = formatJobLocationColumns(job, { fullCountryName });
  const safeJobLink = sanitizeUrl(job.jobLink);
  const jobKey = getJobKeyForJob(job);
  const isSaved = savedJobKeys.has(jobKey);
  return renderJobRowContent(job, {
    safeTitle,
    safeCompany,
    locationColumns,
    safeJobLink,
    jobKey,
    isSaved,
    isJobsApiReady,
    canManageAvailability,
    isSeen,
    isNew,
    toContractClass,
    fullCountryName,
    capitalizeFirst
  });
}
