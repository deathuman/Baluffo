/**
 * Renders a single job row HTML string. Used by jobs page.
 * @param {object} job - Job with title, company, jobLink, freshnessScore, etc.
 * @param {{ fullCountryName: function, sanitizeUrl: function, getJobKeyForJob: function, savedJobKeys: Set, isSeen?: boolean, isNew?: boolean, isJobsApiReady: function, toContractClass: function, capitalizeFirst: function }} options
 * @returns {string} HTML string for the row
 */
import { escapeHtml, tooltipAttrs } from "../ui/index.js";
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
    return '<div class="col-freshness" aria-hidden="true"></div>';
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

export function renderJobRow(job, options = {}) {
  const {
    fullCountryName,
    sanitizeUrl,
    getJobKeyForJob,
    savedJobKeys,
    isSeen = false,
    isNew = false,
    isJobsApiReady,
    toContractClass,
    capitalizeFirst
  } = options;
  const safeTitle = escapeHtml(job.title);
  const safeCompany = escapeHtml(job.company);
  const safeSector = escapeHtml(job.sector || "Unknown");
  const locationColumns = formatJobLocationColumns(job, { fullCountryName });
  const safeCity = escapeHtml(locationColumns.cityLabel);
  const safeCountry = escapeHtml(locationColumns.countryLabel);
  const safeJobLink = sanitizeUrl(job.jobLink);
  const jobKey = getJobKeyForJob(job);
  const isSaved = savedJobKeys.has(jobKey);
  const rowClasses = [
    "job-row",
    safeJobLink ? "job-row-link" : "",
    isSeen ? "job-row-seen" : "",
    isNew ? "job-row-new" : ""
  ].filter(Boolean).join(" ");
  const newBadge = isNew ? '<span class="job-new-badge">New</span>' : "";
  const content = `
    <button
      class="save-job-btn job-inline-save-btn ${isSaved ? "saved" : ""}"
      data-job-id="${job.id}"
      data-job-key="${jobKey}"
      data-ui="save-job-btn"
      ${!isJobsApiReady() ? "disabled" : ""}
      aria-label="${isSaved ? "Remove saved job" : "Save job"}"
    >
      ${isSaved ? "x" : "+"}
    </button>
    ${renderFreshnessCell(job)}
    <div class="col-title job-cell" data-label="Position">
      <div class="job-title-wrap">
        <div class="job-title-compact">${safeTitle}</div>
        ${newBadge}
        ${renderLifecycleBadgeHtml(job)}
      </div>
    </div>
    <div class="col-company job-cell" data-label="Company">
      <span class="job-company-compact"${tooltipAttrs(job.company)}>${safeCompany}</span>
    </div>
    <div class="col-sector job-cell" data-label="Sector">
      <span class="job-sector">${safeSector}</span>
    </div>
    <div class="col-city job-cell" data-label="City">
      <span class="job-location"${tooltipAttrs(locationColumns.cityLabel)}>${safeCity}</span>
    </div>
    <div class="col-country job-cell" data-label="Country">
      <span class="job-location">${safeCountry}</span>
    </div>
    <div class="col-contract job-cell" data-label="Contract">
      <span class="job-contract ${toContractClass(job.contractType)}">${escapeHtml(job.contractType || "Unknown")}</span>
    </div>
    <div class="col-type job-cell" data-label="Type">
      <span class="job-tag ${job.workType.toLowerCase()}">${capitalizeFirst(job.workType)}</span>
    </div>
  `;
  return `<div class="${rowClasses}" data-job-link="${safeJobLink}" data-job-key="${escapeHtml(jobKey)}" data-ui="job-row">${content}</div>`;
}
