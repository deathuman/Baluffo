import { escapeHtml, showToast, setText } from "../shared/ui/index.js";

export function jobsEscapeHtml(value) {
  return escapeHtml(value);
}

export function setJobsStatus(el, text) {
  setText(el, text);
}

export function showJobsToast(message, type = "info", options = {}) {
  showToast(message, type, options);
}

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
  return parsed.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric"
  });
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
      <span class="job-freshness-ping ${tier}" title="${escapeHtml(tooltip)}"></span>
    </div>
  `;
}

function formatDateForStatus(value) {
  const parsed = new Date(String(value || ""));
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
}

function renderLifecycleBadge(job) {
  const status = String(job?.status || "active").trim().toLowerCase() || "active";
  let label = "Active";
  let cssClass = "active";
  if (status === "likely_removed") {
    label = "Likely removed";
    cssClass = "likely-removed";
  } else if (status === "archived") {
    label = "Archived";
    cssClass = "archived";
  }
  const removedDate = status !== "active" ? formatDateForStatus(job?.removedAt) : "";
  const title = removedDate ? `${label} since ${removedDate}` : label;
  return `<span class="job-lifecycle-badge ${cssClass}" title="${escapeHtml(title)}">${escapeHtml(label)}</span>`;
}

export function renderJobRowHtml(job, options = {}) {
  const {
    fullCountryName,
    sanitizeUrl,
    getJobKeyForJob,
    savedJobKeys,
    isJobsApiReady,
    toContractClass,
    capitalizeFirst
  } = options;
  const safeTitle = escapeHtml(job.title);
  const safeCompany = escapeHtml(job.company);
  const safeSector = escapeHtml(job.sector || "Unknown");
  const safeCity = escapeHtml(job.city || "");
  const safeCountry = escapeHtml(fullCountryName(job.country));
  const safeJobLink = sanitizeUrl(job.jobLink);
  const jobKey = getJobKeyForJob(job);
  const isSaved = savedJobKeys.has(jobKey);
  const content = `
    <button
      class="save-job-btn job-inline-save-btn ${isSaved ? "saved" : ""}"
      data-job-id="${job.id}"
      data-job-key="${jobKey}"
      ${!isJobsApiReady() ? "disabled" : ""}
      aria-label="${isSaved ? "Remove saved job" : "Save job"}"
    >
      ${isSaved ? "x" : "+"}
    </button>
    ${renderFreshnessCell(job)}
    <div class="col-title job-cell" data-label="Position">
      <div class="job-title-wrap">
        <div class="job-title-compact">${safeTitle}</div>
        ${renderLifecycleBadge(job)}
      </div>
    </div>
    <div class="col-company job-cell" data-label="Company">
      <span class="job-company-compact" title="${safeCompany}">${safeCompany}</span>
    </div>
    <div class="col-sector job-cell" data-label="Sector">
      <span class="job-sector">${safeSector}</span>
    </div>
    <div class="col-city job-cell" data-label="City">
      <span class="job-location">${safeCity}</span>
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
  return `<div class="job-row ${safeJobLink ? "job-row-link" : ""}" data-job-link="${safeJobLink}">${content}</div>`;
}

export function showJobsLoading(jobsListEl, text) {
  if (!jobsListEl) return;
  jobsListEl.innerHTML = `<div class="loading">${escapeHtml(text)}</div>`;
}

export function showJobsError(jobsListEl, paginationEl, message, onRetry) {
  if (!jobsListEl) return;
  jobsListEl.innerHTML = `
    <div class="error">
      <p>${escapeHtml(message)}</p>
      <button id="retry-fetch-btn" class="btn retry-btn">Retry</button>
    </div>
  `;
  if (paginationEl) paginationEl.innerHTML = "";
  const retryBtn = document.getElementById("retry-fetch-btn");
  if (retryBtn && typeof onRetry === "function") {
    retryBtn.addEventListener("click", onRetry);
  }
}
