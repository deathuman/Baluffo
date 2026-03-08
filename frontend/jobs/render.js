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
    <div class="col-title job-cell" data-label="Position">
      <div class="job-title-wrap">
        <div class="job-title-compact">${safeTitle}</div>
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
