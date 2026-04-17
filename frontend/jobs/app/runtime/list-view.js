import { getVisiblePages } from "../pagination.js";
import { sanitizeUrl, toContractClass, capitalizeFirst } from "../runtime-utils.js";

export function updateResultsSummary(resultsSummary, total, from, to, loadedTotal = total) {
  if (!resultsSummary) return;
  const loaded = Number.isFinite(Number(loadedTotal)) ? Number(loadedTotal) : total;
  if (total === 0) {
    if (loaded > 0) {
      resultsSummary.textContent = `Showing 0 jobs (${loaded.toLocaleString()} loaded)`;
      return;
    }
    resultsSummary.textContent = "0 jobs";
    return;
  }
  const filteredText = `Showing ${from}-${to} of ${total.toLocaleString()} jobs`;
  resultsSummary.textContent = loaded > total
    ? `${filteredText} (${loaded.toLocaleString()} loaded)`
    : filteredText;
}

function renderJobRow(job, {
  currentUser,
  seenJobKeys,
  savedJobKeys,
  isJobsApiReady,
  getJobKeyForJob,
  fullCountryName,
  renderJobRowHtml
}) {
  const jobKey = getJobKeyForJob(job);
  const isSeen = Boolean(currentUser && seenJobKeys.has(jobKey));
  return renderJobRowHtml(job, {
    fullCountryName,
    sanitizeUrl,
    getJobKeyForJob,
    savedJobKeys,
    isSeen,
    isNew: Boolean(currentUser && !isSeen),
    isJobsApiReady,
    toContractClass,
    capitalizeFirst
  });
}

function renderPagination(totalPages, {
  pagination,
  state,
  goToPage
}) {
  let html = "";

  if (totalPages > 1) {
    if (state.currentPage > 1) {
      html += `<button class="page-btn" data-page="${state.currentPage - 1}" aria-label="Previous page">Prev</button>`;
    }

    const visiblePages = getVisiblePages(totalPages, state.currentPage);
    visiblePages.forEach(item => {
      if (item === "...") {
        html += '<span class="page-ellipsis">...</span>';
      } else {
        html += `<button class="page-btn ${item === state.currentPage ? "active" : ""}" data-page="${item}">${item}</button>`;
      }
    });

    if (state.currentPage < totalPages) {
      html += `<button class="page-btn" data-page="${state.currentPage + 1}" aria-label="Next page">Next</button>`;
    }
  }

  pagination.innerHTML = html;

  pagination.querySelectorAll(".page-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const page = parseInt(btn.dataset.page, 10);
      if (!Number.isNaN(page)) {
        goToPage(page);
      }
    });
  });
}

export function goToPage(page, {
  filteredJobs,
  state,
  displayJobs,
  writeStateToUrl
}) {
  const totalPages = Math.max(1, Math.ceil(filteredJobs.length / state.itemsPerPage));
  const nextPage = Math.min(Math.max(page, 1), totalPages);
  if (nextPage === state.currentPage) return;

  state.currentPage = nextPage;
  displayJobs(filteredJobs);
  writeStateToUrl();
}

export function displayJobs(jobs, {
  jobsList,
  pagination,
  resultsSummary,
  state,
  allJobs,
  currentUser,
  seenJobKeys,
  savedJobKeys,
  isJobsApiReady,
  getJobKeyForJob,
  fullCountryName,
  goToPage,
  emitDesktopStartupMetric,
  renderJobRowHtml
}, {
  pageJobsOverride = null,
  totalCountOverride = null
} = {}) {
  if (!jobsList || !pagination) return;
  const hasTotalCountOverride = totalCountOverride !== null && totalCountOverride !== undefined;
  const totalCount = hasTotalCountOverride ? Number(totalCountOverride) : jobs.length;
  emitDesktopStartupMetric("jobs_display_start", {
    totalCount,
    currentPage: state.currentPage
  });

  if (totalCount === 0) {
    jobsList.innerHTML = '<div class="no-results">No jobs found matching your filters.</div>';
    pagination.innerHTML = "";
    updateResultsSummary(resultsSummary, 0, 0, 0, allJobs.length);
    emitDesktopStartupMetric("jobs_display_empty");
    return;
  }

  const totalPages = Math.ceil(totalCount / state.itemsPerPage);
  if (state.currentPage > totalPages) state.currentPage = totalPages;

  const startIndex = (state.currentPage - 1) * state.itemsPerPage;
  const pageJobs = Array.isArray(pageJobsOverride)
    ? pageJobsOverride
    : jobs.slice(startIndex, startIndex + state.itemsPerPage);
  emitDesktopStartupMetric("jobs_display_markup_start", {
    pageJobs: pageJobs.length,
    totalPages
  });

  jobsList.innerHTML = `
    <div class="jobs-table-header">
      <div class="job-row-header">
        <div class="col-freshness" title="Freshness (posted/fetched recency)" aria-hidden="true"></div>
        <div class="col-title">Position</div>
        <div class="col-company">Company</div>
        <div class="col-sector">Sector</div>
        <div class="col-city">City</div>
        <div class="col-country">Country</div>
        <div class="col-contract">Contract</div>
        <div class="col-type">Type</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${pageJobs.map(job => renderJobRow(job, {
        currentUser,
        seenJobKeys,
        savedJobKeys,
        isJobsApiReady,
        getJobKeyForJob,
        fullCountryName,
        renderJobRowHtml
      })).join("")}
    </div>
  `;
  emitDesktopStartupMetric("jobs_display_dom_committed", {
    pageJobs: pageJobs.length
  });

  renderPagination(totalPages, { pagination, state, goToPage });
  emitDesktopStartupMetric("jobs_display_pagination_complete", {
    totalPages
  });
  updateResultsSummary(resultsSummary, totalCount, startIndex + 1, startIndex + pageJobs.length, allJobs.length);
  emitDesktopStartupMetric("jobs_display_complete", {
    startIndex: startIndex + 1,
    endIndex: startIndex + pageJobs.length,
    totalCount
  });
  window.requestAnimationFrame(() => {
    emitDesktopStartupMetric("jobs_display_frame_presented", {
      pageJobs: pageJobs.length
    });
  });
}
