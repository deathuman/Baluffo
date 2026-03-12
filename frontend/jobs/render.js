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
  let label = "";
  let cssClass = "";
  if (status === "likely_removed") {
    label = "Likely removed";
    cssClass = "likely-removed";
  } else if (status === "archived") {
    label = "Archived";
    cssClass = "archived";
  } else {
    return "";
  }
  const removedDate = formatDateForStatus(job?.removedAt);
  const title = removedDate ? `${label} since ${removedDate}` : label;
  return `<span class="job-lifecycle-badge ${cssClass}" title="${escapeHtml(title)}">${escapeHtml(label)}</span>`;
}

export function renderJobRowHtml(job, options = {}) {
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
  const safeCity = escapeHtml(job.city || "");
  const safeCountry = escapeHtml(fullCountryName(job.country));
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
  return `<div class="${rowClasses}" data-job-link="${safeJobLink}" data-job-key="${escapeHtml(jobKey)}">${content}</div>`;
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

function sourceUrlFromRegistry(row) {
  if (!row || typeof row !== "object") return "";
  return String(
    row.api_url
    || row.feed_url
    || row.board_url
    || row.listing_url
    || (Array.isArray(row.pages) && row.pages.length ? row.pages[0] : "")
    || ""
  ).trim();
}

function sanitizeSourceUrl(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  try {
    const parsed = new URL(text);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return "";
    return parsed.href;
  } catch {
    return "";
  }
}

function isStaticSourceReportName(name) {
  return String(name || "").trim().toLowerCase().startsWith("static_source::");
}

function compactStaticSourceLabel(rawName) {
  const text = String(rawName || "").trim();
  const marker = "listing_url:";
  const idx = text.toLowerCase().indexOf(marker);
  if (idx >= 0) {
    const rawUrl = text.slice(idx + marker.length).trim();
    const safeUrl = sanitizeSourceUrl(rawUrl);
    if (safeUrl) {
      try {
        const parsed = new URL(safeUrl);
        const host = String(parsed.hostname || "").trim();
        if (host) return `Static source (${host})`;
      } catch {
        // Keep generic fallback below.
      }
    }
  }
  return "Static source";
}

function resolveSheetsForMetadata(sheetsFallbackSource, sheetsFallbackSources) {
  const list = Array.isArray(sheetsFallbackSources) ? sheetsFallbackSources : [];
  if (list.length > 0) {
    return list.filter(row => row && typeof row.sheetId === "string" && row.sheetId.trim());
  }
  if (sheetsFallbackSource && typeof sheetsFallbackSource.sheetId === "string") {
    return [sheetsFallbackSource];
  }
  return [];
}

export function normalizeSourceRows(activeRegistry, fetchReport, sheetsFallbackSource, sheetsFallbackSources) {
  const MAX_STATIC_ACTIVE_ROWS = 8;
  const rows = [];
  const seen = new Set();
  const push = (name, url, status, note = "") => {
    const key = `${String(name || "").toLowerCase()}|${String(url || "").toLowerCase()}`;
    if (!name || seen.has(key)) return;
    seen.add(key);
    rows.push({ name, url, status, note });
  };

  const sheets = resolveSheetsForMetadata(sheetsFallbackSource, sheetsFallbackSources);
  sheets.forEach((sheet, index) => {
    const gid = String(sheet.gid ?? "0");
    const name = index === 0 ? "Google Sheets" : `Google Sheets ${index + 1}`;
    push(name, `https://docs.google.com/spreadsheets/d/${sheet.sheetId}/edit?gid=${gid}`, "core");
  });
  push("Remote OK", "https://remoteok.com/", "core");
  push("GamesIndustry Jobs", "https://jobs.gamesindustry.biz/jobs", "core");

  const reportSources = Array.isArray(fetchReport?.sources) ? fetchReport.sources : [];
  const reportByName = new Map();
  reportSources.forEach(item => {
    reportByName.set(String(item?.name || ""), item);
  });

  const activeRows = (Array.isArray(activeRegistry) ? activeRegistry : [])
    .filter(row => row && typeof row === "object")
    .map(row => ({ ...row, _safeUrl: sanitizeSourceUrl(sourceUrlFromRegistry(row)) }));

  const activeStaticRows = activeRows.filter(row => String(row.adapter || "").trim().toLowerCase() === "static");
  const activeNonStaticRows = activeRows.filter(row => String(row.adapter || "").trim().toLowerCase() !== "static");

  activeNonStaticRows.forEach(row => {
      const name = String(row.name || row.studio || row.adapter || "Source").trim();
      const url = row._safeUrl || "";
      push(name, url, "active");
  });

  const staticRowsSorted = activeStaticRows
    .slice()
    .sort((a, b) => String(a.name || a.studio || "").localeCompare(String(b.name || b.studio || "")));
  staticRowsSorted
    .slice(0, MAX_STATIC_ACTIVE_ROWS)
    .forEach(row => {
      const name = String(row.name || row.studio || "Static source").trim();
      push(name, row._safeUrl || "", "active");
    });
  if (staticRowsSorted.length > MAX_STATIC_ACTIVE_ROWS) {
    push(
      "Static sources",
      "",
      "active",
      `${(staticRowsSorted.length - MAX_STATIC_ACTIVE_ROWS).toLocaleString()} additional static sources hidden for readability.`
    );
  }

  const excludedRows = reportSources.filter(item => String(item?.status || "").toLowerCase() === "excluded");
  const excludedStaticRows = excludedRows.filter(item => isStaticSourceReportName(item?.name));
  const excludedNonStaticRows = excludedRows.filter(item => !isStaticSourceReportName(item?.name));

  excludedNonStaticRows
    .forEach(item => {
      const name = String(item?.name || "Excluded source");
      push(name, "", "excluded", String(item?.error || "").trim());
    });
  if (excludedStaticRows.length > 0) {
    const labels = new Set(excludedStaticRows.slice(0, 3).map(row => compactStaticSourceLabel(row?.name)));
    const hint = Array.from(labels).join(", ");
    const suffix = hint ? ` (${hint})` : "";
    push(
      "Static sources (excluded)",
      "",
      "excluded",
      `${excludedStaticRows.length.toLocaleString()} static sources excluded${suffix}.`
    );
  }

  rows.sort((a, b) => a.name.localeCompare(b.name));
  return { rows, reportByName };
}

function renderSourceListRows(listEl, rows, reportByName) {
  if (!listEl) return;
  if (!rows.length) {
    listEl.innerHTML = "<li>No source metadata available.</li>";
    return;
  }

  listEl.innerHTML = rows.map(item => {
    const reportKeyCandidates = [
      item.name,
      String(item.name || "").toLowerCase().replace(/\s+/g, "_")
    ];
    let report = null;
    for (const key of reportKeyCandidates) {
      if (reportByName.has(key)) {
        report = reportByName.get(key);
        break;
      }
    }

    const fetched = Number(report?.fetchedCount || 0);
    const kept = Number(report?.keptCount || 0);
    const status = String(item.status || "active");
    const suffix = report
      ? ` - fetched ${fetched.toLocaleString()}, kept ${kept.toLocaleString()}`
      : item.note
        ? ` - ${escapeHtml(item.note)}`
        : "";

    if (item.url) {
      return `<li><a href="${escapeHtml(item.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.name)}</a> (${escapeHtml(status)})${suffix}</li>`;
    }
    return `<li>${escapeHtml(item.name)} (${escapeHtml(status)})${suffix}</li>`;
  }).join("");
}

// Source-metadata panel lives in render layer because it is pure view composition around already-fetched data.
export async function renderDataSourcesPanel(options) {
  const {
    dataSourcesListEl,
    dataSourcesCaptionEl,
    sourceRegistryActiveUrls,
    jobsFetchReportUrls,
    sheetsFallbackSource,
    sheetsFallbackSources,
    fetchJsonFromCandidates
  } = options;

  if (!dataSourcesListEl) return;

  const [activeRegistry, fetchReport] = await Promise.all([
    fetchJsonFromCandidates(sourceRegistryActiveUrls),
    fetchJsonFromCandidates(jobsFetchReportUrls)
  ]);

  const normalized = normalizeSourceRows(activeRegistry, fetchReport, sheetsFallbackSource, sheetsFallbackSources);
  renderSourceListRows(dataSourcesListEl, normalized.rows, normalized.reportByName);

  if (dataSourcesCaptionEl) {
    const finishedAt = String(fetchReport?.finishedAt || "").trim();
    if (!finishedAt) {
      dataSourcesCaptionEl.textContent = "Source list reflects your current local fetch configuration.";
    } else {
      const dt = new Date(finishedAt);
      const stamp = Number.isNaN(dt.getTime())
        ? finishedAt
        : dt.toLocaleString([], { year: "numeric", month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
      dataSourcesCaptionEl.textContent = `Source list reflects your current local fetch configuration and latest fetch report (${stamp}).`;
    }
  }
}
