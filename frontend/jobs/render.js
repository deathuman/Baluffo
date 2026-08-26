import { escapeHtml } from "../shared/ui/index.js";
import { renderJobRow as renderJobRowFromComponent } from "../shared/components/JobRow.js";
import { renderErrorState } from "../shared/components/ErrorState.js";

export function renderJobRowHtml(job, options = {}) {
  return renderJobRowFromComponent(job, options);
}

export function showJobsError(jobsListEl, paginationEl, message, onRetry) {
  if (!jobsListEl) return;
  jobsListEl.innerHTML = renderErrorState(message);
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

function sourceUrlHostMatches(sourceUrl, domain) {
  try {
    const host = new URL(String(sourceUrl || "")).hostname.toLowerCase().replace(/\.$/, "");
    const normalizedDomain = String(domain || "").toLowerCase().replace(/^\./, "");
    return Boolean(
      host
      && normalizedDomain
      && (host === normalizedDomain || host.endsWith(`.${normalizedDomain}`))
    );
  } catch {
    return false;
  }
}

function detectAdapterFromSource(sourceName, sourceUrl) {
  const name = String(sourceName || "").toLowerCase();

  // Check for common adapter patterns
  if (name.includes("lever") || sourceUrlHostMatches(sourceUrl, "lever.co")) return "Lever";
  if (name.includes("greenhouse") || sourceUrlHostMatches(sourceUrl, "greenhouse.io")) {
    return "Greenhouse";
  }
  if (name.includes("teamtailor") || sourceUrlHostMatches(sourceUrl, "teamtailor.com")) {
    return "Teamtailor";
  }
  if (name.includes("smartrecruiters") || sourceUrlHostMatches(sourceUrl, "smartrecruiters.com")) {
    return "SmartRecruiters";
  }
  if (name.includes("workable") || sourceUrlHostMatches(sourceUrl, "workable.com")) return "Workable";
  if (name.includes("personio") || sourceUrlHostMatches(sourceUrl, "personio.com")) return "Personio";
  if (name.includes("ashby") || sourceUrlHostMatches(sourceUrl, "ashbyhq.com")) return "Ashby";
  if (name.includes("pinpoint") || sourceUrlHostMatches(sourceUrl, "pinpointhq.com")) return "Pinpoint";
  if (name.includes("recruitee") || sourceUrlHostMatches(sourceUrl, "recruitee.com")) return "Recruitee";
  if (name.includes("gamesmap") || sourceUrlHostMatches(sourceUrl, "gamesmap.com")) return "Gamesmap";
  if (name.includes("sheet") || name.includes("google")) return "Sheet";

  return "Manual Website";
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

  // Group non-static sources by adapter type
  const adapterGroups = new Map();

  activeNonStaticRows.forEach(row => {
    const name = String(row.name || row.studio || row.adapter || "Source").trim();
    const url = row._safeUrl || "";
    const adapter = detectAdapterFromSource(name, url);

    if (!adapterGroups.has(adapter)) {
      adapterGroups.set(adapter, {
        adapter: adapter,
        count: 0,
        name: name,
        url: url,
        status: "active",
        note: ""
      });
    }

    const group = adapterGroups.get(adapter);
    group.count += 1;
    group.name = `${group.count} ${adapter} sources`;
  });

  // Add grouped adapter entries
  adapterGroups.forEach(group => {
    push(group.name, group.url, group.status, group.note);
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

  try {
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
  } catch {
    dataSourcesListEl.innerHTML = "<li>Source metadata unavailable.</li>";
    if (dataSourcesCaptionEl) {
      dataSourcesCaptionEl.textContent = "Source metadata unavailable.";
    }
  }
}
