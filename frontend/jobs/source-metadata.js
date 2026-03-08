import { escapeHtml } from "../shared/ui/index.js";

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

export function normalizeSourceRows(activeRegistry, fetchReport, sheetsFallbackSource) {
  const rows = [];
  const seen = new Set();
  const push = (name, url, status, note = "") => {
    const key = `${String(name || "").toLowerCase()}|${String(url || "").toLowerCase()}`;
    if (!name || seen.has(key)) return;
    seen.add(key);
    rows.push({ name, url, status, note });
  };

  push("Google Sheets", `https://docs.google.com/spreadsheets/d/${sheetsFallbackSource.sheetId}/edit?gid=${sheetsFallbackSource.gid}`, "core");
  push("Remote OK", "https://remoteok.com/", "core");
  push("GamesIndustry Jobs", "https://jobs.gamesindustry.biz/jobs", "core");

  const reportSources = Array.isArray(fetchReport?.sources) ? fetchReport.sources : [];
  const reportByName = new Map();
  reportSources.forEach(item => {
    reportByName.set(String(item?.name || ""), item);
  });

  const activeRows = Array.isArray(activeRegistry) ? activeRegistry : [];
  activeRows
    .filter(row => row && typeof row === "object" && Boolean(row.enabledByDefault))
    .forEach(row => {
      const name = String(row.name || row.studio || row.adapter || "Source").trim();
      const url = sourceUrlFromRegistry(row);
      push(name, url, "active");
    });

  reportSources
    .filter(item => String(item?.status || "").toLowerCase() === "excluded")
    .forEach(item => {
      const name = String(item?.name || "Excluded source");
      push(name, "", "excluded", String(item?.error || "").trim());
    });

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

export async function renderDataSourcesPanel(options) {
  const {
    dataSourcesListEl,
    dataSourcesCaptionEl,
    sourceRegistryActiveUrls,
    jobsFetchReportUrls,
    sheetsFallbackSource,
    fetchJsonFromCandidates
  } = options;

  if (!dataSourcesListEl) return;

  const [activeRegistry, fetchReport] = await Promise.all([
    fetchJsonFromCandidates(sourceRegistryActiveUrls),
    fetchJsonFromCandidates(jobsFetchReportUrls)
  ]);

  const normalized = normalizeSourceRows(activeRegistry, fetchReport, sheetsFallbackSource);
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
