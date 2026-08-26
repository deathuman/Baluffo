import {
  fetchUnifiedJobs as fetchUnifiedJobsFromData,
  fetchJsonFromCandidates as fetchJsonFromCandidatesFromData,
  parseUnifiedJobsPayload,
  parseCSVLarge as parseCSVLargeFromData
} from "../data-source.js";
import { resolveContainerRuntimeMode } from "../../shared/local-data/runtime-context.js";
import { renderDataSourcesPanel } from "../render.js";

const UNIFIED_JSON_SOURCES = [
  { name: "Unified JSON light (local data)", url: "data/jobs-unified-light.json" }
];

export const STARTUP_PREVIEW_JSON_URLS = [
  "data/jobs-unified-startup.json",
  "data/jobs-unified-light.json"
];

const UNIFIED_CSV_SOURCES = [];

const SHEETS_FALLBACK_SOURCES = [
  { sheetId: "1ZOJpVS3CcnrkwhpRgkP7tzf3wc4OWQj-uoWFfv4oHZE", gid: "1560329579" },
  { sheetId: "1eR2oAXOuflr8CZeGoz3JTrsgNj3KuefbdXJOmNtjEVM", gid: "0" },
  { sheetId: "1MvqHXAtXP_6ogtfrLM0g_RzGdJQyx5Q8mhPX4lZECkI", gid: "0" }
];

const SOURCE_REGISTRY_ACTIVE_URLS = [
  "data/source-registry-active.json",
  "source-registry-active.json"
];

const JOBS_FETCH_REPORT_URLS = [
  "data/jobs-fetch-report.json",
  "jobs-fetch-report.json"
];
export { JOBS_FETCH_REPORT_URLS };

export const JOBS_AVAILABILITY_HISTORY_URLS = [
  "data/jobs-availability-history.json",
  "jobs-availability-history.json"
];

function isDesktopRuntimeMode() {
  const win = globalThis.window;
  if (!win) return false;
  if (win.__baluffoDesktopMode) return true;
  try {
    return new URL(win.location?.href || "").searchParams.get("desktop") === "1";
  } catch {
    return false;
  }
}

function isContainerRuntimeMode() {
  try {
    return resolveContainerRuntimeMode();
  } catch {
    return false;
  }
}

export function getStartupPreviewJsonUrlsForRuntime() {
  return isContainerRuntimeMode() ? ["data/jobs-unified-startup.json"] : STARTUP_PREVIEW_JSON_URLS;
}

export function getSourceRegistryActiveUrlsForRuntime() {
  return isDesktopRuntimeMode() || isContainerRuntimeMode() ? [] : SOURCE_REGISTRY_ACTIVE_URLS;
}

export function getJobsFetchReportUrlsForRuntime() {
  return isContainerRuntimeMode() ? [] : JOBS_FETCH_REPORT_URLS;
}

export async function fetchUnifiedJobs({
  setSourceStatus,
  jobsParsing,
  parserDeps,
  timeoutMs,
  allowSheetsFallback = true
}) {
  return fetchUnifiedJobsFromData({
    unifiedJsonSources: UNIFIED_JSON_SOURCES,
    unifiedCsvSources: UNIFIED_CSV_SOURCES,
    sheetsFallbackSources: SHEETS_FALLBACK_SOURCES,
    setSourceStatus,
    timeoutMs,
    allowSheetsFallback,
    parseUnifiedPayload: payload => parseUnifiedJobsPayload(payload, jobsParsing),
    parseCSV: csv => parseCSVLargeFromData(csv, {
      jobsParsing,
      parserDeps
    })
  });
}

export async function fetchJsonFromCandidates(urls, options) {
  return fetchJsonFromCandidatesFromData(urls, options);
}

export async function renderDataSources({
  dataSourcesListEl,
  dataSourcesCaptionEl
}) {
  return renderDataSourcesPanel({
    dataSourcesListEl,
    dataSourcesCaptionEl,
    sourceRegistryActiveUrls: getSourceRegistryActiveUrlsForRuntime(),
    jobsFetchReportUrls: getJobsFetchReportUrlsForRuntime(),
    sheetsFallbackSources: SHEETS_FALLBACK_SOURCES,
    fetchJsonFromCandidates
  });
}
