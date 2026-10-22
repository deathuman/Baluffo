import { timeFrontendAsync } from "../shared/perf-counters.js";

async function fetchWithTimeout(url, timeoutMs, init = {}) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  return timeFrontendAsync("frontend_fetch_jobs_feed", async () => {
    try {
      return await fetch(url, {
        ...init,
        signal: controller.signal,
        mode: "cors",
        credentials: "omit"
      });
    } finally {
      clearTimeout(timeoutId);
    }
  }, { url: String(url || "").slice(0, 180) });
}

function withCacheBuster(url) {
  const separator = String(url || "").includes("?") ? "&" : "?";
  return `${url}${separator}t=${Date.now()}`;
}

export function parseUnifiedJobsPayload(payload, jobsParsing) {
  if (jobsParsing && typeof jobsParsing.parseUnifiedJobsPayload === "function") {
    return jobsParsing.parseUnifiedJobsPayload(payload);
  }
  let rows = [];
  if (Array.isArray(payload)) rows = payload;
  else if (payload && typeof payload === "object") {
    rows = Array.isArray(payload.jobs) ? payload.jobs : Array.isArray(payload.items) ? payload.items : [];
  }
  return rows.filter(row => row && typeof row === "object");
}

export function parseCSVLarge(csv, options = {}) {
  const { jobsParsing, parserDeps } = options;
  if (jobsParsing && typeof jobsParsing.parseCSVLarge === "function") {
    return jobsParsing.parseCSVLarge(csv, parserDeps || {});
  }
  return [];
}

function resolveSheetsFallbackSources(options = {}) {
  const list = Array.isArray(options.sheetsFallbackSources) ? options.sheetsFallbackSources : [];
  if (list.length > 0) {
    return list.filter(row => row && typeof row.sheetId === "string" && row.sheetId.trim());
  }
  if (options.sheetsFallbackSource && typeof options.sheetsFallbackSource.sheetId === "string") {
    return [options.sheetsFallbackSource];
  }
  return [];
}

async function fetchFromGoogleSheets(options = {}) {
  const {
    setSourceStatus,
    parseCSV,
    fetcher = fetchWithTimeout,
    timeoutMs = 20000
  } = options;
  const sheets = resolveSheetsFallbackSources(options);
  const sources = [];
  sheets.forEach((sheet, index) => {
    const gid = String(sheet.gid ?? "0");
    const csvUrl = `https://docs.google.com/spreadsheets/d/${sheet.sheetId}/export?format=csv&gid=${gid}`;
    const label = index === 0 ? "Google Sheets fallback" : `Google Sheets fallback ${index + 1}`;
    sources.push({ name: label, url: csvUrl });
    sources.push({ name: `${label} mirror`, url: `https://api.allorigins.win/raw?url=${encodeURIComponent(csvUrl)}` });
  });

  for (const source of sources) {
    try {
      if (typeof setSourceStatus === "function") setSourceStatus(`Fetching from ${source.name}...`);
      const response = await fetcher(source.url, Number(timeoutMs) > 0 ? Number(timeoutMs) : 20000);
      if (!response.ok) continue;
      const csv = await response.text();
      if (!csv || csv.length < 100) continue;
      const jobs = parseCSV(csv);
      if (jobs.length > 0) return { jobs, error: "", sourceName: source.name };
    } catch {
      // Try next source.
    }
  }

  return {
    jobs: null,
    error: "Could not fetch listings from the Sheets fallback feed.",
    sourceName: ""
  };
}

// ponytail: raced instead of sequential so one dead/slow mirror can't chain
// 20s timeouts into a long feed load; tiers still prefer JSON over CSV over Sheets.
async function raceFeedSources(sources, attemptOne, setSourceStatus, label) {
  const candidateSources = (Array.isArray(sources) ? sources : []).filter(s => s && s.url);
  if (candidateSources.length === 0) {
    return { jobs: null, error: "", sourceName: "" };
  }
  if (typeof setSourceStatus === "function") {
    setSourceStatus(candidateSources.length > 1
      ? `Checking ${candidateSources.length} ${label}...`
      : `Fetching from ${candidateSources[0].name}...`);
  }
  try {
    return await Promise.any(candidateSources.map(async source => {
      const result = await attemptOne(source);
      if (!result || !Array.isArray(result.jobs) || result.jobs.length === 0) {
        throw new Error(`no jobs from ${source.name}`);
      }
      return { jobs: result.jobs, error: "", sourceName: source.name };
    }));
  } catch {
    return { jobs: null, error: "", sourceName: "" };
  }
}

export async function fetchUnifiedJobs(options = {}) {
  const {
    unifiedJsonSources,
    unifiedCsvSources,
    setSourceStatus,
    parseUnifiedPayload,
    parseCSV,
    fetcher = fetchWithTimeout,
    timeoutMs = 20000,
    allowSheetsFallback = true
  } = options;
  const requestTimeoutMs = Number(timeoutMs) > 0 ? Number(timeoutMs) : 20000;

  const jsonResult = await raceFeedSources(
    unifiedJsonSources,
    async source => {
      const response = await fetcher(withCacheBuster(source.url), requestTimeoutMs, {
        cache: "no-store",
        headers: { Accept: "application/json" }
      });
      if (!response.ok) throw new Error(`${source.name}: HTTP ${response.status}`);
      const payload = await response.json();
      return { jobs: parseUnifiedPayload(payload) };
    },
    setSourceStatus,
    "unified JSON feed sources"
  );
  if (jsonResult.jobs && jsonResult.jobs.length > 0) return jsonResult;

  const csvResult = await raceFeedSources(
    unifiedCsvSources,
    async source => {
      const response = await fetcher(withCacheBuster(source.url), requestTimeoutMs, {
        cache: "no-store",
        headers: { Accept: "text/csv,*/*" }
      });
      if (!response.ok) throw new Error(`${source.name}: HTTP ${response.status}`);
      const csvText = await response.text();
      if (!csvText || csvText.length < 100) throw new Error(`${source.name}: empty csv`);
      return { jobs: parseCSV(csvText) };
    },
    setSourceStatus,
    "unified CSV feed sources"
  );
  if (csvResult.jobs && csvResult.jobs.length > 0) return csvResult;

  if (allowSheetsFallback) {
    const sheetsFallback = await fetchFromGoogleSheets({
      sheetsFallbackSource: options.sheetsFallbackSource,
      sheetsFallbackSources: options.sheetsFallbackSources,
      setSourceStatus,
      parseCSV,
      fetcher,
      timeoutMs: requestTimeoutMs
    });
    if (sheetsFallback.jobs && sheetsFallback.jobs.length > 0) return sheetsFallback;
  }
  return {
    jobs: null,
    error: allowSheetsFallback
      ? "Could not fetch listings from unified feeds or Sheets fallback source."
      : "Could not fetch listings from local unified feeds.",
    sourceName: ""
  };
}

function candidateClockMs() {
  return typeof performance !== "undefined" && typeof performance.now === "function"
    ? performance.now()
    : Date.now();
}

// A slow candidate row cannot say on its own whether the listener answered late or
// the answer sat in the renderer, so attach the browser's own resource timing and
// the page visibility it was measured under. Diagnostics only.
function candidateResourceTiming(url) {
  try {
    if (typeof performance === "undefined" || typeof performance.getEntriesByType !== "function") {
      return null;
    }
    const needle = String(url || "");
    const entries = performance.getEntriesByType("resource") || [];
    for (let index = entries.length - 1; index >= 0; index -= 1) {
      const entry = entries[index];
      if (!entry || !String(entry.name || "").includes(needle)) continue;
      return {
        startTimeMs: Math.round(Number(entry.startTime) || 0),
        responseStartMs: Math.round(Number(entry.responseStart) || 0),
        responseEndMs: Math.round(Number(entry.responseEnd) || 0),
        durationMs: Math.round(Number(entry.duration) || 0),
        connectMs: Math.round(
          Math.max(0, Number(entry.connectEnd || 0) - Number(entry.connectStart || 0))
        ),
        transferSize: Math.round(Number(entry.transferSize) || 0)
      };
    }
  } catch {
    return null;
  }
  return null;
}

function candidateVisibility() {
  try {
    return typeof document !== "undefined" ? String(document.visibilityState || "") : "";
  } catch {
    return "";
  }
}

// `jobs.html` hoists the first-render requests ahead of the module graph
// (`frontend/jobs/early-data.js`), so the answer can already be in the renderer
// by the time the feed asks for it. Take that in-flight response when it is
// still here and fetch on the caller's own clock otherwise; a hoist that failed
// resolves to null so the request is simply re-issued.
function takeHoistedResponse(url) {
  try {
    const hoisted = globalThis.__baluffoJobsEarlyData;
    return typeof hoisted?.take === "function" ? hoisted.take(String(url || "")) : null;
  } catch {
    return null;
  }
}

function reportCandidate(onCandidate, url, startedAt, status, ok, hoisted) {
  if (typeof onCandidate !== "function") return;
  const resource = candidateResourceTiming(url);
  try {
    onCandidate({
      url: String(url || ""),
      status: Math.max(0, Number(status) || 0),
      ok: Boolean(ok),
      ms: Math.max(0, Math.round(candidateClockMs() - startedAt)),
      hoisted: Boolean(hoisted),
      visibility: candidateVisibility(),
      resourceStartMs: resource?.startTimeMs ?? null,
      resourceResponseStartMs: resource?.responseStartMs ?? null,
      resourceResponseEndMs: resource?.responseEndMs ?? null,
      resourceConnectMs: resource?.connectMs ?? null
    });
  } catch {
    // Instrumentation must never change the fetch result.
  }
}

export async function fetchJsonFromCandidates(urls, options = {}) {
  const fetcher = options.fetcher || fetchWithTimeout;
  const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : 12000;
  const onCandidate = typeof options.onCandidate === "function" ? options.onCandidate : null;
  for (const url of urls || []) {
    const startedAt = candidateClockMs();
    const hoisted = takeHoistedResponse(url);
    let fromHoist = false;
    try {
      // The report probe gates the first render, and Chromium's network scheduler can
      // hold a page-start request back for most of a second when it reads as low
      // priority; the site answered in single-digit milliseconds in those runs.
      const hoistedResponse = await hoisted;
      fromHoist = Boolean(hoistedResponse);
      const response = hoistedResponse || await fetcher(withCacheBuster(url), timeoutMs, {
        cache: "no-store",
        priority: "high"
      });
      reportCandidate(onCandidate, url, startedAt, response?.status, Boolean(response?.ok), fromHoist);
      if (!response.ok) continue;
      return await response.json();
    } catch {
      reportCandidate(onCandidate, url, startedAt, 0, false, fromHoist);
      // Try next source.
    }
  }
  return null;
}
