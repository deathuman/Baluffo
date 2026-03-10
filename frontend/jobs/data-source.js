export async function fetchWithTimeout(url, timeoutMs, init = {}) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
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

export async function fetchFromGoogleSheets(options = {}) {
  const {
    sheetsFallbackSource,
    setSourceStatus,
    parseCSV,
    fetcher = fetchWithTimeout,
    timeoutMs = 20000
  } = options;
  const csvUrl = `https://docs.google.com/spreadsheets/d/${sheetsFallbackSource.sheetId}/export?format=csv&gid=${sheetsFallbackSource.gid}`;
  const sources = [
    { name: "Google Sheets fallback", url: csvUrl },
    { name: "AllOrigins mirror fallback", url: `https://api.allorigins.win/raw?url=${encodeURIComponent(csvUrl)}` }
  ];

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

export async function fetchUnifiedJobs(options = {}) {
  const {
    unifiedJsonSources,
    unifiedCsvSources,
    sheetsFallbackSource,
    setSourceStatus,
    parseUnifiedPayload,
    parseCSV,
    fetcher = fetchWithTimeout,
    timeoutMs = 20000,
    allowSheetsFallback = true
  } = options;
  const requestTimeoutMs = Number(timeoutMs) > 0 ? Number(timeoutMs) : 20000;

  for (const source of unifiedJsonSources || []) {
    try {
      if (typeof setSourceStatus === "function") setSourceStatus(`Fetching from ${source.name}...`);
      const response = await fetcher(source.url, requestTimeoutMs, { headers: { Accept: "application/json" } });
      if (!response.ok) continue;
      const payload = await response.json();
      const jobs = parseUnifiedPayload(payload);
      if (jobs.length > 0) return { jobs, error: "", sourceName: source.name };
    } catch {
      // Try next source.
    }
  }

  for (const source of unifiedCsvSources || []) {
    try {
      if (typeof setSourceStatus === "function") setSourceStatus(`Fetching from ${source.name}...`);
      const response = await fetcher(source.url, requestTimeoutMs, { headers: { Accept: "text/csv,*/*" } });
      if (!response.ok) continue;
      const csv = await response.text();
      if (!csv || csv.length < 100) continue;
      const jobs = parseCSV(csv);
      if (jobs.length > 0) return { jobs, error: "", sourceName: source.name };
    } catch {
      // Try next source.
    }
  }

  if (allowSheetsFallback) {
    const sheetsFallback = await fetchFromGoogleSheets({
      sheetsFallbackSource,
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

export async function fetchJsonFromCandidates(urls, options = {}) {
  const fetcher = options.fetcher || fetchWithTimeout;
  const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : 12000;
  for (const url of urls || []) {
    try {
      const response = await fetcher(`${url}?t=${Date.now()}`, timeoutMs, { cache: "no-store" });
      if (!response.ok) continue;
      return await response.json();
    } catch {
      // Try next source.
    }
  }
  return null;
}
