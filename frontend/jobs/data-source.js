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
    legacySheetsSource,
    setSourceStatus,
    parseCSV,
    fetcher = fetchWithTimeout
  } = options;
  const csvUrl = `https://docs.google.com/spreadsheets/d/${legacySheetsSource.sheetId}/export?format=csv&gid=${legacySheetsSource.gid}`;
  const sources = [
    { name: "Google Sheets fallback", url: csvUrl },
    { name: "AllOrigins mirror fallback", url: `https://api.allorigins.win/raw?url=${encodeURIComponent(csvUrl)}` }
  ];

  for (const source of sources) {
    try {
      if (typeof setSourceStatus === "function") setSourceStatus(`Fetching from ${source.name}...`);
      const response = await fetcher(source.url, 20000);
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
    error: "Could not fetch listings from the fallback sheets feed.",
    sourceName: ""
  };
}

export async function fetchUnifiedJobs(options = {}) {
  const {
    unifiedJsonSources,
    unifiedCsvSources,
    legacySheetsSource,
    setSourceStatus,
    parseUnifiedPayload,
    parseCSV,
    fetcher = fetchWithTimeout
  } = options;

  for (const source of unifiedJsonSources || []) {
    try {
      if (typeof setSourceStatus === "function") setSourceStatus(`Fetching from ${source.name}...`);
      const response = await fetcher(source.url, 20000, { headers: { Accept: "application/json" } });
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
      const response = await fetcher(source.url, 20000, { headers: { Accept: "text/csv,*/*" } });
      if (!response.ok) continue;
      const csv = await response.text();
      if (!csv || csv.length < 100) continue;
      const jobs = parseCSV(csv);
      if (jobs.length > 0) return { jobs, error: "", sourceName: source.name };
    } catch {
      // Try next source.
    }
  }

  const legacy = await fetchFromGoogleSheets({
    legacySheetsSource,
    setSourceStatus,
    parseCSV,
    fetcher
  });
  if (legacy.jobs && legacy.jobs.length > 0) return legacy;
  return {
    jobs: null,
    error: "Could not fetch listings from unified feeds or fallback sheets source.",
    sourceName: ""
  };
}

export async function fetchJsonFromCandidates(urls, options = {}) {
  const fetcher = options.fetcher || fetchWithTimeout;
  for (const url of urls || []) {
    try {
      const response = await fetcher(`${url}?t=${Date.now()}`, 12000, { cache: "no-store" });
      if (!response.ok) continue;
      return await response.json();
    } catch {
      // Try next source.
    }
  }
  return null;
}
