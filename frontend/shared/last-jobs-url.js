export function readLastJobsUrlFromSession(safeReadSession, storageKey, fallback = "jobs.html") {
  const rawUrl = String(safeReadSession(storageKey, "") || "").trim();
  if (!rawUrl) return fallback;
  try {
    const parsed = new URL(rawUrl, "http://baluffo.local/");
    const pathname = String(parsed.pathname || "").toLowerCase();
    if (pathname === "/jobs.html") {
      return rawUrl;
    }
  } catch {
    // Fall through to fallback.
  }
  return fallback;
}
