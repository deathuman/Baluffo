export function hashFNV1a(input) {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

export function sanitizeJobUrl(url) {
  if (!url) return "";
  try {
    const parsed = new URL(url);
    return parsed.protocol === "https:" || parsed.protocol === "http:" ? parsed.href : "";
  } catch {
    return "";
  }
}

export function generateJobKey(job) {
  const explicit = String(job?.jobKey || "").trim();
  if (/^job_[a-f0-9]{8}$/i.test(explicit)) {
    return explicit.toLowerCase();
  }
  const salt = String(job?.keySalt || "").trim().toLowerCase();
  const canonicalLink = sanitizeJobUrl(job.jobLink || "").toLowerCase();
  const fallback = [
    job.title || "",
    job.company || "",
    job.city || "",
    job.country || ""
  ].join("|").toLowerCase();
  const seedBase = canonicalLink || fallback;
  const seed = salt ? `${seedBase}|${salt}` : seedBase;
  return `job_${hashFNV1a(seed)}`;
}

export function buildAttachmentPath(uid, jobKey, filename) {
  const base = `users/${uid}/saved_jobs/${jobKey}`;
  return filename ? `${base}/${filename}` : `${base}/`;
}

export function nowIso() {
  return new Date().toISOString();
}

export function normalizeSectorValue(sector, companyType = "") {
  const raw = String(sector || "").trim();
  const lower = raw.toLowerCase();
  if (lower === "game" || lower === "game company" || lower === "gaming") return "Game";
  if (lower === "tech" || lower === "tech company" || lower === "technology") return "Tech";
  const ct = String(companyType || "").trim().toLowerCase();
  if (ct === "game" || ct === "game company") return "Game";
  if (ct === "tech" || ct === "tech company") return "Tech";
  return raw || "Tech";
}

export function normalizeCustomSourceLabel(label) {
  const text = String(label || "").trim();
  return text || "Personal";
}
