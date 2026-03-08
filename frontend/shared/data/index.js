const COUNTRY_NAME_BY_CODE = {
  US: "United States",
  CA: "Canada",
  GB: "United Kingdom",
  UK: "United Kingdom",
  DE: "Germany",
  FI: "Finland",
  JP: "Japan",
  AU: "Australia",
  SG: "Singapore",
  FR: "France",
  NL: "Netherlands",
  SE: "Sweden",
  NO: "Norway",
  DK: "Denmark",
  ES: "Spain",
  IT: "Italy",
  BR: "Brazil",
  IN: "India",
  Remote: "Remote"
};

export function sanitizeUrl(url) {
  if (!url) return "";
  try {
    const parsed = new URL(url);
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      return parsed.href;
    }
    return "";
  } catch {
    return "";
  }
}

export function fullCountryName(code) {
  const value = String(code || "").trim();
  if (!value) return "";
  const upper = value.toUpperCase();
  return COUNTRY_NAME_BY_CODE[upper] || COUNTRY_NAME_BY_CODE[value] || value;
}

export function toContractClass(contractType) {
  const normalized = String(contractType || "").toLowerCase();
  if (normalized === "full-time") return "full-time";
  if (normalized === "internship") return "internship";
  if (normalized === "temporary") return "temporary";
  return "unknown";
}

export function getLastJobsUrl(storageKey, fallback = "jobs.html") {
  try {
    const url = sessionStorage.getItem(storageKey);
    if (!url) return fallback;
    if (!url.startsWith("/") && !url.startsWith("jobs.html")) return fallback;
    return url;
  } catch {
    return fallback;
  }
}
