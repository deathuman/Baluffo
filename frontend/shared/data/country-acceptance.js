const COUNTRY_ACCEPTANCE_URL = new URL("../../../data/contracts/country_acceptance.json", import.meta.url);

function normalizeCountryAcceptanceToken(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/g, "");
}

function normalizeCountryAcceptanceContract(data) {
  const countryNameByCode = {};
  for (const [code, name] of Object.entries(data?.countryNameByCode || {})) {
    const key = String(code || "").trim();
    const text = String(name || "").trim();
    if (!key || !text || countryNameByCode[key]) continue;
    countryNameByCode[key] = text;
  }

  const exactLabelMap = new Map();
  for (const label of Array.isArray(data?.acceptedExactLabels) ? data.acceptedExactLabels : []) {
    const token = normalizeCountryAcceptanceToken(label);
    const text = String(label || "").trim();
    if (!token || !text || exactLabelMap.has(token)) continue;
    exactLabelMap.set(token, text);
  }

  const aliasToCanonical = new Map();
  for (const [alias, canonical] of Object.entries(data?.normalizeAliasesToValue || {})) {
    const token = normalizeCountryAcceptanceToken(alias);
    const text = String(canonical || "").trim();
    if (!token || !text || aliasToCanonical.has(token)) continue;
    aliasToCanonical.set(token, text);
  }

  return {
    version: Number(data?.version || 1),
    countryNameByCode,
    exactLabelMap,
    aliasToCanonical
  };
}

async function loadCountryAcceptanceContract() {
  if (typeof process !== "undefined" && process?.versions?.node) {
    const [{ readFile }, { fileURLToPath }] = await Promise.all([
      import("node:fs/promises"),
      import("node:url")
    ]);
    const raw = await readFile(fileURLToPath(COUNTRY_ACCEPTANCE_URL), "utf8");
    return normalizeCountryAcceptanceContract(JSON.parse(raw));
  }

  const response = await fetch(COUNTRY_ACCEPTANCE_URL);
  if (!response.ok) {
    throw new Error(`Failed to load country acceptance contract: ${response.status}`);
  }
  return normalizeCountryAcceptanceContract(await response.json());
}

export const COUNTRY_ACCEPTANCE = await loadCountryAcceptanceContract();

export function resolveCountryAcceptanceValue(value) {
  const token = normalizeCountryAcceptanceToken(value);
  if (!token) return "";
  const raw = String(value || "").trim();
  const resolved = COUNTRY_ACCEPTANCE.aliasToCanonical.get(token)
    || COUNTRY_ACCEPTANCE.exactLabelMap.get(token)
    || COUNTRY_ACCEPTANCE.countryNameByCode[raw]
    || COUNTRY_ACCEPTANCE.countryNameByCode[raw.toUpperCase()]
    || "";
  if (resolved) return resolved;
  for (const name of Object.values(COUNTRY_ACCEPTANCE.countryNameByCode)) {
    if (normalizeCountryAcceptanceToken(name) === token) return name;
  }
  return "";
}
