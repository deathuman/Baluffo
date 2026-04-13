const CITY_NOISE_URL = new URL("../../../data/contracts/city_noise_contract.json", import.meta.url);

function normalizeCityNoiseText(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function normalizeCityNoiseList(values) {
  const result = [];
  const seen = new Set();
  for (const value of Array.isArray(values) ? values : []) {
    const text = normalizeCityNoiseText(value);
    if (!text || seen.has(text)) continue;
    seen.add(text);
    result.push(text);
  }
  return result;
}

function normalizeCityNoiseContract(data) {
  return {
    version: Number(data?.version || 1),
    proseFragments: normalizeCityNoiseList(data?.proseFragments),
    sentencePrefixes: normalizeCityNoiseList(data?.sentencePrefixes),
    placeholderFragments: normalizeCityNoiseList(data?.placeholderFragments),
    knownJunkTokens: normalizeCityNoiseList(data?.knownJunkTokens)
  };
}

async function loadCityNoiseContract() {
  if (typeof process !== "undefined" && process?.versions?.node) {
    const [{ readFile }, { fileURLToPath }] = await Promise.all([
      import("node:fs/promises"),
      import("node:url")
    ]);
    const raw = await readFile(fileURLToPath(CITY_NOISE_URL), "utf8");
    return normalizeCityNoiseContract(JSON.parse(raw));
  }

  const response = await fetch(CITY_NOISE_URL);
  if (!response.ok) {
    throw new Error(`Failed to load city noise contract: ${response.status}`);
  }
  return normalizeCityNoiseContract(await response.json());
}

export const CITY_NOISE_CONTRACT = await loadCityNoiseContract();

export { normalizeCityNoiseText };
