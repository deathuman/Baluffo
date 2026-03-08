export function parseUnifiedJobsPayload(payload) {
    let rows = [];
    if (Array.isArray(payload)) {
      rows = payload;
    } else if (payload && typeof payload === "object") {
      if (Array.isArray(payload.jobs)) rows = payload.jobs;
      else if (Array.isArray(payload.items)) rows = payload.items;
    }
    return rows.filter(row => row && typeof row === "object");
  }

  function parseCSVRecords(csv) {
    const rows = [];
    let row = [];
    let field = "";
    let inQuotes = false;

    for (let i = 0; i < csv.length; i++) {
      const ch = csv[i];

      if (ch === '"') {
        if (inQuotes && csv[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
        continue;
      }

      if (ch === "," && !inQuotes) {
        row.push(field);
        field = "";
        continue;
      }

      if ((ch === "\n" || ch === "\r") && !inQuotes) {
        if (ch === "\r" && csv[i + 1] === "\n") i++;
        row.push(field);
        field = "";
        if (row.some(cell => cell.trim() !== "")) {
          rows.push(row);
        }
        row = [];
        continue;
      }

      field += ch;
    }

    if (field.length > 0 || row.length > 0) {
      row.push(field);
      if (row.some(cell => cell.trim() !== "")) {
        rows.push(row);
      }
    }

    return rows;
  }

  function findColumnIndexByPriority(headers, exactNames, containsNames) {
    const normalizedHeaders = headers.map(h => String(h || "").trim().toLowerCase());

    for (const name of exactNames) {
      const idx = normalizedHeaders.findIndex(h => h === name);
      if (idx !== -1) return idx;
    }

    for (let i = 0; i < normalizedHeaders.length; i++) {
      for (const name of containsNames) {
        if (normalizedHeaders[i].includes(name)) return i;
      }
    }
    return -1;
  }

  function findColumnIndex(headers, possibleNames) {
    return findColumnIndexByPriority(headers, possibleNames, possibleNames);
  }

  function findCompanyColumnIndex(headers) {
    const normalizedHeaders = headers.map(h => String(h || "").trim().toLowerCase());
    const exactIdx = normalizedHeaders.findIndex(h => h === "company name" || h === "company");
    if (exactIdx !== -1) return exactIdx;

    for (let i = 0; i < normalizedHeaders.length; i++) {
      const h = normalizedHeaders[i];
      if (!h.includes("company")) continue;
      if (h.includes("type") || h.includes("category") || h.includes("sector")) continue;
      return i;
    }
    return -1;
  }

  function findCompanyNameCandidateIndexes(headers, primaryIdx) {
    const normalizedHeaders = headers.map(h => String(h || "").trim().toLowerCase());
    const seen = new Set();
    const candidates = [];

    const pushIdx = idx => {
      if (idx < 0 || idx >= headers.length) return;
      if (seen.has(idx)) return;
      seen.add(idx);
      candidates.push(idx);
    };

    pushIdx(primaryIdx);
    normalizedHeaders.forEach((h, idx) => {
      const isNameLike =
        h.includes("company name") ||
        h === "company" ||
        h.includes("studio") ||
        h.includes("employer") ||
        h.includes("organization") ||
        h.includes("organisation");
      const isTypeLike = h.includes("type") || h.includes("category") || h.includes("sector") || h.includes("industry");
      if (isNameLike && !isTypeLike) pushIdx(idx);
    });
    return candidates;
  }

  function isGenericCompanyLabel(value) {
    const lower = String(value || "").trim().toLowerCase();
    return (
      lower === "game" ||
      lower === "tech" ||
      lower === "game company" ||
      lower === "tech company" ||
      lower === "gaming company" ||
      lower === "technology company"
    );
  }

  function resolveCompanyName(fields, primaryIdx, candidateIdxs) {
    const allCandidates = [];
    if (Number.isInteger(primaryIdx) && primaryIdx >= 0) {
      allCandidates.push(String(fields[primaryIdx] || "").trim());
    }
    (candidateIdxs || []).forEach(idx => {
      allCandidates.push(String(fields[idx] || "").trim());
    });

    for (const value of allCandidates) {
      if (value && !isGenericCompanyLabel(value)) return value;
    }
    for (const value of allCandidates) {
      if (value) return value;
    }
    return "";
  }

  export function parseCSVLarge(csv, deps) {
    const {
      mapProfession,
      normalizeSector,
      classifyCompanyType,
      detectWorkType,
      detectContractType,
      logInfo,
      logError
    } = deps || {};

    try {
      const startTime = performance.now();
      const rows = parseCSVRecords(csv);
      if (rows.length < 2) return [];

      let headerIdx = -1;
      for (let i = 0; i < Math.min(250, rows.length); i++) {
        const normalizedRow = rows[i].map(cell => cell.toLowerCase().trim()).filter(Boolean);
        const hasTitleHeader = normalizedRow.includes("title");
        const hasCompanyHeader = normalizedRow.includes("company name") || normalizedRow.includes("company");
        const hasLocationHeader = normalizedRow.includes("city") || normalizedRow.includes("country");
        if (hasTitleHeader && hasCompanyHeader && hasLocationHeader) {
          headerIdx = i;
          break;
        }
      }
      if (headerIdx === -1) return [];

      const headers = rows[headerIdx].map(h => h.toLowerCase().trim());
      const companyIdx = findCompanyColumnIndex(headers);
      const companyNameCandidateIdxs = findCompanyNameCandidateIndexes(headers, companyIdx);
      const titleIdx = findColumnIndex(headers, ["title", "role"]);
      const cityIdx = findColumnIndex(headers, ["city"]);
      const countryIdx = findColumnIndex(headers, ["country"]);
      const locationTypeIdx = findColumnIndex(headers, ["location type", "work type"]);
      const contractTypeIdx = findColumnIndex(headers, ["employment type", "contract type", "employment", "contract", "position type"]);
      const jobLinkIdx = findColumnIndex(headers, ["job link", "url", "apply"]);
      const sectorIdx = findColumnIndexByPriority(headers, ["sector", "industry", "company type", "company category"], []);

      if (titleIdx === -1 || companyIdx === -1) return [];

      const jobs = [];
      let uncategorizedCount = 0;
      for (let i = headerIdx + 1; i < rows.length; i++) {
        const fields = rows[i];
        if (!fields || fields.length === 0) continue;

        const title = (fields[titleIdx] || "").trim();
        const company = resolveCompanyName(fields, companyIdx, companyNameCandidateIdxs);
        const city = (fields[cityIdx] || "").trim();
        const country = (fields[countryIdx] || "Unknown").trim();
        const locationType = (fields[locationTypeIdx] || "On-site").trim();
        const contractTypeText = contractTypeIdx !== -1 ? (fields[contractTypeIdx] || "").trim() : "";
        const jobLink = jobLinkIdx !== -1 ? (fields[jobLinkIdx] || "").trim() : "";
        const sectorText = sectorIdx !== -1 ? (fields[sectorIdx] || "").trim() : "";

        if (!title || !company) continue;
        const profession = typeof mapProfession === "function" ? mapProfession(title) : "other";
        if (profession === "other") uncategorizedCount += 1;

        jobs.push({
          id: 1000 + i,
          title,
          company,
          sector: typeof normalizeSector === "function" ? normalizeSector(sectorText, company, title) : sectorText,
          companyType: typeof classifyCompanyType === "function" ? classifyCompanyType(company, title) : "",
          city,
          country,
          workType: typeof detectWorkType === "function" ? detectWorkType(locationType) : locationType,
          contractType: typeof detectContractType === "function" ? detectContractType(contractTypeText, title) : contractTypeText,
          profession,
          description: `${title} at ${company}`,
          jobLink
        });
      }

      if (typeof logInfo === "function") {
        logInfo(`Role mapper uncategorized: ${uncategorizedCount}/${jobs.length}`);
        const elapsedSec = ((performance.now() - startTime) / 1000).toFixed(2);
        logInfo(`Loaded ${jobs.length} jobs in ${elapsedSec}s`);
      }
      return jobs;
    } catch (err) {
      if (typeof logError === "function") {
        logError("Error parsing CSV", err);
      }
      return [];
    }
  }
export const BaluffoJobsParsing = {
  parseUnifiedJobsPayload,
  parseCSVLarge
};
