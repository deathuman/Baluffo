import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { request as playwrightRequest } from "@playwright/test";
import { buildWriteReport, BASE_URL, BRIDGE_BASE } from "./helpers/packaged-smoke-shared.mjs";

const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/fetch-evidence-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/fetch-evidence-output");

async function writeJson(name, payload) {
  await fs.mkdir(OUTPUT_DIR, { recursive: true });
  const target = path.join(OUTPUT_DIR, name);
  await fs.writeFile(target, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return target;
}

const writeReport = buildWriteReport(REPORT_PATH);

async function fetchBridgeJson(apiRequest, relativePath, label) {
  const response = await apiRequest.get(`${BRIDGE_BASE}${relativePath}`);
  assert.equal(response.ok(), true, `${label} request should succeed`);
  return response.json();
}

async function postBridgeJson(apiRequest, relativePath, data, label) {
  const response = await apiRequest.post(`${BRIDGE_BASE}${relativePath}`, { data });
  assert.equal(response.ok(), true, `${label} request should succeed`);
  return response.json();
}

function diagnosticsFor(storageHealth, surface) {
  return Array.isArray(storageHealth?.storage?.diagnostics)
    ? storageHealth.storage.diagnostics.filter(row => row?.surface === surface)
    : [];
}

function hasPassingDiagnostic(storageHealth, surface, codes) {
  const expected = new Set(codes);
  return diagnosticsFor(storageHealth, surface).some(
    row => row?.ok === true && expected.has(String(row?.code || ""))
  );
}

function registryJournalMetricsAreBounded(storageMetrics) {
  const registryMetrics = storageMetrics?.registryJournals || storageMetrics?.jsonJournals || {};
  const rows = Array.isArray(registryMetrics)
    ? registryMetrics
    : Object.values(registryMetrics).filter(row => row && typeof row === "object");
  if (!rows.length) {
    return true;
  }
  return rows.every(row => {
    const bytes = Number(row?.bytes || row?.sizeBytes || 0);
    const compactMax = Number(row?.compactMaxBytes || row?.maxBytes || 1_048_576);
    return Number.isFinite(bytes) && bytes <= Math.max(compactMax * 4, 4_194_304);
  });
}

async function main() {
  const scenarios = [];
  const errors = [];
  let apiRequest;
  try {
    apiRequest = await playwrightRequest.newContext({ baseURL: BRIDGE_BASE });

    const fetchEvidence = {
      name: "Packaged M6 fetch evidence",
      slug: "packaged-m6-fetch-evidence",
      status: "passed",
      durationMs: 0,
      error: ""
    };
    scenarios.push(fetchEvidence);

    const startedAt = Date.now();
    try {
      const startPayload = await postBridgeJson(
        apiRequest,
        "/tasks/run-fetcher",
        { preset: "default", quiet: true, socialEnabled: false },
        "packaged fetch evidence run"
      );
      assert.equal(Boolean(startPayload?.started), true, "fetch evidence run should start");
      const runId = String(startPayload?.runId || "").trim();
      assert.match(runId, /^fetch_[a-f0-9]{10}$/i, "fetch evidence run id should look valid");
      const deterministic = startPayload?.smokeMode === "source-runs";

      const fetchReport = await fetchBridgeJson(
        apiRequest,
        `/ops/fetch-report?runId=${encodeURIComponent(runId)}`,
        "fetch report"
      );
      const registrySummary = await fetchBridgeJson(
        apiRequest,
        "/registry/summary",
        "registry summary"
      );
      const storageHealth = await fetchBridgeJson(
        apiRequest,
        "/ops/storage-health",
        "storage health"
      );
      const storageMetrics = await fetchBridgeJson(
        apiRequest,
        "/ops/storage-metrics",
        "storage metrics"
      );
      const feedResponse = await apiRequest.get(
        `${BASE_URL}/data/jobs-unified-light.json?m6=${encodeURIComponent(runId)}`
      );
      assert.equal(feedResponse.ok(), true, "static jobs feed should be served");
      const staticJobsFeed = await feedResponse.json();

      await writeJson("storage-health.post-fetch.json", storageHealth);
      await writeJson("storage-metrics.post-fetch.json", storageMetrics);
      await writeJson("fetch-report.post-fetch.json", fetchReport);
      await writeJson("registry-summary.post-fetch.json", registrySummary);
      await writeJson("jobs-unified-light.sample.json", staticJobsFeed);

      assert.equal(storageHealth?.storage?.migrationVersion, "008");
      assert.equal(storageHealth?.storage?.authorityModes?.sourceRuns, "sqlite");
      assert.equal(storageHealth?.storage?.authorityModes?.jobsFeed, "sqlite");
      assert.equal(storageHealth?.storage?.authorityModes?.sourceRegistry, "sqlite");
      assert.equal(fetchReport?.runId, runId, "fetch report should match fetch evidence run");
      // The full /ops/fetch-report view does not expose a `source` field; SQLite
      // authority is already pinned via storageHealth.authorityModes.sourceRuns above.
      assert.ok(registryJournalMetricsAreBounded(storageMetrics));
      assert.ok(
        hasPassingDiagnostic(storageHealth, "sourceRuns", [
          "source_runs_projection_match",
          "source_runs_read_projection_match"
        ]),
        "storage diagnostics should include passing source-runs diagnostics"
      );
      assert.ok(
        hasPassingDiagnostic(storageHealth, "jobsFeed", ["jobs_feed_projection_match"]),
        "storage diagnostics should include passing jobs-feed diagnostics"
      );
      assert.ok(
        hasPassingDiagnostic(storageHealth, "sourceRegistry", [
          "source_registry_projection_match",
          "source_registry_seeded_from_json"
        ]),
        "storage diagnostics should include passing source-registry diagnostics"
      );

      if (deterministic) {
        assert.equal(startPayload?.smokeMode, "source-runs");
        assert.equal(fetchReport?.sources?.[0]?.name, "Packaged Smoke Source");
        assert.equal(fetchReport?.sources?.[0]?.details?.[0]?.name, "Packaged Smoke Job");
        assert.equal(fetchReport?.sources?.[0]?.details?.[0]?.name, "Packaged Smoke Job");
        assert.equal(staticJobsFeed?.[0]?.title, "Packaged Smoke Job");
        assert.equal(staticJobsFeed?.[0]?.company, "Packaged Smoke Studio");
      }

      const evidenceSummary = {
        ok: true,
        deterministic,
        runId,
        migrationVersion: storageHealth?.storage?.migrationVersion,
        authorityModes: storageHealth?.storage?.authorityModes || {},
        diagnostics: {
          sourceRuns: diagnosticsFor(storageHealth, "sourceRuns"),
          jobsFeed: diagnosticsFor(storageHealth, "jobsFeed"),
          sourceRegistry: diagnosticsFor(storageHealth, "sourceRegistry")
        },
        compactFetchReportHydrated: Boolean(fetchReport?.sources?.[0]?.details?.length),
        sourceDetailsQuerySource: fetchReport?.source || "",
        staticJobsFeedCount: Array.isArray(staticJobsFeed) ? staticJobsFeed.length : 0,
        registrySummary: registrySummary?.summary || registrySummary || {},
        boundedRegistryJournalMetrics: registryJournalMetricsAreBounded(storageMetrics)
      };
      await writeJson("m6-fetch-evidence-summary.json", evidenceSummary);
    } catch (error) {
      fetchEvidence.status = "failed";
      fetchEvidence.error = error instanceof Error ? error.message : String(error);
      throw error;
    } finally {
      fetchEvidence.durationMs = Date.now() - startedAt;
    }
  } catch (error) {
    errors.push(error instanceof Error ? error.stack || error.message : String(error));
  } finally {
    if (apiRequest) {
      await apiRequest.dispose();
    }
    await writeReport({
      ok: errors.length === 0,
      scenarios,
      errors,
      outputDir: OUTPUT_DIR
    });
  }
  if (errors.length) {
    throw new Error(errors[0]);
  }
}

await main();
