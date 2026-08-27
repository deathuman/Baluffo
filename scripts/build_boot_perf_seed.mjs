#!/usr/bin/env node
// ponytail: deterministic fixture feeds for the container Jobs boot-perf gate.
// The CI container boots with an empty BALUFFO_DATA_DIR (nothing feed-like is
// committed under data/), so the gate must mount a seeded data directory to
// make the bounded-boot contract observable. Usage:
//   node scripts/build_boot_perf_seed.mjs [targetDir]  (default .tmp/boot-perf-seed)
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";

const ROW_COUNT = 24;
const CITIES = [
  ["Stockholm", "SE"],
  ["Berlin", "DE"],
  ["Montreal", "CA"],
  ["Tokyo", "JP"]
];

function buildRow(index) {
  const [city, country] = CITIES[index % CITIES.length];
  return {
    id: index + 1,
    title: `Gameplay Programmer ${index + 1} (boot-perf fixture)`,
    company: `Fixture Studio ${index % 6}`,
    city,
    country,
    workType: "Hybrid",
    contractType: "Full-time",
    jobLink: `https://example.com/jobs/boot-perf-${index + 1}`,
    sector: "Game",
    profession: "gameplay-engineer",
    locations: [{ city, country }],
    locationSummary: `${city}, ${country}`,
    source: "fixture_seed",
    postedAt: "2026-08-01T09:00:00+00:00",
    status: "active",
    lastSeenAt: "2026-08-20T09:00:00+00:00",
    removedAt: "",
    lifecycleEvent: "preserved",
    lifecycleReason: "",
    availabilityId: "",
    availabilityStatus: "unknown",
    availabilityCheckedAt: "",
    availabilityVerifiedAt: "",
    availabilityUnavailableAt: "",
    availabilityEvidence: "",
    qualityScore: 0.8,
    focusScore: 0.8,
    sourceBundleCount: 1
  };
}

const rows = Array.from({ length: ROW_COUNT }, (_, index) => buildRow(index));
const target = process.argv[2] ?? ".tmp/boot-perf-seed";
mkdirSync(target, { recursive: true });
writeFileSync(path.join(target, "jobs-unified-startup.json"), `${JSON.stringify(rows)}\n`);
writeFileSync(path.join(target, "jobs-unified-light.json"), `${JSON.stringify(rows)}\n`);
writeFileSync(path.join(target, "jobs-fetch-report.json"), `${JSON.stringify({
  generatedAt: new Date().toISOString(),
  summary: { rowsUnified: rows.length, smokeMode: "boot-perf-seed" }
}, null, 2)}\n`);
console.log(`[seed] wrote ${rows.length} fixture rows + fetch report to ${target}`);
