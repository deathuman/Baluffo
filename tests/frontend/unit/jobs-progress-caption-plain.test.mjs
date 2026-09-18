// ponytail: the end-user Jobs caption formatter lives in its own file so the
// technical formatter's suite (jobs-pipeline-progress-caption.test.mjs) stays
// within the repo test line budget. Both formatters are exported from the same
// module; this file covers the plain-language one only.
import test from "node:test";
import assert from "node:assert/strict";
import { formatJobsProgressCaption } from "../../../frontend/jobs/app/pipeline.js";

test("formatJobsProgressCaption keeps the end-user caption plain", () => {
  // The worst offender before this change was the discovery caption, which
  // leaked a raw adapter id plus stage/counter detail:
  // "Probing candidates · stage 2/5 · probing steam_curator_feeds ·
  //  138/500 candidates probed · generated 312 · endpoints 87 · queued 41"
  const discovery = formatJobsProgressCaption({
    taskType: "discovery",
    taskProgress: {
      phaseKey: "probing_candidates",
      phaseLabel: "Probing candidates",
      mode: "determinate",
      targetLabel: "steam_curator_feeds",
      counts: {
        stageIndex: 2,
        stageTotal: 5,
        generatedCandidates: 312,
        foundEndpoints: 87,
        queuedCandidates: 41,
        probedCandidates: 138,
        probeTotal: 500,
        estimatedRemainingMs: 240000
      }
    }
  });
  assert.equal(discovery, "Probing candidates · 138 of 500 sources · ~4m left");
  // No internal vocabulary of any kind reaches the caption. "Probing candidates"
  // is the human phase label and is allowed through; the adapter id, step
  // markers and raw counters are not.
  assert.doesNotMatch(discovery, /steam_curator_feeds|stage \d|generated|endpoints|queued|rate \d/i);

  // A determinate fetch run: phase, count and a rough ETA. The phase leads
  // because the button label above only names the coarser stage ("Fetching job
  // listings"), and the button tooltip no longer carries the breakdown.
  const fetch = formatJobsProgressCaption({
    taskType: "fetch",
    taskProgress: {
      phaseLabel: "Executing sources",
      mode: "determinate",
      counts: {
        resolvedSources: 512,
        sourceCount: 2135,
        completedSourcesPerMinute: 12,
        runningSourceNames: ["Studio A"],
        estimatedRemainingMs: 600000
      }
    }
  });
  assert.equal(fetch, "Executing sources · 512 of 2,135 · ~10m left");
  assert.doesNotMatch(fetch, /rate \d|Studio A/i);
  // The phase already names the thing being counted, so the count must not
  // stutter it back ("Executing sources · 512 of 2,135 sources").
  assert.doesNotMatch(fetch, /of 2,135 sources/i);

  // At most three segments, always.
  for (const label of [discovery, fetch]) {
    assert.ok(label.split(" · ").length <= 3, `too many segments: ${label}`);
  }
});

test("formatJobsProgressCaption falls back to a plain phase, never to internals", () => {
  // Indeterminate, no countable target: the plain phase is the only signal.
  assert.equal(
    formatJobsProgressCaption({
      taskType: "discovery",
      taskProgress: { phaseKey: "scanning_sources", phaseLabel: "Scanning sources", mode: "indeterminate" }
    }),
    "Scanning sources"
  );
  // An adapter-id-shaped phase must never be shown — not raw, and not
  // prettified into "Steam Curator Feeds" either.
  assert.equal(
    formatJobsProgressCaption({
      taskType: "discovery",
      taskProgress: { phaseKey: "steam_curator_feeds", mode: "indeterminate" }
    }),
    "Working…"
  );
  // A recognised stage token still maps through to its user-facing label.
  assert.equal(
    formatJobsProgressCaption({
      taskType: "discovery",
      taskProgress: { phaseKey: "discovery", mode: "indeterminate" }
    }),
    "Checking sources"
  );
  // A stage/step marker alone is internal detail, so the caption stays generic.
  assert.equal(
    formatJobsProgressCaption({
      taskType: "pipeline",
      taskProgress: { counts: { currentStep: 3, totalSteps: 11 } }
    }),
    "Working…"
  );
  // Zero counts must not render "0 of 0".
  assert.equal(
    formatJobsProgressCaption({
      taskType: "fetch",
      taskProgress: { phaseLabel: "Preparing sources", mode: "determinate", counts: { resolvedSources: 0, sourceCount: 0 } }
    }),
    "Preparing sources"
  );
  // Never a dangling separator when the ETA is unknown.
  assert.equal(
    formatJobsProgressCaption({
      taskType: "fetch",
      taskProgress: { mode: "determinate", counts: { resolvedSources: 7, sourceCount: 20 } }
    }),
    "7 of 20 sources"
  );
  // A human phase label keeps its own casing (no title-case mangling).
  assert.equal(
    formatJobsProgressCaption({
      taskType: "sync",
      taskProgress: { phaseLabel: "Pushing local jobs" }
    }),
    "Pushing local jobs"
  );
});
