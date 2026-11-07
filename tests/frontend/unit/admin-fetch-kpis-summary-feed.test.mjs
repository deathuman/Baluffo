import test from "node:test";
import assert from "node:assert/strict";

import { createFetchKpisSummaryFeed } from "../../../frontend/admin/app/fetch-kpis-summary-feed.js";
import { createFetchKpisHydration } from "../../../frontend/admin/app/ops/hydrate-fetch-kpis.js";
import {
  cleanHealthPayload,
  cleanSyncPayload,
  createActionCenterFixture
} from "./helpers/action-center-fixture.mjs";

const HEALTH_ROUTE = "/ops/fetch-kpis?view=summary";

test("fetch-kpis feed shares one in-flight request with every subscriber", async () => {
  const feed = createFetchKpisSummaryFeed();
  const payload = { kpis: { browserFallbackHealth: { ok: false } } };
  const seen = [];
  let calls = 0;
  feed.subscribe(value => seen.push(value));

  const first = feed.load(async () => {
    calls += 1;
    return payload;
  });
  const second = feed.load(async () => {
    calls += 1;
    return { kpis: { browserFallbackHealth: { ok: true } } };
  });

  assert.equal(await first, payload);
  assert.equal(await second, payload);
  assert.equal(calls, 1);
  assert.deepEqual(seen, [payload]);
});

test("fetch-kpis feed reuses a recent payload and refreshes once it is stale", async () => {
  let now = 1000;
  let calls = 0;
  const feed = createFetchKpisSummaryFeed({ now: () => now });
  const fetchPayload = async () => {
    calls += 1;
    return { call: calls };
  };

  assert.deepEqual(await feed.load(fetchPayload, { maxAgeMs: 5000 }), { call: 1 });
  assert.deepEqual(await feed.load(fetchPayload, { maxAgeMs: 5000 }), { call: 1 });
  now = 7000;
  assert.deepEqual(await feed.load(fetchPayload, { maxAgeMs: 5000 }), { call: 2 });
});

test("an Ops-side fetch shared through the feed clears the Action Center signal", async () => {
  const failedHealth = {
    alerts: [{ id: "stale_fetch" }],
    kpis: { lastSuccessfulFetchAge: "30h", failedSourceRatioLatest: 0 }
  };
  const calls = [];
  const feed = createFetchKpisSummaryFeed();
  const { refs, controller } = createActionCenterFixture({
    fetchKpisSummaryFeed: feed,
    getBridge: async path => {
      calls.push(path);
      if (path === HEALTH_ROUTE) return failedHealth;
      if (path === "/sync/status?view=summary") return cleanSyncPayload();
      return null;
    }
  });

  await controller.pollActionCenter({ includeStorage: false });
  assert.match(refs.actionCenterItemsEl.innerHTML, /data-signal="stale_fetch"/);
  const healthCallsBefore = calls.filter(path => path === HEALTH_ROUTE).length;
  const sequenceAfterPoll = refs.actionCenterItemsEl.attributes["data-poll-sequence"];
  assert.equal(sequenceAfterPoll, "1");

  // This is the Ops hydration path publishing through the shared feed. The
  // Action Center must repaint from the payload without issuing its own request.
  await feed.load(async () => cleanHealthPayload());

  assert.equal(calls.filter(path => path === HEALTH_ROUTE).length, healthCallsBefore);
  assert.doesNotMatch(refs.actionCenterItemsEl.innerHTML, /data-signal="stale_fetch"/);
  assert.match(refs.actionCenterItemsEl.innerHTML, /No immediate action from core signals/);
  assert.equal(
    refs.actionCenterItemsEl.attributes["data-poll-sequence"],
    sequenceAfterPoll,
    "a shared feed render must not masquerade as the controller's own poll"
  );

  // The next poll is still inside the feed's short freshness window, so it
  // must paint the shared payload rather than fetch the same route again.
  await controller.pollActionCenter({ includeStorage: false });
  assert.equal(calls.filter(path => path === HEALTH_ROUTE).length, healthCallsBefore);
  assert.doesNotMatch(refs.actionCenterItemsEl.innerHTML, /data-signal="stale_fetch"/);
  assert.equal(refs.actionCenterItemsEl.attributes["data-poll-sequence"], "2");
  controller.dispose();
});

test("Ops fetch-kpis hydration applies a refresh published by the Action Center", async () => {
  const feed = createFetchKpisSummaryFeed();
  const state = { latestOpsHealthCache: {} };
  const renders = [];

  createFetchKpisHydration({
    state,
    canHydrateCompactDuringActiveRun: () => true,
    hasPossibleActiveRunEvidence: () => false,
    markFetchKpisDeferredDuringActiveRun() {},
    renderOpsHealthSnapshot(_token, health) {
      renders.push(health);
    },
    getCachedTaskStatePayload: () => ({}),
    getCachedRegistryConflictsPayload: () => ({}),
    mergeOpsHealth: (existing, incoming) => ({ ...existing, ...incoming }),
    measuredGetBridge: async () => null,
    currentRenderToken: () => 1,
    isStaleRenderToken: () => false,
    OPS_FETCH_KPIS_SUMMARY_PATH: HEALTH_ROUTE,
    fetchKpisSummaryFeed: feed
  });

  await feed.load(async () => ({
    status: "healthy",
    kpis: { lastSuccessfulFetchAge: "2h" }
  }));

  assert.equal(state.latestOpsHealthCache.kpis.lastSuccessfulFetchAge, "2h");
  assert.equal(renders.length, 1);
  assert.equal(renders[0].kpis.lastSuccessfulFetchAge, "2h");
});
