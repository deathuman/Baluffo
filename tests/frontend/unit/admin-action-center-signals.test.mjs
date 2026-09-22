// Action Center signal integrity: the health route must actually carry the fields the
// evaluators read, a missing fetch age must not leak through as Infinity, and the two
// fetch-derived signals must stay mutually exclusive so the display cap is unreachable.
import test from "node:test";
import assert from "node:assert/strict";
import { cleanHealthPayload, cleanStoragePayload, cleanSyncPayload, createActionCenterFixture as createFixture } from "./helpers/action-center-fixture.mjs";

test("action center polls a route that actually carries alerts and kpis", async () => {
  // Regression guard. The controller used to poll `/ops/health?view=ready`, whose
  // payload is built by `compute_ops_health_ready()` and carries *neither* `alerts`
  // nor `kpis`. Both `evaluateStaleFetch` and `evaluateFailedSources` read those
  // keys, so two of the four signals could never fire — and every unit test still
  // passed, because they injected synthetic payloads keyed on whatever path the
  // controller happened to request. The test that would have caught it is this one:
  // assert the *route* carries the fields, not that the renderer can draw them.
  const { calls, controller } = createFixture();
  await controller.pollActionCenter({ includeStorage: false });

  const healthRoute = calls[0];
  assert.ok(
    healthRoute.startsWith("/ops/") && healthRoute.includes("view=summary"),
    `expected a cached ops summary route, got ${healthRoute}`,
  );
  assert.doesNotMatch(
    healthRoute,
    /\/ops\/health\?view=ready/,
    "the ready payload has no alerts/kpis, so the stale-fetch and failed-source signals die",
  );

  // Drive the controller from a payload shaped like the real route's, and assert
  // the signal that the ready payload could never produce actually renders.
  const { refs: liveRefs, controller: liveController } = createFixture({
    getBridge: async path => {
      if (path === healthRoute) {
        return {
          alerts: [{ id: "stale_fetch", severity: "warning" }],
          kpis: { lastSuccessfulFetchAge: "4d", failedSourceRatioLatest: 0 },
          alertsEvaluated: true
        };
      }
      if (path === "/sync/status?view=summary") return cleanSyncPayload();
      return null;
    }
  });
  await liveController.pollActionCenter({ includeStorage: false });
  assert.match(
    liveRefs.actionCenterItemsEl.innerHTML,
    /Last successful fetch was 4d ago/,
    "the stale-fetch signal must render from a real-route-shaped payload",
  );
});

test("action center never renders a non-finite fetch age as Infinityd", async () => {
  // `parseAge` returns Infinity for a missing or unparseable age, and `formatAge`
  // used to interpolate it directly, producing the literal string
  // "Last successful fetch was Infinityd ago" — which is reachable from any route
  // whose `kpis` omit `lastSuccessfulFetchAge`.
  const { refs, controller } = createFixture({
    getBridge: async path => {
      if (path === "/ops/fetch-kpis?view=summary") {
        return { alerts: [{ id: "stale_fetch" }], kpis: {} };
      }
      if (path === "/sync/status?view=summary") return cleanSyncPayload();
      return null;
    }
  });
  await controller.pollActionCenter({ includeStorage: false });
  const html = refs.actionCenterItemsEl.innerHTML;
  assert.doesNotMatch(html, /Infinity/, "a missing fetch age must not reach the UI as Infinity");
  assert.doesNotMatch(html, /NaN/);
  assert.match(html, /Last successful fetch was at an unknown time/);
});

test("action center renders every reachable signal and no unreachable overflow row", async () => {
  // Three is the ceiling: `stale_fetch` needs a fetch age above 12h and
  // `failed_sources` needs one at or below 12h, so they are mutually exclusive. The
  // reachable maximum is storage_health + sync_status + failed_sources, and all
  // three must render. The "View all" row that used to follow them could never
  // appear and has been removed.
  const { refs, controller } = createFixture({
    getBridge: async path => {
      if (path === "/ops/fetch-kpis?view=summary") {
        return { alerts: [], kpis: { lastSuccessfulFetchAge: "1h", failedSourceRatioLatest: 0.5 } };
      }
      if (path === "/sync/status?view=summary") {
        return {
          config: { enabled: true, ready: true, state: "remote_conflict" },
          runtime: { lastAction: "push", lastResult: "error", lastError: "is at a but expected b" }
        };
      }
      if (path === "/ops/storage-health") {
        return { ok: true, storage: { healthy: false, diagnostics: [{ ok: false }] } };
      }
      return null;
    }
  });

  await controller.pollActionCenter({ includeStorage: true });

  const html = refs.actionCenterItemsEl.innerHTML;
  for (const id of ["storage_health", "sync_status", "failed_sources"]) {
    assert.match(html, new RegExp(`data-signal="${id}"`), `${id} must render`);
  }
  assert.equal((html.match(/class="action-center-signal /g) || []).length, 3);
  assert.doesNotMatch(html, /view-all/);
});

test("stale_fetch and failed_sources are mutually exclusive in the live signal set", async () => {
  // This is the invariant that makes the display cap unreachable. If a future change
  // lets both fire together, the signal count reaches four, the cap binds, and the
  // removed overflow row becomes necessary again — so this test is the tripwire.
  const at = async (age, alerts, ratio) => {
    const { refs, controller } = createFixture({
      getBridge: async path => {
        if (path === "/ops/fetch-kpis?view=summary") {
          return { alerts, kpis: { lastSuccessfulFetchAge: age, failedSourceRatioLatest: ratio } };
        }
        if (path === "/sync/status?view=summary") return { config: { enabled: false }, runtime: {} };
        if (path === "/ops/storage-health") return cleanStoragePayload();
        return null;
      }
    });
    await controller.pollActionCenter({ includeStorage: true });
    return [...new Set([...refs.actionCenterItemsEl.innerHTML.matchAll(/data-signal="([^"]+)"/g)].map(m => m[1]))];
  };

  const stale = await at("30h", [{ id: "stale_fetch" }], 0.5);
  assert.deepEqual(stale, ["stale_fetch"], "a stale fetch must not also report failed sources");

  const fresh = await at("1h", [], 0.5);
  assert.deepEqual(fresh, ["failed_sources"], "failed sources require a recent successful fetch");
});
