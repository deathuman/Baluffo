import assert from "node:assert/strict";
import test from "node:test";
import { importFresh } from "./helpers/browser-test-helpers.mjs";

test("Admin runtime bridge base preserves explicit same-origin empty string", async () => {
  const { resolveAdminBridgeBase } = await importFresh(
    "../../../frontend/admin/app/runtime/bridge-base.js",
    { relativeTo: import.meta.url }
  );

  assert.equal(resolveAdminBridgeBase({ ADMIN_BRIDGE_BASE: "" }), "");
});

test("Admin runtime bridge base keeps legacy localhost fallback when missing", async () => {
  const { resolveAdminBridgeBase } = await importFresh(
    "../../../frontend/admin/app/runtime/bridge-base.js",
    { relativeTo: import.meta.url }
  );

  assert.equal(resolveAdminBridgeBase({}), "http://127.0.0.1:8877");
  assert.equal(resolveAdminBridgeBase({ ADMIN_BRIDGE_BASE: undefined }), "http://127.0.0.1:8877");
  assert.equal(resolveAdminBridgeBase({ ADMIN_BRIDGE_BASE: null }), "http://127.0.0.1:8877");
});

test("Admin same-origin bridge base produces relative API URLs", async () => {
  const fetchCalls = [];
  global.fetch = async (url) => {
    fetchCalls.push(String(url));
    return {
      ok: true,
      status: 200,
      json: async () => ({ ok: true })
    };
  };

  const { resolveAdminBridgeBase } = await importFresh(
    "../../../frontend/admin/app/runtime/bridge-base.js",
    { relativeTo: import.meta.url }
  );
  const { fetchJson } = await importFresh("../../../frontend/shared/api-client.js", {
    relativeTo: import.meta.url
  });

  await fetchJson(resolveAdminBridgeBase({ ADMIN_BRIDGE_BASE: "" }), "/ops/health", {
    timeoutMs: 1000
  });

  assert.equal(fetchCalls.length, 1);
  assert.match(fetchCalls[0], /^\/ops\/health\?t=\d+$/);
  assert.equal(fetchCalls[0].includes("127.0.0.1:8877"), false);
});
