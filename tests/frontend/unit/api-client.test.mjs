import test from "node:test";
import assert from "node:assert/strict";
import { importFresh } from "./helpers/browser-test-helpers.mjs";

function createOkResponse(payload = { ok: true }) {
  return {
    ok: true,
    status: 200,
    json: async () => payload
  };
}

test("fetchBridge uses relative URLs when bridge base is explicit same-origin", async () => {
  const fetchCalls = [];
  global.fetch = async (url, options = {}) => {
    fetchCalls.push({ url: String(url), options });
    return createOkResponse();
  };

  const { fetchBridge } = await importFresh("../../../frontend/shared/api-client.js", {
    relativeTo: import.meta.url
  });

  await fetchBridge("", "/ops/health", { timeoutMs: 1000 });

  assert.equal(fetchCalls.length, 1);
  assert.match(fetchCalls[0].url, /^\/ops\/health\?t=\d+$/);
  assert.equal(fetchCalls[0].url.includes("127.0.0.1:8877"), false);
});

test("fetchBridge keeps legacy localhost fallback when no bridge base is configured", async () => {
  const fetchCalls = [];
  global.fetch = async (url, options = {}) => {
    fetchCalls.push({ url: String(url), options });
    return createOkResponse();
  };

  const { fetchBridge } = await importFresh("../../../frontend/shared/api-client.js", {
    relativeTo: import.meta.url
  });

  await fetchBridge(undefined, "/ops/health", { timeoutMs: 1000 });

  assert.equal(fetchCalls.length, 1);
  assert.match(fetchCalls[0].url, /^http:\/\/127\.0\.0\.1:8877\/ops\/health\?t=\d+$/);
});
