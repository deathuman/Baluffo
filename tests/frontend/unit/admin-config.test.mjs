import test from "node:test";
import assert from "node:assert/strict";

function buildSessionStorage() {
  const state = new Map();
  return {
    getItem(key) {
      return state.has(key) ? state.get(key) : null;
    },
    setItem(key, value) {
      state.set(key, String(value));
    }
  };
}

test("AdminConfig loads generated frontend-safe defaults without XHR", async () => {
  global.window = {
    location: { href: "http://127.0.0.1:8080/admin.html" },
    sessionStorage: buildSessionStorage()
  };

  const { AdminConfig } = await import("../../../admin-config.js");
  assert.equal(AdminConfig.ADMIN_BRIDGE_BASE, "http://127.0.0.1:8877");
  assert.equal(AdminConfig.ADMIN_PIN_DEFAULT, "1234");
  assert.equal(AdminConfig.GITHUB_APP_ENABLED_DEFAULT, true);
});
