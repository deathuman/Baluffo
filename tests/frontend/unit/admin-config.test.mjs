import test from "node:test";
import assert from "node:assert/strict";
import { importFresh } from "./helpers/browser-test-helpers.mjs";

function buildSessionStorage() {
  const state = new Map();
  return {
    getItem(key) {
      return state.has(key) ? state.get(key) : null;
    },
    setItem(key, value) {
      state.set(key, String(value));
    },
    removeItem(key) {
      state.delete(String(key));
    }
  };
}

test("AdminConfig loads generated frontend-safe defaults without XHR", async () => {
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  global.window = {
    location: { href: "http://127.0.0.1:8080/admin.html" },
    sessionStorage: buildSessionStorage()
  };

  const { AdminConfig } = await importFresh(
    "../../../frontend/shared/config/admin-config.js",
    { relativeTo: import.meta.url }
  );
  assert.equal(AdminConfig.ADMIN_BRIDGE_BASE, "http://127.0.0.1:8877");
  assert.equal(AdminConfig.DESKTOP_JOBS_COLD_START, false);
  assert.equal(AdminConfig.GITHUB_APP_ENABLED_DEFAULT, true);
});

test("AdminConfig uses desktop-served runtime config when URL lacks bridge params", async () => {
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze({
    bridge: {
      host: "127.0.0.1",
      port: 61234
    },
    security: {
      github_app_enabled_default: true
    },
    runtime: {
      desktop: true
    }
  });
  global.window = {
    location: { href: "http://127.0.0.1:8080/jobs.html?desktop=1" },
    sessionStorage: buildSessionStorage()
  };

  const { AdminConfig } = await importFresh(
    "../../../frontend/shared/config/admin-config.js",
    { relativeTo: import.meta.url }
  );

  assert.equal(AdminConfig.ADMIN_BRIDGE_BASE, "http://127.0.0.1:61234");
  assert.equal(AdminConfig.DESKTOP_JOBS_COLD_START, false);
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
});

test("AdminConfig uses relative bridge base for explicit same-origin runtime config", async () => {
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze({
    bridge: {
      sameOrigin: true
    },
    runtime: {
      mode: "container",
      localDataMode: "bridge"
    }
  });
  const sessionStorage = buildSessionStorage();
  sessionStorage.setItem("baluffo_runtime_bridge_base", "http://127.0.0.1:8877");
  global.window = {
    location: {
      href: "http://192.168.50.61:8877/jobs.html?bridgePort=8877&bridgeHost=127.0.0.1"
    },
    sessionStorage
  };

  const { AdminConfig } = await importFresh(
    "../../../frontend/shared/config/admin-config.js",
    { relativeTo: import.meta.url }
  );

  assert.equal(AdminConfig.ADMIN_BRIDGE_BASE, "");
  assert.equal(sessionStorage.getItem("baluffo_runtime_bridge_base"), "");
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
});

test("AdminConfig treats runtime jobsColdStart as a desktop cold-start signal", async () => {
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze({
    bridge: {
      host: "127.0.0.1",
      port: 61236
    },
    runtime: {
      desktop: true,
      jobsColdStart: true
    }
  });
  global.window = {
    location: { href: "http://127.0.0.1:8080/jobs.html?desktop=1" },
    sessionStorage: buildSessionStorage()
  };

  const { AdminConfig } = await importFresh(
    "../../../frontend/shared/config/admin-config.js",
    { relativeTo: import.meta.url }
  );

  assert.equal(AdminConfig.DESKTOP_JOBS_COLD_START, true);
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
});

test("AdminConfig treats jobsColdStart URL flag as an independent fallback", async () => {
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  global.window = {
    location: { href: "http://127.0.0.1:8080/jobs.html?desktop=1&jobsColdStart=1" },
    sessionStorage: buildSessionStorage()
  };

  const { AdminConfig } = await importFresh(
    "../../../frontend/shared/config/admin-config.js",
    { relativeTo: import.meta.url }
  );

  assert.equal(AdminConfig.DESKTOP_JOBS_COLD_START, true);
});

test("AdminConfig lets explicit runtime cold-start false override stale URL flag", async () => {
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze({
    bridge: {
      host: "127.0.0.1",
      port: 61236
    },
    runtime: {
      desktop: true,
      jobsColdStart: false
    }
  });
  global.window = {
    location: { href: "http://127.0.0.1:8080/jobs.html?desktop=1&jobsColdStart=1" },
    sessionStorage: buildSessionStorage()
  };

  const { AdminConfig } = await importFresh(
    "../../../frontend/shared/config/admin-config.js",
    { relativeTo: import.meta.url }
  );

  assert.equal(AdminConfig.DESKTOP_JOBS_COLD_START, false);
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
});

test("AdminConfig overwrites stale cached bridge base with active desktop runtime config", async () => {
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze({
    bridge: {
      host: "127.0.0.1",
      port: 61235
    },
    security: {
      github_app_enabled_default: true
    },
    runtime: {
      desktop: true
    }
  });
  const sessionStorage = buildSessionStorage();
  sessionStorage.setItem("baluffo_runtime_bridge_base", "http://127.0.0.1:8877");
  global.window = {
    location: { href: "http://127.0.0.1:8080/saved.html?desktop=1" },
    sessionStorage
  };

  const { AdminConfig } = await importFresh(
    "../../../frontend/shared/config/admin-config.js",
    { relativeTo: import.meta.url }
  );

  assert.equal(AdminConfig.ADMIN_BRIDGE_BASE, "http://127.0.0.1:61235");
  assert.equal(sessionStorage.getItem("baluffo_runtime_bridge_base"), "http://127.0.0.1:61235");
  delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
});
