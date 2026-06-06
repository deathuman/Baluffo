import test from "node:test";
import assert from "node:assert/strict";

import {
  getSourceRegistryActiveUrlsForRuntime,
  getStartupPreviewJsonUrlsForRuntime
} from "../../../frontend/jobs/app/sources.js";

test("container startup sources avoid known 404 fallback probes", () => {
  const previousConfig = globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = { runtime: { mode: "container" } };
  try {
    assert.deepEqual(getStartupPreviewJsonUrlsForRuntime(), ["data/jobs-unified-startup.json"]);
    assert.deepEqual(getSourceRegistryActiveUrlsForRuntime(), []);
  } finally {
    if (previousConfig === undefined) {
      delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
    } else {
      globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = previousConfig;
    }
  }
});
