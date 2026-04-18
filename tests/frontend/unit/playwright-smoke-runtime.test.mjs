import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  cleanupSmokeBridge,
  readBridgeMeta,
  startSmokeBridge,
} from "../../frontend/smoke-bridge-runtime.js";

function createFakeBridgeProcess(pid = 321) {
  const processEmitter = new EventEmitter();
  processEmitter.pid = pid;
  processEmitter.stdout = new EventEmitter();
  processEmitter.stderr = new EventEmitter();
  processEmitter.killCalls = 0;
  processEmitter.kill = () => {
    processEmitter.killCalls += 1;
    processEmitter.emit("exit", 0, null);
    return true;
  };
  return processEmitter;
}

test("startSmokeBridge writes bridge metadata for a successful setup", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "baluffo-playwright-smoke-"));
  try {
    const metaPath = path.join(tmpDir, "bridge-meta.json");
    const logPath = path.join(tmpDir, "bridge.log");
    const bridgeProcess = createFakeBridgeProcess(4321);

    const result = await startSmokeBridge({
      dataDir: path.join(tmpDir, "bridge-data"),
      port: 9988,
      metaPath,
      logPath,
      spawnImpl: () => bridgeProcess,
      waitForBridgeReadyImpl: async () => {},
    });

    assert.equal(result.bridgeProcess, bridgeProcess);
    const meta = readBridgeMeta({ metaPath });
    assert.equal(meta.bridgePort, 9988);
    assert.equal(meta.bridgePid, 4321);
    assert.match(String(meta.bridgeDataDir || ""), /bridge-data/);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("cleanupSmokeBridge stops a recorded pid and removes stale metadata", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "baluffo-playwright-smoke-"));
  try {
    const metaPath = path.join(tmpDir, "bridge-meta.json");
    const logPath = path.join(tmpDir, "bridge.log");
    const calls = [];
    await writeFile(
      metaPath,
      `${JSON.stringify({
        bridgeHost: "127.0.0.1",
        bridgePort: 8877,
        bridgePid: 5555,
      })}\n`,
      "utf8"
    );

    await cleanupSmokeBridge({
      metaPath,
      logPath,
      pidIsRunningImpl: pid => {
        calls.push(["running", pid]);
        return pid === 5555;
      },
      stopBridgePidImpl: pid => {
        calls.push(["stop", pid]);
        return true;
      },
      waitForPidExitImpl: async pid => {
        calls.push(["wait", pid]);
        return true;
      },
    });

    assert.deepEqual(calls, [
      ["running", 5555],
      ["stop", 5555],
      ["wait", 5555],
    ]);
    const meta = readBridgeMeta({ metaPath });
    assert.deepEqual(meta, {});
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

test("startSmokeBridge cleans up when readiness fails", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "baluffo-playwright-smoke-"));
  try {
    const metaPath = path.join(tmpDir, "bridge-meta.json");
    const logPath = path.join(tmpDir, "bridge.log");
    const bridgeProcess = createFakeBridgeProcess(8765);
    const cleanupCalls = [];

    await assert.rejects(
      startSmokeBridge({
        dataDir: path.join(tmpDir, "bridge-data"),
        port: 8899,
        metaPath,
        logPath,
        spawnImpl: () => bridgeProcess,
        waitForBridgeReadyImpl: async () => {
          throw new Error("bridge not ready");
        },
        cleanupImpl: async args => {
          cleanupCalls.push(args);
          await cleanupSmokeBridge({
            ...args,
            pidIsRunningImpl: () => true,
            stopBridgePidImpl: () => true,
            waitForPidExitImpl: async () => true,
          });
        },
      }),
      /bridge not ready/
    );

    assert.equal(cleanupCalls.length, 1);
    assert.equal(cleanupCalls[0].processHandle, bridgeProcess);
    assert.deepEqual(readBridgeMeta({ metaPath }), {});
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});
