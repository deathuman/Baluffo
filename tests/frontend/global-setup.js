import { cpSync, mkdirSync, readdirSync, rmSync, writeFileSync } from "fs";
import net from "net";
import path from "path";
import { spawn } from "child_process";

function resolvePlaywrightPythonCommand() {
  if (process.env.PLAYWRIGHT_PYTHON) {
    return process.env.PLAYWRIGHT_PYTHON;
  }
  if (process.platform === "win32") {
    return "python";
  }
  return "python3";
}

const playwrightPython = resolvePlaywrightPythonCommand();
const repoDataDir = path.resolve("data");
const playwrightTmpDir = path.resolve(".tmp", "playwright");
const bridgeDataDir = path.join(playwrightTmpDir, "admin-bridge-data");
const bridgeMetaPath = path.join(playwrightTmpDir, "bridge-meta.json");
const bridgeDataExcludes = new Set(["local-user-data", "updater"]);

let bridgeProcess = null;

function prepareBridgeDataDir() {
  mkdirSync(playwrightTmpDir, { recursive: true });
  rmSync(bridgeDataDir, { recursive: true, force: true });
  mkdirSync(bridgeDataDir, { recursive: true });

  for (const entry of readdirSync(repoDataDir)) {
    if (bridgeDataExcludes.has(entry)) {
      continue;
    }
    cpSync(path.join(repoDataDir, entry), path.join(bridgeDataDir, entry), {
      recursive: true,
      force: true
    });
  }

  return bridgeDataDir;
}

function reserveBridgePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = address && typeof address === "object" ? Number(address.port) : 0;
      server.close((closeErr) => {
        if (closeErr) {
          reject(closeErr);
          return;
        }
        if (!port) {
          reject(new Error("Could not reserve an isolated bridge port for Playwright."));
          return;
        }
        resolve(port);
      });
    });
  });
}

async function waitForBridgeReady(port, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = "";
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/ops/health`);
      if (response.ok) {
        return;
      }
      lastError = `HTTP ${response.status}`;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error(`Timed out waiting for admin bridge on port ${port}${lastError ? ` (${lastError})` : ""}.`);
}

async function startBridge(dataDir, port) {
  console.log(`[bridge] Starting admin bridge on port ${port}...`);

  return new Promise((resolve, reject) => {
    const args = [
      "src/admin_bridge.py",
      "--port", String(port),
      "--host", "127.0.0.1",
      "--data-dir", dataDir
    ];

    bridgeProcess = spawn(playwrightPython, args, {
      stdio: "pipe",
      detached: false
    });

    let settled = false;

    bridgeProcess.stdout.on("data", (data) => {
      const output = data.toString();
      console.log(`[bridge] ${output}`);
    });

    bridgeProcess.stderr.on("data", (data) => {
      const output = data.toString();
      console.log(`[bridge] ${output}`);
    });

    bridgeProcess.on("error", (err) => {
      console.error(`[bridge] Error: ${err.message}`);
      if (settled) return;
      settled = true;
      reject(err);
    });

    bridgeProcess.on("exit", (code, signal) => {
      if (settled) return;
      settled = true;
      reject(new Error(`Admin bridge exited before readiness check completed (code=${code}, signal=${signal ?? "none"}).`));
    });

    waitForBridgeReady(port).then(() => {
      if (settled) return;
      settled = true;
      resolve();
    }).catch((error) => {
      if (settled) return;
      settled = true;
      reject(error);
    });
  });
}

function stopBridge() {
  if (bridgeProcess) {
    try {
      bridgeProcess.kill();
    } catch {
      // ignore
    }
    bridgeProcess = null;
    console.log("[bridge] Stopped admin bridge");
  }
}

function writeBridgeMeta(port) {
  mkdirSync(playwrightTmpDir, { recursive: true });
  writeFileSync(bridgeMetaPath, JSON.stringify({
    bridgeHost: "127.0.0.1",
    bridgePort: Number(port)
  }, null, 2));
}

export default async function globalSetup() {
  console.log("[setup] Starting admin bridge for smoke tests...");
  const dataDir = prepareBridgeDataDir();
  const port = await reserveBridgePort();
  writeBridgeMeta(port);
  console.log(`[setup] Using isolated bridge data dir: ${dataDir}`);
  console.log(`[setup] Using isolated bridge port: ${port}`);
  try {
    await startBridge(dataDir, port);
  } catch (error) {
    stopBridge();
    throw error;
  }
  console.log("[setup] Admin bridge started");
}

export async function globalTeardown() {
  stopBridge();
}
