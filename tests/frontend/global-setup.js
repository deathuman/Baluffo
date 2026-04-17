import { cpSync, mkdirSync, readdirSync, rmSync } from "fs";
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

async function startBridge(dataDir) {
  console.log("[bridge] Starting admin bridge...");

  return new Promise((resolve) => {
    const args = [
      "src/admin_bridge.py",
      "--port", "8877",
      "--host", "127.0.0.1",
      "--data-dir", dataDir
    ];

    bridgeProcess = spawn(playwrightPython, args, {
      stdio: "pipe",
      detached: false
    });

    let started = false;

    bridgeProcess.stdout.on("data", (data) => {
      const output = data.toString();
      console.log(`[bridge] ${output}`);
      if (!started && (output.includes("Uvicorn running on") || output.includes("Application startup complete"))) {
        started = true;
        setTimeout(resolve, 2000);
      }
    });

    bridgeProcess.stderr.on("data", (data) => {
      const output = data.toString();
      console.log(`[bridge] ${output}`);
    });

    bridgeProcess.on("error", (err) => {
      console.error(`[bridge] Error: ${err.message}`);
    });

    setTimeout(() => {
      if (!started) {
        console.log("[bridge] Started admin bridge (timeout)");
        resolve();
      }
    }, 10000);
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

export default async function globalSetup() {
  console.log("[setup] Starting admin bridge for smoke tests...");
  const dataDir = prepareBridgeDataDir();
  console.log(`[setup] Using isolated bridge data dir: ${dataDir}`);
  await startBridge(dataDir);
  console.log("[setup] Admin bridge started");
}

export async function globalTeardown() {
  stopBridge();
}
