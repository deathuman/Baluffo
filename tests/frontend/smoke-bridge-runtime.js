import {
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "fs";
import net from "net";
import path from "path";
import { spawn, spawnSync } from "child_process";

export const repoDataDir = path.resolve("data");
export const playwrightTmpDir = path.resolve(".tmp", "playwright");
export const bridgeDataDir = path.join(playwrightTmpDir, "admin-bridge-data");
export const bridgeMetaPath = path.join(playwrightTmpDir, "bridge-meta.json");
export const bridgeLogPath = path.join(playwrightTmpDir, "bridge.log");
export const bridgeDataExcludes = new Set(["local-user-data", "updater"]);
export const BRIDGE_LOG_MAX_BYTES = 64 * 1024;
export const BRIDGE_EXIT_WAIT_TIMEOUT_MS = 5_000;
export const BRIDGE_READY_TIMEOUT_MS = 15_000;

export function resolvePlaywrightPythonCommand() {
  if (process.env.PLAYWRIGHT_PYTHON) {
    return process.env.PLAYWRIGHT_PYTHON;
  }
  if (process.platform === "win32") {
    return "python";
  }
  return "python3";
}

export function smokeBridgeDebugEnabled() {
  return String(process.env.BALUFFO_PLAYWRIGHT_BRIDGE_DEBUG || "").trim() === "1";
}

function safeUnlink(filePath) {
  try {
    unlinkSync(filePath);
  } catch {
    // Ignore missing-file cleanup errors.
  }
}

function safeRm(targetPath) {
  try {
    rmSync(targetPath, { recursive: true, force: true });
  } catch {
    // Ignore cleanup errors.
  }
}

function trimUtf8Tail(text, maxBytes) {
  let output = String(text || "");
  while (Buffer.byteLength(output, "utf8") > maxBytes && output.length > 0) {
    output = output.slice(Math.floor(output.length / 4));
  }
  return output;
}

export function appendBoundedLog(logPath, chunk, { maxBytes = BRIDGE_LOG_MAX_BYTES } = {}) {
  const text = Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk || "");
  if (!text) {
    return;
  }
  let existing = "";
  try {
    existing = readFileSync(logPath, "utf8");
  } catch {
    existing = "";
  }
  const next = trimUtf8Tail(`${existing}${text}`, maxBytes);
  mkdirSync(path.dirname(logPath), { recursive: true });
  writeFileSync(logPath, next, "utf8");
}

export function readBoundedLogTail(logPath) {
  try {
    const stats = statSync(logPath);
    if (!stats.isFile()) {
      return "";
    }
    const text = readFileSync(logPath, "utf8");
    return trimUtf8Tail(text, BRIDGE_LOG_MAX_BYTES);
  } catch {
    return "";
  }
}

export function writeBridgeMeta(meta, { metaPath = bridgeMetaPath } = {}) {
  mkdirSync(path.dirname(metaPath), { recursive: true });
  writeFileSync(metaPath, `${JSON.stringify(meta, null, 2)}\n`, "utf8");
  return meta;
}

export function readBridgeMeta({ metaPath = bridgeMetaPath } = {}) {
  try {
    const payload = JSON.parse(readFileSync(metaPath, "utf8"));
    return payload && typeof payload === "object" ? payload : {};
  } catch {
    return {};
  }
}

export function prepareBridgeDataDir({
  sourceDir = repoDataDir,
  targetDir = bridgeDataDir,
  excludes = bridgeDataExcludes,
} = {}) {
  mkdirSync(playwrightTmpDir, { recursive: true });
  safeRm(targetDir);
  mkdirSync(targetDir, { recursive: true });

  for (const entry of readdirSync(sourceDir)) {
    if (excludes.has(entry)) {
      continue;
    }
    cpSync(path.join(sourceDir, entry), path.join(targetDir, entry), {
      recursive: true,
      force: true,
    });
  }

  return targetDir;
}

export function reserveBridgePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = address && typeof address === "object" ? Number(address.port) : 0;
      server.close(closeErr => {
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

export async function waitForBridgeReady(port, timeoutMs = BRIDGE_READY_TIMEOUT_MS) {
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
  throw new Error(
    `Timed out waiting for admin bridge on port ${port}${lastError ? ` (${lastError})` : ""}.`
  );
}

export function pidIsRunning(pid) {
  const normalizedPid = Number(pid || 0);
  if (!normalizedPid) {
    return false;
  }
  try {
    process.kill(normalizedPid, 0);
    return true;
  } catch {
    return false;
  }
}

export function stopBridgePid(pid, { platform = process.platform, timeoutMs = BRIDGE_EXIT_WAIT_TIMEOUT_MS } = {}) {
  const normalizedPid = Number(pid || 0);
  if (!normalizedPid) {
    return false;
  }
  try {
    if (platform === "win32") {
      spawnSync("taskkill", ["/PID", String(normalizedPid), "/T", "/F"], {
        stdio: "ignore",
        timeout: timeoutMs,
      });
    } else {
      process.kill(normalizedPid, "SIGTERM");
    }
  } catch {
    return false;
  }
  return true;
}

async function waitForPidExit(pid, timeoutMs = BRIDGE_EXIT_WAIT_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!pidIsRunning(pid)) {
      return true;
    }
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  return !pidIsRunning(pid);
}

export async function cleanupSmokeBridge({
  metaPath = bridgeMetaPath,
  logPath = bridgeLogPath,
  processHandle = null,
  quiet = false,
  pidIsRunningImpl = pidIsRunning,
  stopBridgePidImpl = stopBridgePid,
  waitForPidExitImpl = waitForPidExit,
} = {}) {
  const meta = readBridgeMeta({ metaPath });
  const pid = Number(meta?.bridgePid || processHandle?.pid || 0);
  if (processHandle && typeof processHandle.kill === "function") {
    try {
      processHandle.kill();
    } catch {
      // Fall back to pid-based cleanup below.
    }
  }
  if (pid > 0 && pidIsRunningImpl(pid)) {
    stopBridgePidImpl(pid);
    await waitForPidExitImpl(pid);
  }
  safeUnlink(metaPath);
  if (!quiet && existsSync(logPath) && smokeBridgeDebugEnabled()) {
    console.log(`[bridge] log tail:\n${readBoundedLogTail(logPath)}`);
  }
  return { pid, cleaned: true };
}

export async function startSmokeBridge({
  dataDir = bridgeDataDir,
  port,
  pythonCommand = resolvePlaywrightPythonCommand(),
  metaPath = bridgeMetaPath,
  logPath = bridgeLogPath,
  timeoutMs = BRIDGE_READY_TIMEOUT_MS,
  spawnImpl = spawn,
  waitForBridgeReadyImpl = waitForBridgeReady,
  cleanupImpl = cleanupSmokeBridge,
} = {}) {
  if (!port) {
    throw new Error("Missing bridge port for Playwright smoke runtime.");
  }
  safeUnlink(logPath);
  const args = [
    "src/admin_bridge.py",
    "--port", String(port),
    "--host", "127.0.0.1",
    "--data-dir", dataDir,
  ];
  const bridgeProcess = spawnImpl(pythonCommand, args, {
    stdio: ["ignore", "pipe", "pipe"],
    detached: false,
    windowsHide: true,
  });

  const meta = writeBridgeMeta(
    {
      bridgeHost: "127.0.0.1",
      bridgePort: Number(port),
      bridgePid: Number(bridgeProcess.pid || 0),
      bridgeDataDir: String(dataDir),
      bridgeLogPath: String(logPath),
      startedAt: new Date().toISOString(),
    },
    { metaPath }
  );

  const echoLogs = smokeBridgeDebugEnabled();
  const logChunk = (label, data) => {
    const text = String(data || "");
    if (!text) {
      return;
    }
    appendBoundedLog(logPath, `[${label}] ${text}`);
    if (echoLogs) {
      console.log(`[bridge:${label}] ${text}`);
    }
  };
  bridgeProcess.stdout?.on("data", chunk => logChunk("stdout", chunk));
  bridgeProcess.stderr?.on("data", chunk => logChunk("stderr", chunk));

  let settled = false;
  try {
    await Promise.race([
      waitForBridgeReadyImpl(port, timeoutMs),
      new Promise((_, reject) => {
        bridgeProcess.once("error", error => {
          reject(error);
        });
        bridgeProcess.once("exit", (code, signal) => {
          reject(
            new Error(
              `Admin bridge exited before readiness check completed (code=${code}, signal=${signal ?? "none"}).`
            )
          );
        });
      }),
    ]);
    settled = true;
    return { bridgeProcess, meta };
  } catch (error) {
    const tail = readBoundedLogTail(logPath);
    await cleanupImpl({ metaPath, logPath, processHandle: bridgeProcess, quiet: true });
    const suffix = tail ? `\n\nBridge log tail:\n${tail}` : "";
    throw new Error(`${error instanceof Error ? error.message : String(error)}${suffix}`);
  } finally {
    if (!settled && smokeBridgeDebugEnabled()) {
      console.log("[bridge] smoke bridge failed before readiness.");
    }
  }
}
