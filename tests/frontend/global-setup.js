import {
  bridgeDataDir,
  bridgeMetaPath,
  cleanupSmokeBridge,
  prepareBridgeDataDir,
  reserveBridgePort,
  startSmokeBridge,
  writeBridgeMeta,
} from "./smoke-bridge-runtime.js";

export default async function globalSetup() {
  console.log("[setup] Preparing Playwright smoke bridge...");
  await cleanupSmokeBridge({ quiet: true });
  const dataDir = prepareBridgeDataDir();
  const port = await reserveBridgePort();
  writeBridgeMeta(
    {
      bridgeHost: "127.0.0.1",
      bridgePort: Number(port),
      bridgePid: 0,
      bridgeDataDir: String(bridgeDataDir),
      bridgeLogPath: "",
      startedAt: new Date().toISOString(),
    },
    { metaPath: bridgeMetaPath }
  );
  console.log(`[setup] Using isolated bridge data dir: ${dataDir}`);
  console.log(`[setup] Using isolated bridge port: ${port}`);
  await startSmokeBridge({ dataDir, port });
  console.log("[setup] Admin bridge started");
}
