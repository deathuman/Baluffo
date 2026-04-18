import { cleanupSmokeBridge } from "./smoke-bridge-runtime.js";

export default async function globalTeardown() {
  await cleanupSmokeBridge({ quiet: true });
}
