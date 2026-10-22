import { hydrateAppVersionLabels } from "../app-version.js";
import {
  emitStartupProbeMetric,
  resolveStartupProbePage
} from "../../../probes/startup-probe.js";

// The bridge boot sequence, once: initialize the local-data API synchronously so
// page code can rely on `window.JobAppLocalData` at parse time, then report the
// bootstrap through the startup probe. The compositions differ only in which
// client they initialize (hosted vs desktop), so they pass it in.
export function bootBridgeLocalData({ page = resolveStartupProbePage(), localData }) {
  emitStartupProbeMetric(`${page}_page_boot_start`);
  emitStartupProbeMetric(`${page}_local_data_init_start`);
  const bootstrapWaitStartedAt = performance.now();
  let localDataApi;
  try {
    localDataApi = localData.init();
    emitStartupProbeMetric(`${page}_local_data_api_ready`);
  } catch (err) {
    console.error("[baluffo] Bridge local data init failed:", err);
    window.__baluffoInitErrors.push(err);
    return null;
  }
  window.__baluffoLocalDataLoaded = false;
  localData.awaitBootstrap().then(ready => {
    if (!ready) {
      return;
    }
    window.__baluffoLocalDataLoaded = true;
    console.log("[baluffo] Bridge local data initialized successfully");
    hydrateAppVersionLabels().catch(() => {});
    const bootstrapStats = localData.getStats();
    emitStartupProbeMetric(`${page}_local_data_init_ready`, {
      bootstrapWaitMs: Math.max(0, Math.round(performance.now() - bootstrapWaitStartedAt)),
      bootstrapAttemptCount: Math.max(0, Number(bootstrapStats?.attemptCount || 0)),
      bootstrapRetryCount: Math.max(0, Number(bootstrapStats?.failureCount || 0)),
      firstSuccessfulBootstrapAttemptMs: bootstrapStats?.firstSuccessfulAttemptMs ?? null
    });
  }).catch(err => {
    console.error("[baluffo] Bridge local data init failed:", err);
    window.__baluffoInitErrors.push(err);
  });
  return localDataApi;
}
