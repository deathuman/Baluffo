import { setTooltip } from "../../../shared/ui/index.js";

export const FETCHER_FALLBACK_MESSAGES = {
  bridgeUnavailable: "Bridge is offline; using VS Code task fallback for this run.",
  presetNeedsBridge: "VS Code task fallback supports default fetcher runs only. Start admin bridge and retry.",
  launchPrimary: taskLabel => `Triggered VS Code task URI (primary): ${taskLabel}`,
  launchSecondary: "Triggered VS Code task URI fallback (quoted task label).",
  manualHint: "If VS Code did not open, run the manual command fallback shown below.",
  copiedManualCommand: command => `Copied manual command fallback: ${command}`,
  manualCommand: command => `Manual command fallback: ${command}`
};

export const FETCHER_PRESET_META = {
  default: {
    preset: "default",
    buttonKey: "default",
    busyLabel: "Fetcher Running...",
    title: "Run the standard fetcher flow with current defaults (parallel workers, domain limits, circuit breaker).",
    ariaLabel: "Run jobs fetcher with default options"
  },
  incremental: {
    preset: "incremental",
    buttonKey: "incremental",
    busyLabel: "Incremental Running...",
    title: "Run incremental mode: skip recently successful sources based on TTL and reuse existing output.",
    ariaLabel: "Run incremental fetcher"
  },
  uncapped: {
    preset: "uncapped",
    buttonKey: "uncapped",
    busyLabel: "Uncapped Running...",
    title: "Run the fetcher aggressively: bypass freshness skips, circuit-breaker quarantine, and admin-imposed fetch caps.",
    ariaLabel: "Run fetcher uncapped"
  },
  force_full: {
    preset: "force_full",
    buttonKey: "force",
    busyLabel: "Force Running...",
    title: "Run full fetch while ignoring circuit breaker quarantine for temporarily blocked sources.",
    ariaLabel: "Run fetcher ignoring circuit breaker"
  },
  retry_failed: {
    preset: "retry_failed",
    buttonKey: "retry",
    busyLabel: "Retry Running...",
    title: "Run fetcher only for sources that failed in the latest report, bypassing circuit breaker.",
    ariaLabel: "Retry failed sources only",
    requestedLog: "Retry failed sources requested."
  }
};

export function getFetcherPresetMeta(preset) {
  const key = String(preset || "default").trim().toLowerCase();
  return FETCHER_PRESET_META[key] || FETCHER_PRESET_META.default;
}

function getFetcherPresetButtons(refs) {
  return [
    { preset: "default", el: refs.adminRunFetcherBtnEl },
    { preset: "incremental", el: refs.adminRunFetcherIncrementalBtnEl },
    { preset: "uncapped", el: refs.adminRunFetcherUncappedBtnEl },
    { preset: "force_full", el: refs.adminRunFetcherForceBtnEl },
    { preset: "retry_failed", el: refs.adminRetryFailedBtnEl }
  ];
}

export function applyFetcherPresetMetadata(refs) {
  getFetcherPresetButtons(refs).forEach(item => {
    const btn = item?.el;
    if (!btn) return;
    const meta = getFetcherPresetMeta(item.preset);
    btn.dataset.fetcherPreset = meta.preset;
    setTooltip(btn, meta.title || "");
    if (meta.ariaLabel) btn.setAttribute("aria-label", meta.ariaLabel);
  });
}
