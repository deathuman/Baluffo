window.__baluffoSavedIndexLoaded = true;
console.log("[baluffo-saved] index.js: module started");

import "../shared/local-data/app-client.js";
import "./zip-utils.js";
import { boot as bootSavedPage } from "./app.js";
import { observeLongTasks } from "../../probes/long-task-observer.js";
import { emitStartupProbeMetric, resolveStartupProbeEnabled } from "../../probes/startup-probe.js";

emitStartupProbeMetric("saved_page_boot_start");
emitStartupProbeMetric("saved_module_boot_start");
if (resolveStartupProbeEnabled()) {
  observeLongTasks({ page: "saved", emitMetric: emitStartupProbeMetric });
}

export function boot() {
  bootSavedPage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
