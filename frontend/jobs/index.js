import "../shared/local-data/app-client.js";
import "./state.js?v=4";
import "./parsing-utils.js";
import { boot as bootJobsPage } from "./app.js?v=5";
import { installGlobalTooltipController } from "../shared/ui/tooltip-controller.js";
import { observeLongTasks } from "../../probes/long-task-observer.js";
import { emitStartupProbeMetric, resolveStartupProbeEnabled } from "../../probes/startup-probe.js";

emitStartupProbeMetric("jobs_page_boot_start");
emitStartupProbeMetric("jobs_module_boot_start");
if (resolveStartupProbeEnabled()) {
  observeLongTasks({ page: "jobs", emitMetric: emitStartupProbeMetric });
}
installGlobalTooltipController();

export function boot() {
  bootJobsPage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
