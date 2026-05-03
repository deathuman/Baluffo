import "../shared/local-data/app-client.js";
import "./state.js?v=4";
import "./parsing-utils.js";
import { boot as bootJobsPage } from "./app.js?v=4";
import { emitStartupProbeMetric } from "../../probes/startup-probe.js";

emitStartupProbeMetric("jobs_page_boot_start");
emitStartupProbeMetric("jobs_module_boot_start");

export function boot() {
  bootJobsPage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
