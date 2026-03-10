import "../../app-local-data-client.js";
import "../../jobs-state.js";
import "../../jobs-parsing-utils.js";
import { boot as bootJobsPage } from "./app.js";
import { emitStartupProbeMetric } from "../../startup-probe.js";

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
