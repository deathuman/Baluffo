import "../../app-local-data-client.js";
import "../../saved-zip-utils.js";
import { boot as bootSavedPage } from "./app.js";
import { emitStartupProbeMetric } from "../../startup-probe.js";

emitStartupProbeMetric("saved_page_boot_start");
emitStartupProbeMetric("saved_module_boot_start");

export function boot() {
  bootSavedPage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
