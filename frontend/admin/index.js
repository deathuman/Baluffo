import "../../app-local-data-client.js";
import "../../admin-config.js";
import { boot as bootAdminPage } from "./app.js";
import { emitStartupProbeMetric } from "../../startup-probe.js";

emitStartupProbeMetric("admin_page_boot_start");
emitStartupProbeMetric("admin_module_boot_start");

export function boot() {
  bootAdminPage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
