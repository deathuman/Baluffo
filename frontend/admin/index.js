import "../shared/local-data/app-client.js";
import "../shared/config/admin-config.js";
import { boot as bootAdminPage } from "./app.js?v=8";
import { emitStartupProbeMetric } from "../../probes/startup-probe.js";

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
