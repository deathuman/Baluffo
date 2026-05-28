import "../shared/local-data/app-client.js";
import "../shared/config/admin-config.js";
import { boot as bootAdminPage } from "./app.js?v=16";
import { installGlobalTooltipController } from "../shared/ui/tooltip-controller.js";
import { installExplainStateHandler } from "../shared/ui/explain-state.js";
import { observeLongTasks } from "../../probes/long-task-observer.js";
import { emitStartupProbeMetric, resolveStartupProbeEnabled } from "../../probes/startup-probe.js";

emitStartupProbeMetric("admin_page_boot_start");
emitStartupProbeMetric("admin_module_boot_start");
if (resolveStartupProbeEnabled()) {
  observeLongTasks({ page: "admin", emitMetric: emitStartupProbeMetric });
}
installGlobalTooltipController();
installExplainStateHandler();

export function boot() {
  bootAdminPage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
