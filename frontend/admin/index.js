import "../../app-local-data-client.js";
import "../../admin-config.js";
import { boot as bootAdminPage } from "./app.js";

export function boot() {
  bootAdminPage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
