import "../../app-local-data-client.js";
import "../../jobs-state.js";
import "../../jobs-parsing-utils.js";
import { boot as bootJobsPage } from "./app.js";

export function boot() {
  bootJobsPage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
