import "../../local-data-client.js";
import "../../saved-zip-utils.js";
import { boot as bootSavedPage } from "./app.js";

export function boot() {
  bootSavedPage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
