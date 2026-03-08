import { boot as bootHomePage } from "./app.js";

export function boot() {
  bootHomePage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
