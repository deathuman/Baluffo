const RUNTIME_MODE_KEY = "baluffo_runtime_mode";

function resolveDesktopMode() {
  try {
    const url = new URL(window.location.href);
    const explicitDesktop = url.searchParams.get("desktop");
    if (explicitDesktop === "1") {
      window.sessionStorage.setItem(RUNTIME_MODE_KEY, "desktop");
      return true;
    }
    return window.sessionStorage.getItem(RUNTIME_MODE_KEY) === "desktop";
  } catch {
    return false;
  }
}

if (resolveDesktopMode()) {
  import("./desktop-local-data-client.js").catch(error => {
    console.error("[app-local-data-client] failed to load desktop adapter:", error);
  });
} else {
  import("./local-data-client.js").catch(error => {
    console.error("[app-local-data-client] failed to load browser adapter:", error);
  });
}
