(function bootAdminPage(globalScope) {
  function start() {
    if (globalScope.AdminApp && typeof globalScope.AdminApp.boot === "function") {
      globalScope.AdminApp.boot();
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
    return;
  }
  start();
})(window);
