(function bootJobsPage(globalScope) {
  function start() {
    if (globalScope.JobsApp && typeof globalScope.JobsApp.boot === "function") {
      globalScope.JobsApp.boot();
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
    return;
  }
  start();
})(window);
