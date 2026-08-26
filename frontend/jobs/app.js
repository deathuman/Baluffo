import "./app/runtime.js";

export function boot() {
  globalThis.__baluffoBootJobsPage?.();
}
