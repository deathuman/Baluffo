/**
 * Lazy seam for the Jobs pipeline surface.
 *
 * The update button's status watcher and run trigger live in the pipeline
 * controller, which is the largest module in the page (its own file plus the
 * bridge request helpers it pulls in). Nothing on the first-rows path needs it:
 * the feed renders rows from the cached snapshot, and the status watch is
 * scheduled as non-critical startup work after that render.
 *
 * `composition.js` builds this facade synchronously and passes it to the page
 * flow/events exactly where the real controller used to go, so call sites keep
 * their contract: `ensureJobsPipelineStatusWatch()` still returns immediately
 * and starts polling, `triggerJobsPipelineRun()` still returns a promise. Both
 * await the chunk, which means a click that lands while the chunk is in flight
 * runs the pipeline instead of being dropped.
 *
 * Nothing loads until a call arrives, so a page that never asks for the
 * pipeline never fetches it. A failed chunk must not break the Update button:
 * the facade degrades to an inert controller that swallows calls and logs once.
 */

export function createLazyJobsPipelineController({ createController }) {
  let controller = null;
  let loading = null;

  function ensure() {
    if (loading) return loading;
    if (controller) return Promise.resolve(controller);
    loading = Promise.resolve()
      .then(createController)
      .catch(error => {
        console.error("[jobs] Could not load the pipeline surface:", error);
        return null;
      })
      .then(impl => {
        controller = impl || {};
        return controller;
      });
    return loading;
  }

  function delegate(name) {
    return (...args) => ensure().then(impl => impl[name]?.(...args));
  }

  return {
    ensureJobsPipelineStatusWatch: delegate("ensureJobsPipelineStatusWatch"),
    triggerJobsPipelineRun: delegate("triggerJobsPipelineRun"),
    // Test and diagnostic seams.
    ready: () => ensure(),
    isLoaded: () => Boolean(controller)
  };
}
