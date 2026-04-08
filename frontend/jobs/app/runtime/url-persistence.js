export function createJobsUrlPersistence({
  windowObject,
  buildJobsPageUrl,
  resolveStartupProbeEnabled,
  isDesktopRuntimeMode,
  rememberJobsUrl,
  emitMetric,
  getDesktopUrlStateReady,
  setDesktopUrlStateReady,
  getDesktopPendingRememberJobsUrl,
  setDesktopPendingRememberJobsUrl,
  getDesktopPendingJobsUrl,
  setDesktopPendingJobsUrl,
  lastUrlKey
}) {
  function persistDesktopJobsUrlState(url) {
    try {
      emitMetric("jobs_write_state_remember_url_start");
      rememberJobsUrl(lastUrlKey, String(url || ""));
      emitMetric("jobs_write_state_remember_url_complete");
    } catch {
      emitMetric("jobs_write_state_remember_url_failed");
    }
  }

  function rememberCurrentJobsUrl() {
    const url = `${windowObject.location.pathname}${windowObject.location.search}`;
    if (isDesktopRuntimeMode()) {
      if (!getDesktopUrlStateReady()) {
        setDesktopPendingRememberJobsUrl(true);
        setDesktopPendingJobsUrl(url);
        return;
      }
      windowObject.setTimeout(() => {
        persistDesktopJobsUrlState(url);
      }, 0);
      return;
    }
    rememberJobsUrl(lastUrlKey, url);
  }

  function writeStateToUrl(state) {
    emitMetric("jobs_write_state_params_start");
    const url = buildJobsPageUrl(windowObject.location.pathname, state);
    emitMetric("jobs_write_state_params_complete");
    if (resolveStartupProbeEnabled()) {
      emitMetric("jobs_write_state_probe_skip", { url });
      return;
    }
    if (isDesktopRuntimeMode()) {
      if (!getDesktopUrlStateReady()) {
        setDesktopPendingRememberJobsUrl(true);
        setDesktopPendingJobsUrl(url);
        emitMetric("jobs_write_state_desktop_deferred", { url });
        return;
      }
      emitMetric("jobs_write_state_desktop_flush", { url });
      windowObject.setTimeout(() => {
        persistDesktopJobsUrlState(url);
      }, 0);
      return;
    }
    emitMetric("jobs_write_state_replace_state_start", { url });
    windowObject.history.replaceState({}, "", url);
    emitMetric("jobs_write_state_replace_state_complete");
    emitMetric("jobs_write_state_remember_url_start");
    rememberCurrentJobsUrl();
    emitMetric("jobs_write_state_remember_url_complete");
  }

  return {
    persistDesktopJobsUrlState,
    rememberCurrentJobsUrl,
    writeStateToUrl
  };
}
