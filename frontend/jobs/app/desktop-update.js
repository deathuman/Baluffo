function toggleHidden(element, hidden) {
  if (!element?.classList?.toggle) return;
  element.classList.toggle("hidden", Boolean(hidden));
}

function setText(element, value) {
  if (element) {
    element.textContent = String(value || "");
  }
}

function setDisabled(element, disabled) {
  if (!element) return;
  element.disabled = Boolean(disabled);
  element.setAttribute?.("aria-disabled", disabled ? "true" : "false");
}

function errorMessage(error) {
  if (!error) return "Unknown error";
  if (error instanceof Error && error.message) return error.message;
  return String(error);
}

function normalizePercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(100, Math.round(numeric)));
}

export function formatDesktopUpdateBytes(bytes) {
  const value = Number(bytes) || 0;
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

export function normalizeDesktopUpdateStatus(status = {}) {
  const payload = status && typeof status === "object" ? status : {};
  return {
    currentVersion: String(payload.currentVersion || ""),
    latestVersion: String(payload.latestVersion || payload.targetVersion || ""),
    targetVersion: String(payload.targetVersion || payload.latestVersion || ""),
    availability: String(payload.availability || "unknown").toLowerCase(),
    updateAvailable: Boolean(payload.updateAvailable),
    downloadState: String(payload.downloadState || "idle").toLowerCase(),
    downloadedBytes: Math.max(0, Number(payload.downloadedBytes) || 0),
    totalBytes: Math.max(0, Number(payload.totalBytes) || 0),
    downloadPercent: normalizePercent(payload.downloadPercent),
    installState: String(payload.installState || "idle").toLowerCase(),
    installStage: String(payload.installStage || "").toLowerCase(),
    installStageLabel: String(payload.installStageLabel || ""),
    releaseNotesUrl: String(payload.releaseNotesUrl || ""),
    releaseNotesTitle: String(payload.releaseNotesTitle || ""),
    releaseNotesBody: String(payload.releaseNotesBody || ""),
    releaseNotesPublishedAt: String(payload.releaseNotesPublishedAt || ""),
    lastCheckedAt: String(payload.lastCheckedAt || ""),
    lastError: String(payload.lastError || ""),
    blockedReason: String(payload.blockedReason || ""),
  };
}

export function shouldPollDesktopUpdateStatus(status = {}) {
  const normalized = normalizeDesktopUpdateStatus(status);
  if (normalized.availability === "checking") return true;
  if (normalized.downloadState === "downloading") return true;
  return new Set(["handoff_requested", "waiting_for_exit", "installing", "verifying"]).has(
    normalized.installState
  );
}

function blockedReasonMessage(blockedReason) {
  if (blockedReason === "helper_too_old") {
    return "This release requires a newer updater helper than the one packaged with your current build.";
  }
  if (blockedReason === "current_version_too_old") {
    return "This release cannot be installed directly from your current version. Upgrade to a newer portable build first.";
  }
  return "This update is available but cannot be installed automatically from the current build.";
}

function downloadResultMessage(errorCode, fallbackMessage) {
  const code = String(errorCode || "").trim().toLowerCase();
  if (code === "download_in_progress") {
    return { message: "Desktop update download is already in progress.", level: "info" };
  }
  if (code === "update_ready_to_install") {
    return { message: "The update ZIP is already downloaded. Install it when you're ready.", level: "info" };
  }
  if (code === "helper_too_old" || code === "current_version_too_old") {
    return { message: blockedReasonMessage(code), level: "error" };
  }
  if (code === "manifest_cache_missing") {
    return { message: "Update metadata is missing. Check for updates again.", level: "error" };
  }
  if (code === "no_update_available") {
    return { message: fallbackMessage || "No desktop update is available right now.", level: "error" };
  }
  return {
    message: fallbackMessage || "Could not start the desktop update download.",
    level: "error"
  };
}

function installResultMessage(errorCode, fallbackMessage) {
  const code = String(errorCode || "").trim().toLowerCase();
  if (code === "manifest_cache_missing") {
    return { message: "Update metadata is missing. Check for updates again.", level: "error" };
  }
  if (code === "install_not_ready") {
    return { message: "Download the update before trying to install it.", level: "error" };
  }
  if (code === "install_session_unavailable") {
    return { message: "The desktop launcher session is unavailable. Restart Baluffo and try again.", level: "error" };
  }
  return {
    message: fallbackMessage || "Could not start the desktop update install.",
    level: "error"
  };
}

function buildMetaLine(status) {
  const current = String(status.currentVersion || "").trim();
  const latest = String(status.latestVersion || status.targetVersion || "").trim();
  const totalBytes = Number(status.totalBytes || 0);
  const parts = [];
  if (current) parts.push(`Current ${current}`);
  if (latest && latest !== current) parts.push(`Latest ${latest}`);
  if (totalBytes > 0) parts.push(formatDesktopUpdateBytes(totalBytes));
  return parts.join(" | ");
}

function buildReleaseNotesTitle(status) {
  return String(
    status.releaseNotesTitle || status.targetVersion || status.latestVersion || "Release notes"
  ).trim();
}

function inFlightInstallState(installState) {
  return new Set(["handoff_requested", "waiting_for_exit", "installing", "verifying"]).has(
    String(installState || "").toLowerCase()
  );
}

export function shouldExposeJobsDesktopUpdateStatus(status = {}, { hasFreshStatus = false } = {}) {
  if (hasFreshStatus) return true;
  const normalized = normalizeDesktopUpdateStatus(status);
  if (normalized.downloadState === "downloading") return true;
  if (normalized.downloadState === "downloaded") return true;
  if (normalized.installState === "ready") return true;
  return inFlightInstallState(normalized.installState);
}

function failureToastForStatus(previousStatus, nextStatus) {
  const previous = normalizeDesktopUpdateStatus(previousStatus);
  const next = normalizeDesktopUpdateStatus(nextStatus);
  if (previous.downloadState !== "failed" && next.downloadState === "failed") {
    return next.lastError || "Baluffo could not finish downloading the portable update ZIP.";
  }
  if (previous.installState !== "failed" && next.installState === "failed") {
    return next.lastError || "Baluffo could not install the downloaded update.";
  }
  return "";
}

export function deriveDesktopUpdateView(status, { panelOpen = false } = {}) {
  const normalized = normalizeDesktopUpdateStatus(status);
  const installProgress = normalized.installStageLabel || "Waiting for the updater helper to finish install and startup verification.";
  const stateToken = normalized.installState === "failed"
    ? "install_failed"
    : normalized.downloadState === "failed"
      ? "download_failed"
      : inFlightInstallState(normalized.installState)
        ? normalized.installState
        : normalized.downloadState === "downloading"
          ? "downloading"
          : normalized.availability;
  const view = {
    stateToken: stateToken || "unknown",
    buttonLabel: "Check updates",
    panelVisible: Boolean(panelOpen || shouldPollDesktopUpdateStatus(normalized)),
    title: "Desktop updates",
    body: "Check GitHub Releases for a newer Baluffo portable build.",
    meta: buildMetaLine(normalized),
    progress: "",
    releaseNotesUrl: normalized.releaseNotesUrl,
    releaseNotesTitle: buildReleaseNotesTitle(normalized),
    releaseNotesBody: normalized.releaseNotesBody,
    releaseNotesPublishedAt: normalized.releaseNotesPublishedAt,
    releaseNotesVisible: Boolean(normalized.releaseNotesBody || normalized.releaseNotesUrl),
    primaryAction: "check",
    primaryLabel: "Check for updates",
    primaryDisabled: false,
    primaryVisible: true,
    secondaryAction: "close",
    secondaryLabel: "Close",
    secondaryVisible: false,
  };

  if (normalized.availability === "up_to_date") {
    view.buttonLabel = "Up to date";
    view.title = "Baluffo is up to date";
    view.body = normalized.currentVersion
      ? `Version ${normalized.currentVersion} is the newest desktop release available right now.`
      : "This desktop build is already on the newest release.";
    view.primaryLabel = "Check again";
    view.secondaryVisible = Boolean(panelOpen);
    return view;
  }

  if (normalized.availability === "checking") {
    view.buttonLabel = "Checking...";
    view.title = "Checking for updates";
    view.body = "Looking up the latest stable desktop release from GitHub Releases.";
    view.primaryVisible = false;
    view.secondaryVisible = Boolean(panelOpen);
    return view;
  }

  if (normalized.downloadState === "downloading") {
    view.buttonLabel = normalized.downloadPercent > 0
      ? `Downloading ${normalized.downloadPercent}%`
      : "Downloading update";
    view.title = normalized.targetVersion
      ? `Downloading ${normalized.targetVersion}`
      : "Downloading desktop update";
    view.body = "Baluffo can stay open while the portable ZIP downloads in the background.";
    view.progress = normalized.totalBytes > 0
      ? `${formatDesktopUpdateBytes(normalized.downloadedBytes)} of ${formatDesktopUpdateBytes(normalized.totalBytes)} (${normalized.downloadPercent}%)`
      : normalized.downloadedBytes > 0
        ? `${formatDesktopUpdateBytes(normalized.downloadedBytes)} downloaded`
        : "Download started.";
    view.primaryVisible = false;
    view.secondaryVisible = false;
    return view;
  }

  if (normalized.downloadState === "failed") {
    view.buttonLabel = "Download failed";
    view.title = normalized.targetVersion
      ? `Could not download ${normalized.targetVersion}`
      : "Desktop update download failed";
    view.body = normalized.lastError || "Baluffo could not finish downloading the portable update ZIP.";
    view.progress = normalized.downloadedBytes > 0
      ? normalized.totalBytes > 0
        ? `${formatDesktopUpdateBytes(normalized.downloadedBytes)} of ${formatDesktopUpdateBytes(normalized.totalBytes)} downloaded before the failure`
        : `${formatDesktopUpdateBytes(normalized.downloadedBytes)} downloaded before the failure`
      : "";
    view.primaryAction = "download";
    view.primaryLabel = "Try download again";
    view.secondaryAction = "later";
    view.secondaryLabel = "Later";
    view.secondaryVisible = true;
    return view;
  }

  if (inFlightInstallState(normalized.installState)) {
    view.buttonLabel = "Installing...";
    view.title = "Installing desktop update";
    view.body = "Baluffo is handing off to the updater helper and will reopen automatically.";
    view.progress = installProgress;
    view.primaryVisible = false;
    view.secondaryVisible = false;
    return view;
  }

  if (normalized.installState === "failed") {
    view.buttonLabel = "Update failed";
    view.title = "Desktop update failed";
    view.body = normalized.lastError || "Baluffo restored the previous runtime after the update failed.";
    view.progress = normalized.installStageLabel || "";
    view.primaryAction = normalized.downloadState === "downloaded" ? "install" : "check";
    view.primaryLabel = normalized.downloadState === "downloaded" ? "Try install again" : "Check again";
    view.primaryVisible = false;
    view.primaryVisible = true;
    view.secondaryAction = "close";
    view.secondaryLabel = "Close";
    view.secondaryVisible = true;
    return view;
  }

  if (normalized.downloadState === "downloaded" || normalized.installState === "ready") {
    view.buttonLabel = normalized.targetVersion
      ? `Install ${normalized.targetVersion}`
      : "Install update";
    view.title = normalized.targetVersion
      ? `Ready to install ${normalized.targetVersion}`
      : "Ready to install";
    view.body = "Baluffo will close, install the update, preserve ship\\data, and reopen automatically.";
    view.primaryAction = "install";
    view.primaryLabel = "Install and restart";
    view.secondaryAction = "later";
    view.secondaryLabel = "Later";
    view.secondaryVisible = true;
    return view;
  }

  if (normalized.availability === "blocked") {
    view.buttonLabel = "Update blocked";
    view.title = normalized.targetVersion
      ? `${normalized.targetVersion} is available`
      : "Update available";
    view.body = blockedReasonMessage(normalized.blockedReason);
    view.primaryAction = "check";
    view.primaryLabel = "Check again";
    view.secondaryAction = "later";
    view.secondaryLabel = "Close";
    view.secondaryVisible = true;
    return view;
  }

  if (normalized.availability === "available" || normalized.updateAvailable) {
    view.buttonLabel = normalized.targetVersion
      ? `Update ${normalized.targetVersion}`
      : "Update available";
    view.title = normalized.targetVersion
      ? `${normalized.targetVersion} is available`
      : "Update available";
    view.body = "A newer portable desktop build is ready to download.";
    view.primaryAction = "download";
    view.primaryLabel = "Download";
    view.secondaryAction = "later";
    view.secondaryLabel = "Later";
    view.secondaryVisible = true;
    return view;
  }

  if (normalized.availability === "error") {
    view.buttonLabel = "Update error";
    view.title = "Could not check for updates";
    view.body = normalized.lastError || "Baluffo could not reach the release feed right now.";
    view.primaryAction = "check";
    view.primaryLabel = "Try again";
    view.secondaryVisible = Boolean(panelOpen);
    return view;
  }

  view.secondaryVisible = Boolean(panelOpen);
  return view;
}

export function createJobsDesktopUpdateController({
  refs,
  baseUrl,
  fetchJson,
  postJson,
  bindAsyncClick,
  showToast,
  requestConfirmationDialog,
  isDesktopRuntimeMode,
  showReleaseNotesDialog,
  openExternalUrl,
  setTimeoutFn = setTimeout,
  clearTimeoutFn = clearTimeout,
}) {
  const state = {
    bound: false,
    mounted: false,
    autoCheckStarted: false,
    hasFreshStatus: false,
    panelOpen: false,
    pendingAction: "",
    pollTimer: null,
    status: normalizeDesktopUpdateStatus(),
    dismissedTargetVersion: "",
  };

  function stopPolling() {
    if (!state.pollTimer) return;
    clearTimeoutFn(state.pollTimer);
    state.pollTimer = null;
  }

  function schedulePoll(delayMs = 1400) {
    stopPolling();
    state.pollTimer = setTimeoutFn(() => {
      refreshStatus({ silent: true }).catch(() => {});
    }, Math.max(400, Number(delayMs) || 1400));
    state.pollTimer?.unref?.();
  }

  function maybeOpenPanelForStatus(nextStatus) {
    const targetVersion = String(nextStatus.targetVersion || nextStatus.latestVersion || "").trim();
    if (!targetVersion || targetVersion !== state.dismissedTargetVersion) {
      if (!nextStatus.updateAvailable) {
        state.dismissedTargetVersion = "";
      }
      return true;
    }
    return false;
  }

  function syncPolling(nextStatus) {
    if (shouldPollDesktopUpdateStatus(nextStatus)) {
      schedulePoll();
      return;
    }
    stopPolling();
  }

  function render() {
    const desktopMode = Boolean(isDesktopRuntimeMode?.());
    const shouldExposeStatus = shouldExposeJobsDesktopUpdateStatus(state.status, {
      hasFreshStatus: state.hasFreshStatus
    });
    toggleHidden(refs.desktopUpdateToggleBtn, !desktopMode || !shouldExposeStatus);
    if (!desktopMode || !shouldExposeStatus) {
      toggleHidden(refs.desktopUpdatePanel, true);
      return;
    }
    const view = deriveDesktopUpdateView(state.status, { panelOpen: state.panelOpen });
    if (refs.desktopUpdateToggleBtn) {
      refs.desktopUpdateToggleBtn.dataset.updateState = view.stateToken;
      refs.desktopUpdateToggleBtn.textContent = view.buttonLabel;
      refs.desktopUpdateToggleBtn.setAttribute("aria-expanded", view.panelVisible ? "true" : "false");
      refs.desktopUpdateToggleBtn.title = view.buttonLabel;
    }
    if (refs.desktopUpdatePanel) {
      refs.desktopUpdatePanel.dataset.updateState = view.stateToken;
    }
    toggleHidden(refs.desktopUpdatePanel, !view.panelVisible);
    setText(refs.desktopUpdateTitle, view.title);
    setText(refs.desktopUpdateBody, view.body);
    setText(refs.desktopUpdateMeta, view.meta);
    setText(refs.desktopUpdateProgress, view.progress);
    toggleHidden(refs.desktopUpdateMeta, !view.meta);
    toggleHidden(refs.desktopUpdateProgress, !view.progress);
    if (refs.desktopUpdateReleaseNotes) {
      refs.desktopUpdateReleaseNotes.href = view.releaseNotesUrl || "#";
      toggleHidden(refs.desktopUpdateReleaseNotes, !view.releaseNotesVisible);
    }
    if (refs.desktopUpdatePrimaryBtn) {
      refs.desktopUpdatePrimaryBtn.dataset.action = view.primaryAction;
      refs.desktopUpdatePrimaryBtn.textContent = view.primaryLabel;
      setDisabled(refs.desktopUpdatePrimaryBtn, view.primaryDisabled || Boolean(state.pendingAction));
      toggleHidden(refs.desktopUpdatePrimaryBtn, !view.primaryVisible);
    }
    if (refs.desktopUpdateSecondaryBtn) {
      refs.desktopUpdateSecondaryBtn.dataset.action = view.secondaryAction;
      refs.desktopUpdateSecondaryBtn.textContent = view.secondaryLabel;
      setDisabled(refs.desktopUpdateSecondaryBtn, false);
      toggleHidden(refs.desktopUpdateSecondaryBtn, !view.secondaryVisible);
    }
  }

  function applyStatus(status, {
    openPanel = false,
    autoOpenImportant = false,
    isFresh = false
  } = {}) {
    const previousStatus = state.status;
    const nextStatus = normalizeDesktopUpdateStatus(status);
    const targetVersion = String(nextStatus.targetVersion || nextStatus.latestVersion || "").trim();
    if (isFresh) {
      state.hasFreshStatus = true;
    }
    if (!nextStatus.updateAvailable || nextStatus.availability === "up_to_date") {
      state.dismissedTargetVersion = "";
    }
    if (openPanel) {
      state.panelOpen = true;
    } else if (autoOpenImportant && maybeOpenPanelForStatus(nextStatus)) {
      const important = nextStatus.updateAvailable || shouldPollDesktopUpdateStatus(nextStatus);
      if (important) {
        state.panelOpen = true;
      }
    } else if (!targetVersion || targetVersion !== state.dismissedTargetVersion) {
      // Keep later/dismiss scoped to the same target version only.
      if (state.dismissedTargetVersion && targetVersion !== state.dismissedTargetVersion) {
        state.dismissedTargetVersion = "";
      }
    }
    state.status = nextStatus;
    const failureToast = failureToastForStatus(previousStatus, nextStatus);
    if (failureToast) {
      showToast?.(failureToast, "error");
    }
    syncPolling(nextStatus);
    render();
  }

  async function refreshStatus({
    silent = false,
    openPanel = false,
    autoOpenImportant = false,
    isFresh = true
  } = {}) {
    try {
      const payload = await fetchJson(baseUrl, "/app/update-status");
      applyStatus(payload, { openPanel, autoOpenImportant, isFresh });
      return payload;
    } catch (error) {
      if (!silent) {
        showToast?.(`Could not load desktop update status: ${errorMessage(error)}`, "error");
      }
      return null;
    }
  }

  async function checkForUpdates({ force = true, silent = false, openPanel = true, autoOpenImportant = false } = {}) {
    applyStatus({ ...state.status, availability: "checking", lastError: "" }, { openPanel });
    try {
      const payload = await postJson(baseUrl, "/app/check-for-update", { force: Boolean(force) });
      const nextStatus = payload?.status || payload || {};
      applyStatus(nextStatus, { openPanel, autoOpenImportant, isFresh: true });
      if (!silent && String(nextStatus?.availability || "") === "up_to_date") {
        showToast?.("Baluffo is already up to date.", "success");
      }
      return nextStatus;
    } catch (error) {
      const failedStatus = {
        ...state.status,
        availability: "error",
        lastError: errorMessage(error),
      };
      applyStatus(failedStatus, { openPanel, isFresh: true });
      if (!silent) {
        showToast?.(`Could not check for updates: ${errorMessage(error)}`, "error");
      }
      return failedStatus;
    }
  }

  async function downloadUpdate() {
    try {
      const payload = await postJson(baseUrl, "/app/download-update", {});
      const nextStatus = payload?.status || state.status;
      applyStatus(nextStatus, { openPanel: true });
      if (payload?.started === false || payload?.error) {
        const feedback = downloadResultMessage(payload?.errorCode, payload?.error);
        showToast?.(feedback.message, feedback.level);
        return payload;
      }
      showToast?.("Desktop update download started.", "info");
      return payload;
    } catch (error) {
      const refreshed = await refreshStatus({ silent: true, openPanel: true });
      const recoveredStatus = normalizeDesktopUpdateStatus(refreshed || {});
      if (refreshed && recoveredStatus.downloadState === "downloading") {
        showToast?.("Desktop update download started.", "info");
        return { started: true, status: refreshed, recovered: true };
      }
      if (refreshed && (recoveredStatus.downloadState === "downloaded" || recoveredStatus.installState === "ready")) {
        showToast?.("Desktop update is ready to install.", "info");
        return { started: false, status: refreshed, recovered: true };
      }
      if (refreshed && recoveredStatus.downloadState === "failed" && recoveredStatus.lastError) {
        showToast?.(recoveredStatus.lastError, "error");
        return { started: false, status: refreshed, recovered: true };
      }
      applyStatus({ ...state.status, availability: "error", lastError: errorMessage(error) }, { openPanel: true });
      showToast?.(`Could not download the update: ${errorMessage(error)}`, "error");
      return null;
    }
  }

  async function installUpdate() {
    const confirmed = await requestConfirmationDialog?.({
      title: "Install update now?",
      description: "Baluffo will close, install the downloaded update, preserve ship\\data, and reopen automatically.",
      confirmLabel: "Install and restart",
      cancelLabel: "Later",
    });
    if (!confirmed) return null;
    try {
      const payload = await postJson(baseUrl, "/app/install-update", {});
      const nextStatus = payload?.status || { ...state.status, installState: "handoff_requested" };
      applyStatus(nextStatus, { openPanel: true });
      if (payload?.started === false || payload?.error) {
        const feedback = installResultMessage(payload?.errorCode, payload?.error);
        showToast?.(feedback.message, feedback.level);
        return payload;
      }
      showToast?.("Closing Baluffo to install the update...", "info");
      return payload;
    } catch (error) {
      applyStatus({ ...state.status, availability: "error", lastError: errorMessage(error) }, { openPanel: true });
      showToast?.(`Could not start install: ${errorMessage(error)}`, "error");
      return null;
    }
  }

  function openReleaseNotes() {
    const title = buildReleaseNotesTitle(state.status);
    const markdown = String(state.status.releaseNotesBody || "").trim();
    const releaseNotesUrl = String(state.status.releaseNotesUrl || "").trim();
    const fallbackMessage = releaseNotesUrl
      ? "Release notes are unavailable in-app for this build. You can open the release on GitHub instead."
      : "Release notes are unavailable for this build.";
    if (typeof showReleaseNotesDialog === "function") {
      return showReleaseNotesDialog({
        title,
        markdown,
        publishedAt: state.status.releaseNotesPublishedAt,
        releaseNotesUrl,
        openExternalUrl,
        fallbackMessage,
      });
    }
    if (releaseNotesUrl) {
      openExternalUrl?.(releaseNotesUrl);
      return null;
    }
    showToast?.("Release notes are unavailable for this build.", "info");
    return null;
  }

  async function handlePrimaryAction() {
    const action = String(refs.desktopUpdatePrimaryBtn?.dataset?.action || "check");
    if (state.pendingAction) return;
    state.pendingAction = action;
    render();
    try {
      if (action === "download") {
        await downloadUpdate();
        return;
      }
      if (action === "install") {
        await installUpdate();
        return;
      }
      await checkForUpdates({ force: true, silent: false, openPanel: true });
    } finally {
      state.pendingAction = "";
      render();
    }
  }

  async function handleSecondaryAction() {
    const action = String(refs.desktopUpdateSecondaryBtn?.dataset?.action || "close");
    if (action === "later") {
      const targetVersion = String(state.status.targetVersion || state.status.latestVersion || "").trim();
      if (targetVersion) {
        state.dismissedTargetVersion = targetVersion;
      }
    }
    state.panelOpen = false;
    render();
  }

  function handleReleaseNotesClick(event) {
    event?.preventDefault?.();
    openReleaseNotes();
  }

  async function handleToggleClick() {
    if (!state.panelOpen) {
      state.panelOpen = true;
      render();
      if (!state.status.lastCheckedAt && state.status.availability === "unknown") {
        await checkForUpdates({ force: false, silent: true, openPanel: true, autoOpenImportant: true });
        return;
      }
      if (!shouldPollDesktopUpdateStatus(state.status)) {
        await refreshStatus({ silent: true, openPanel: true });
      }
      return;
    }
    state.panelOpen = false;
    render();
  }

  function bind() {
    if (state.bound) return;
    state.bound = true;
    bindAsyncClick?.(refs.desktopUpdateToggleBtn, handleToggleClick);
    bindAsyncClick?.(refs.desktopUpdatePrimaryBtn, handlePrimaryAction);
    bindAsyncClick?.(refs.desktopUpdateSecondaryBtn, handleSecondaryAction);
    refs.desktopUpdateReleaseNotes?.addEventListener?.("click", handleReleaseNotesClick);
  }

  async function mount() {
    bind();
    state.mounted = true;
    render();
    if (!isDesktopRuntimeMode?.()) return;
    await refreshStatus({ silent: true, autoOpenImportant: true, isFresh: false });
  }

  async function startAutoCheck() {
    if (!isDesktopRuntimeMode?.() || state.autoCheckStarted) return;
    state.autoCheckStarted = true;
    await checkForUpdates({ force: false, silent: true, openPanel: false, autoOpenImportant: true });
  }

  return {
    mount,
    render,
    refreshStatus,
    checkForUpdates,
    startAutoCheck,
    handlePrimaryAction,
    handleSecondaryAction,
    handleReleaseNotesClick,
    stopPolling,
    _getState() {
      return { ...state };
    },
  };
}
