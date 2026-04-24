import { setDisabled, setText, toggleHidden } from "./desktop-update-dom.js";
import {
  buildReleaseNotesTitle,
  deriveDesktopUpdateView,
  downloadResultMessage,
  failureToastForStatus,
  inFlightInstallState,
  installHandoffFailureStatus,
  installResultMessage,
  normalizeDesktopUpdateStatus,
  optimisticInstallHandoffStatus,
  shouldExposeJobsDesktopUpdateStatus,
  shouldPollDesktopUpdateStatus
} from "./desktop-update-model.js";

function errorMessage(error) {
  if (!error) return "Unknown error";
  if (error instanceof Error && error.message) return error.message;
  return String(error);
}

export function createJobsDesktopUpdateController({
  refs,
  baseUrl,
  fetchJson,
  postJson,
  bindAsyncClick,
  showToast,
  requestConfirmationDialog,
  confirmFallback,
  isDesktopRuntimeMode,
  awaitDesktopBootstrap = async () => true,
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
    installStartPending: false,
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
    if (state.installStartPending) {
      schedulePoll(600);
      return;
    }
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
    let nextStatus = normalizeDesktopUpdateStatus(status);
    if (state.installStartPending) {
      if (
        inFlightInstallState(nextStatus.installState)
        || new Set(["failed", "installed"]).has(nextStatus.installState)
      ) {
        state.installStartPending = false;
      } else if (
        nextStatus.installState === "ready"
        || nextStatus.downloadState === "downloaded"
      ) {
        nextStatus = installHandoffFailureStatus(nextStatus);
        state.installStartPending = false;
      } else {
        nextStatus = optimisticInstallHandoffStatus(nextStatus, previousStatus);
      }
    }
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
    if (!await awaitDesktopBootstrap()) {
      return null;
    }
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

  async function checkForUpdates({
    force = true,
    silent = false,
    openPanel = true,
    autoOpenImportant = false
  } = {}) {
    if (!await awaitDesktopBootstrap()) {
      return null;
    }
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
      if (
        refreshed
        && (recoveredStatus.downloadState === "downloaded" || recoveredStatus.installState === "ready")
      ) {
        showToast?.("Desktop update is ready to install.", "info");
        return { started: false, status: refreshed, recovered: true };
      }
      if (refreshed && recoveredStatus.downloadState === "failed" && recoveredStatus.lastError) {
        showToast?.(recoveredStatus.lastError, "error");
        return { started: false, status: refreshed, recovered: true };
      }
      applyStatus(
        { ...state.status, availability: "error", lastError: errorMessage(error) },
        { openPanel: true }
      );
      showToast?.(`Could not download the update: ${errorMessage(error)}`, "error");
      return null;
    }
  }

  async function installUpdate() {
    const confirmationOptions = {
      title: "Install update now?",
      description: "Baluffo will close, install the downloaded update, preserve ship\\data, and reopen automatically.",
      confirmLabel: "Install and restart",
      cancelLabel: "Later",
    };
    let confirmed = false;
    if (typeof requestConfirmationDialog === "function") {
      confirmed = Boolean(await requestConfirmationDialog(confirmationOptions));
    } else if (typeof confirmFallback === "function") {
      confirmed = Boolean(confirmFallback(
        `${confirmationOptions.title}\n\n${confirmationOptions.description}`
      ));
    } else {
      showToast?.("Could not open the install confirmation dialog. Restart Baluffo and try again.", "error");
      return null;
    }
    if (!confirmed) return null;
    state.installStartPending = false;
    try {
      const payload = await postJson(baseUrl, "/app/install-update", {});
      const payloadStatus = payload?.status || {};
      const payloadInstallState = normalizeDesktopUpdateStatus(payloadStatus).installState;
      const optimisticHandoff = optimisticInstallHandoffStatus(payloadStatus, state.status);
      const nextStatus = payload?.started ? optimisticHandoff : payloadStatus || optimisticHandoff;
      applyStatus(nextStatus, { openPanel: true });
      if (payload?.started === false || payload?.error) {
        state.installStartPending = false;
        const feedback = installResultMessage(payload?.errorCode, payload?.error);
        showToast?.(feedback.message, feedback.level);
        return payload;
      }
      state.installStartPending = !inFlightInstallState(payloadInstallState);
      if (state.installStartPending) {
        schedulePoll(600);
      }
      showToast?.("Closing Baluffo to install the update...", "info");
      return payload;
    } catch (error) {
      state.installStartPending = false;
      applyStatus(
        { ...state.status, availability: "error", lastError: errorMessage(error) },
        { openPanel: true }
      );
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
