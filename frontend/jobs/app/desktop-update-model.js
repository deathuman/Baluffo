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

function normalizeReleaseNotesHistory(history) {
  if (!Array.isArray(history)) return [];
  return history
    .filter(item => item && typeof item === "object")
    .map(item => ({
      releaseNotesUrl: String(item.releaseNotesUrl || ""),
      releaseNotesTitle: String(item.releaseNotesTitle || ""),
      releaseNotesBody: String(item.releaseNotesBody || ""),
      releaseNotesPublishedAt: String(item.releaseNotesPublishedAt || ""),
      releaseTag: String(item.releaseTag || ""),
      releaseVersion: String(item.releaseVersion || ""),
    }))
    .filter(item => (
      item.releaseNotesUrl
      || item.releaseNotesTitle
      || item.releaseNotesBody
      || item.releaseNotesPublishedAt
      || item.releaseTag
      || item.releaseVersion
    ));
}

function releaseNotesHistoryVisible(history) {
  return history.some(item => item.releaseNotesBody || item.releaseNotesUrl);
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
    releaseNotesHistory: normalizeReleaseNotesHistory(payload.releaseNotesHistory),
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

export function downloadResultMessage(errorCode, fallbackMessage) {
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

export function installResultMessage(errorCode, fallbackMessage) {
  const code = String(errorCode || "").trim().toLowerCase();
  if (code === "manifest_cache_missing") {
    return { message: "Update metadata is missing. Check for updates again.", level: "error" };
  }
  if (code === "install_not_ready") {
    return { message: "Download the update before trying to install it.", level: "error" };
  }
  if (code === "install_handoff_unconfirmed") {
    return { message: "Baluffo could not confirm the updater handoff. Try Install and restart again.", level: "error" };
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

export function buildReleaseNotesTitle(status) {
  return String(
    status.releaseNotesTitle || status.targetVersion || status.latestVersion || "Release notes"
  ).trim();
}

export function inFlightInstallState(installState) {
  return new Set(["handoff_requested", "waiting_for_exit", "installing", "verifying"]).has(
    String(installState || "").toLowerCase()
  );
}

function installHandoffFailureMessage(status = {}) {
  return String(status.lastError || "").trim()
    || "Baluffo could not confirm the updater handoff. Try Install and restart again.";
}

export function optimisticInstallHandoffStatus(status = {}, fallbackStatus = {}) {
  const normalized = normalizeDesktopUpdateStatus({
    ...fallbackStatus,
    ...(status && typeof status === "object" ? status : {}),
  });
  if (inFlightInstallState(normalized.installState)) {
    return normalized;
  }
  return normalizeDesktopUpdateStatus({
    ...normalized,
    installState: "handoff_requested",
    installStage: "preparing",
    installStageLabel: normalized.installStageLabel || "Preparing update",
    lastError: "",
  });
}

export function installHandoffFailureStatus(status = {}) {
  const normalized = normalizeDesktopUpdateStatus(status);
  return normalizeDesktopUpdateStatus({
    ...normalized,
    installState: "failed",
    installStage: "",
    installStageLabel: "",
    lastError: installHandoffFailureMessage(normalized),
  });
}

export function shouldExposeJobsDesktopUpdateStatus(status = {}, { hasFreshStatus = false } = {}) {
  if (hasFreshStatus) return true;
  const normalized = normalizeDesktopUpdateStatus(status);
  if (normalized.downloadState === "downloading") return true;
  if (normalized.downloadState === "downloaded") return true;
  if (normalized.installState === "ready") return true;
  return inFlightInstallState(normalized.installState);
}

export function failureToastForStatus(previousStatus, nextStatus) {
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
    releaseNotesHistory: normalized.releaseNotesHistory,
    releaseNotesVisible: Boolean(
      normalized.releaseNotesBody
      || normalized.releaseNotesUrl
      || releaseNotesHistoryVisible(normalized.releaseNotesHistory)
    ),
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
