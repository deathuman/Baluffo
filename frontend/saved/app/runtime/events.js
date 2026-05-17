import { UI_TOKENS, ui } from "../../../shared/ui/selectors.js";

export function bindSavedPageEvents({
  dom,
  viewState,
  bindUi,
  bindAsyncClick,
  getLastJobsUrl,
  navigateDesktopPage,
  showToast,
  defaultSavedFilter,
  defaultSavedSort,
  defaultSavedGroup = "none",
  timelineScopeAll,
  setCustomJobPanelOpen,
  createCustomJob,
  updateCustomJobWarning,
  setSavedFilter,
  setSavedSort,
  setSavedGroup = () => {},
  persistSavedListPreferences = () => {},
  renderSavedJobs,
  setActivityPanelOpen,
  refreshActivityLog,
  signInUser,
  signOutUser,
  exportBackup,
  importBackup,
  setTimelineScope,
  renderTimeline
}) {
  const {
    jobsPageBtnEl,
    adminPageBtnEl,
    addCustomJobBtnEl,
    customJobCancelBtnEl,
    customJobFormEl,
    customJobTitleEl,
    customJobLinkEl,
    savedCustomFilterBtnEls,
    savedSortBtnEls,
    savedGroupBtnEls = [],
    historyPanelToggleBtnEl,
    activityRefreshBtnEl,
    activityCloseBtnEl,
    signInBtnEl,
    signOutBtnEl,
    exportBackupBtnEl,
    importBackupBtnEl,
    importBackupInputEl,
    activityScopeBtnEls
  } = dom;

  bindUi(jobsPageBtnEl, "click", () => {
    const target = getLastJobsUrl();
    navigateDesktopPage(target);
  });
  bindUi(adminPageBtnEl, "click", () => {
    if (viewState.adminBridgeButtonState !== "online") {
      showToast("Admin bridge is offline.", "info");
      return;
    }
    navigateDesktopPage("admin.html");
  });
  bindUi(addCustomJobBtnEl, "click", () => {
    if (!viewState.currentUser) {
      showToast("Sign in to add custom jobs.", "info");
      return;
    }
    setCustomJobPanelOpen(!viewState.customJobPanelOpen);
    if (viewState.customJobPanelOpen) customJobTitleEl?.focus();
  });
  bindUi(customJobCancelBtnEl, "click", () => {
    setCustomJobPanelOpen(false);
  });

  if (customJobFormEl) {
    customJobFormEl.addEventListener("submit", async event => {
      event.preventDefault();
      await createCustomJob();
    });
  }

  if (customJobLinkEl) {
    customJobLinkEl.addEventListener("input", updateCustomJobWarning);
  }

  savedCustomFilterBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const nextFilter = String(btn.dataset.savedFilter || defaultSavedFilter).toLowerCase();
      setSavedFilter(nextFilter);
      renderSavedJobs(Array.from(viewState.lastSavedJobsByKey.values()));
    });
  });

  savedSortBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const sortKey = String(btn.dataset.savedSort || defaultSavedSort).toLowerCase();
      setSavedSort(sortKey);
      renderSavedJobs(Array.from(viewState.lastSavedJobsByKey.values()));
    });
  });

  savedGroupBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const groupKey = String(btn.dataset.savedGroup || defaultSavedGroup).toLowerCase();
      setSavedGroup(groupKey);
      persistSavedListPreferences(viewState.currentUser?.uid || "", {
        group: viewState.activeSavedGroup
      });
      renderSavedJobs(Array.from(viewState.lastSavedJobsByKey.values()));
    });
  });

  bindUi(historyPanelToggleBtnEl, "click", () => {
    setActivityPanelOpen(!viewState.activityPanelOpen);
  });
  bindUi(activityCloseBtnEl, "click", () => {
    setActivityPanelOpen(false);
  });
  bindAsyncClick(activityRefreshBtnEl, refreshActivityLog);
  bindAsyncClick(signInBtnEl, signInUser);
  bindAsyncClick(signOutBtnEl, signOutUser);
  bindAsyncClick(exportBackupBtnEl, exportBackup);

  if (importBackupBtnEl && importBackupInputEl) {
    importBackupBtnEl.addEventListener("click", () => {
      importBackupInputEl.click();
    });
    importBackupInputEl.addEventListener("change", async () => {
      const file = importBackupInputEl.files && importBackupInputEl.files[0];
      if (!file) return;
      await importBackup(file);
      importBackupInputEl.value = "";
    });
  }

  activityScopeBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const scope = String(btn.dataset.timelineScope || timelineScopeAll);
      if (scope === "selected" && !viewState.selectedJobKey) {
        showToast("Select or expand a job first.", "info");
        return;
      }
      setTimelineScope(scope);
      renderTimeline();
    });
  });
}

export function bindSavedJobsListDelegation({
  dom,
  viewState,
  cssEscape,
  setSelectedJobKey,
  removeSavedJob,
  updatePhase,
  updateOutcome,
  toggleDetailsForJob,
  openCustomJobEditor,
  setJobDetailsTab,
  applyJobDetailsTab,
  refreshActivityLog,
  renderSavedJobs,
  queueNotesSave,
  flushNotesSave,
  uploadAttachments
}) {
  const { savedJobsListEl } = dom;
  if (!savedJobsListEl) return;
  const t = UI_TOKENS.saved;

  savedJobsListEl.addEventListener("click", event => {
    const target = event.target;
    if (!(target instanceof Element)) return;

    const removeBtn = target.closest(ui(t.removeBtn));
    if (removeBtn) {
      const jobKey = removeBtn.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      removeSavedJob(jobKey).catch(() => {});
      return;
    }

    const phaseBtn = target.closest(ui(t.phaseBtn));
    if (phaseBtn) {
      const jobKey = phaseBtn.dataset.jobKey || "";
      const phase = phaseBtn.dataset.phase || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      updatePhase(jobKey, phase).catch(() => {});
      return;
    }

    const outcomeBtn = target.closest(ui(t.outcomeBtn));
    if (outcomeBtn) {
      const jobKey = outcomeBtn.dataset.jobKey || "";
      const outcomeStatus = outcomeBtn.dataset.outcomeStatus || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      updateOutcome(jobKey, outcomeStatus).catch(() => {});
      return;
    }

    const trackingOverrideConfirmBtn = target.closest(ui(t.trackingOverrideConfirmBtn));
    if (trackingOverrideConfirmBtn) {
      const jobKey = trackingOverrideConfirmBtn.dataset.jobKey || "";
      const kind = trackingOverrideConfirmBtn.dataset.trackingKind || "phase";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      if (kind === "outcome") {
        updateOutcome(jobKey, trackingOverrideConfirmBtn.dataset.outcomeStatus || "", {
          overrideThisTransition: true
        }).catch(() => {});
      } else {
        updatePhase(jobKey, trackingOverrideConfirmBtn.dataset.phase || "", {
          overrideThisTransition: true
        }).catch(() => {});
      }
      return;
    }

    const trackingOverrideCancelBtn = target.closest(ui(t.trackingOverrideCancelBtn));
    if (trackingOverrideCancelBtn) {
      viewState.phaseOverrideContext = null;
      viewState.trackingOverrideContext = null;
      renderSavedJobs(Array.from(viewState.lastSavedJobsByKey.values()));
      return;
    }

    const detailsToggle = target.closest(ui(t.detailsToggle));
    if (detailsToggle) {
      const jobKey = detailsToggle.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      toggleDetailsForJob(jobKey);
      return;
    }

    const personalEditBtn = target.closest(ui(t.personalEditBtn));
    if (personalEditBtn) {
      const jobKey = personalEditBtn.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      openCustomJobEditor(jobKey, false);
      return;
    }

    const personalDuplicateBtn = target.closest(ui(t.personalDuplicateBtn));
    if (personalDuplicateBtn) {
      const jobKey = personalDuplicateBtn.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      openCustomJobEditor(jobKey, true);
      return;
    }

    const detailsTabBtn = target.closest(ui(t.detailsTabBtn));
    if (detailsTabBtn) {
      const jobKey = detailsTabBtn.dataset.jobKey || "";
      const tab = detailsTabBtn.dataset.detailsTab || "notes";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      setJobDetailsTab(jobKey, tab);
      applyJobDetailsTab(jobKey, tab);
      return;
    }

    const historyRefreshBtn = target.closest(ui(t.historyRefreshBtn));
    if (historyRefreshBtn) {
      const jobKey = historyRefreshBtn.dataset.jobKey || "";
      setSelectedJobKey(jobKey, { rerenderTimeline: false });
      refreshActivityLog().then(() => {
        renderSavedJobs(Array.from(viewState.lastSavedJobsByKey.values()));
      }).catch(() => {});
      return;
    }

    const attachUploadBtn = target.closest(ui(t.attachUploadBtn));
    if (attachUploadBtn) {
      const key = attachUploadBtn.dataset.jobKey || "";
      setSelectedJobKey(key, { rerenderTimeline: false });
      const input = savedJobsListEl.querySelector(`.attach-file-input[data-job-key="${cssEscape(key)}"]`);
      if (input) input.click();
      return;
    }

  });

  savedJobsListEl.addEventListener("input", event => {
    const target = event.target;
    if (!(target instanceof HTMLTextAreaElement)) return;
    if (!target.classList.contains("job-notes-input")) return;
    const jobKey = target.dataset.jobKey || "";
    setSelectedJobKey(jobKey, { rerenderTimeline: false });
    queueNotesSave(jobKey, target.value);
  });

  savedJobsListEl.addEventListener("focusout", event => {
    const target = event.target;
    if (!(target instanceof HTMLTextAreaElement)) return;
    if (!target.classList.contains("job-notes-input")) return;
    const jobKey = target.dataset.jobKey || "";
    setSelectedJobKey(jobKey, { rerenderTimeline: false });
    flushNotesSave(jobKey, target.value).catch(() => {});
  });

  savedJobsListEl.addEventListener("change", event => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (!target.classList.contains("attach-file-input")) return;
    const files = target.files ? Array.from(target.files) : [];
    if (files.length === 0) return;
    const jobKey = target.dataset.jobKey || "";
    setSelectedJobKey(jobKey, { rerenderTimeline: false });
    uploadAttachments(jobKey, files).catch(() => {});
    target.value = "";
  });
}
