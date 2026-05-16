import { escapeHtml } from "../../../shared/ui/index.js";
import {
  sanitizeUrl,
  toContractClass,
  fullCountryName
} from "../../../shared/data/index.js";
import { normalizeToken } from "../../../shared/text-utils.js";
import {
  buildSavedJobViewModel,
  filterSavedJobViews,
  isCustomJob,
  sortSavedJobViews
} from "../view-state.js";

function captureActiveNotesContext(savedJobsListEl) {
  const active = document.activeElement;
  if (!(active instanceof HTMLTextAreaElement)) return null;
  if (!active.classList.contains("job-notes-input")) return null;
  const jobKey = String(active.dataset.jobKey || "").trim();
  if (!jobKey) return null;
  return {
    jobKey,
    selectionStart: Number(active.selectionStart) || 0,
    selectionEnd: Number(active.selectionEnd) || 0,
    scrollTop: Number(active.scrollTop) || 0,
    pageScrollX: Number(window.scrollX) || 0,
    pageScrollY: Number(window.scrollY) || 0,
    listBound: Boolean(savedJobsListEl)
  };
}

export function createSavedRenderController({
  dom,
  viewState,
  timelineScopeAll,
  timelineScopeSelected,
  phaseOptions = [],
  phaseLabels = {},
  outcomeOptions = [],
  outcomeLabels = {},
  normalizePhase: normalizePhaseValue = phase => String(phase || "bookmark").trim().toLowerCase() || "bookmark",
  normalizeOutcome: normalizeOutcomeValue = outcome => String(outcome || "active").trim().toLowerCase() || "active",
  canTransitionPhase = () => true,
  canSetOutcome: canSetOutcomeValue = () => true,
  customSourceLabel,
  reminderSoonHours,
  maxAttachmentsPerJob,
  maxAttachmentBytes,
  computeAnchorScrollDelta,
  cssEscape,
  renderTimeline,
  setActivityPanelOpen,
  renderWorkspaceStats,
  renderSelectedJobHint,
  updateTimelineScopeButtons,
  setSavedFilterBarVisible,
  setSavedSortBarVisible,
  renderSavedFilterMeta,
  renderReminderCounter,
  hydrateAttachmentLists,
  hydrateAttachmentListForJob,
  bindAttachmentActionButtons,
  renderSavedJobBlockHtml,
  parseIsoDate,
  getReminderMeta,
  formatRelativeTime,
  getJobHistoryEntries,
  renderPhaseBar,
  renderWebIcon,
  formatPhaseTimestamp,
  renderDetailsSummary,
  activityTypeLabel,
  formatActivityDetail
}) {
  function renderAuthRequired(message) {
    const { savedJobsListEl } = dom;
    if (!savedJobsListEl) return;
    savedJobsListEl.innerHTML = `
      <div class="no-results saved-auth-required">
        <strong>Saved jobs require a local profile.</strong>
        <p>${escapeHtml(message)}</p>
        <p class="muted">Sign in on this device to save jobs, add notes, attach files, and track application progress.</p>
      </div>
    `;
  }

  function restoreActiveNotesContext(context, options = {}) {
    const { restorePage = true } = options;
    const { savedJobsListEl } = dom;
    if (!context || !savedJobsListEl) return;
    const selector = `.job-notes-input[data-job-key="${cssEscape(context.jobKey)}"]`;
    const textarea = savedJobsListEl.querySelector(selector);
    if (!(textarea instanceof HTMLTextAreaElement)) return;
    try {
      textarea.focus({ preventScroll: true });
    } catch {
      textarea.focus();
    }
    try {
      textarea.setSelectionRange(context.selectionStart, context.selectionEnd);
    } catch {
      // Ignore selection restore issues.
    }
    textarea.scrollTop = context.scrollTop;
    if (restorePage) {
      window.scrollTo(context.pageScrollX, context.pageScrollY);
    }
  }

  function captureRenderContext() {
    const { savedJobsListEl } = dom;
    const notesContext = captureActiveNotesContext(savedJobsListEl);
    const anchorKey = String(
      notesContext?.jobKey || viewState.selectedJobKey || viewState.expandedJobKey || ""
    ).trim();
    let anchorTop = Number.NaN;
    let listScrollTop = 0;
    if (savedJobsListEl) {
      listScrollTop = Number(savedJobsListEl.scrollTop) || 0;
      if (anchorKey) {
        const anchorSelector = `.saved-job-block[data-job-key="${cssEscape(anchorKey)}"]`;
        const anchorEl = savedJobsListEl.querySelector(anchorSelector);
        if (anchorEl instanceof HTMLElement) {
          anchorTop = Number(anchorEl.getBoundingClientRect().top);
        }
      }
    }
    return {
      notesContext,
      anchorKey,
      anchorTop,
      listScrollTop,
      pageScrollX: Number(window.scrollX) || 0,
      pageScrollY: Number(window.scrollY) || 0
    };
  }

  function restoreRenderContext(context) {
    const { savedJobsListEl } = dom;
    if (!context || !savedJobsListEl) return;
    const notesContext = context.notesContext || null;
    if (notesContext) {
      restoreActiveNotesContext(notesContext, { restorePage: false });
    }

    savedJobsListEl.scrollTop = Number(context.listScrollTop) || 0;

    const anchorKey = String(context.anchorKey || "").trim();
    if (anchorKey) {
      const anchorSelector = `.saved-job-block[data-job-key="${cssEscape(anchorKey)}"]`;
      const anchorEl = savedJobsListEl.querySelector(anchorSelector);
      if (anchorEl instanceof HTMLElement) {
        const delta = computeAnchorScrollDelta(
          context.anchorTop,
          anchorEl.getBoundingClientRect().top
        );
        if (Math.abs(delta) > 1) {
          window.scrollBy(0, delta);
        }
      }
    }

    if (!notesContext) {
      window.scrollTo(Number(context.pageScrollX) || 0, Number(context.pageScrollY) || 0);
    }
  }

  function normalizeSavedSector(job) {
    const raw = String(job?.sector || "").trim();
    const lower = raw.toLowerCase();
    if (lower === "game" || lower === "game company" || lower === "gaming") return "Game";
    if (lower === "tech" || lower === "tech company" || lower === "technology") return "Tech";

    const companyType = normalizeToken(job?.companyType);
    if (companyType === "game" || companyType === "game company") return "Game";
    if (companyType === "tech" || companyType === "tech company") return "Tech";
    return raw || "Tech";
  }

  function renderMissingInfoChips(job) {
    if (!isCustomJob(job)) return "";
    const chips = [];
    if (!sanitizeUrl(job.jobLink || "")) chips.push("No link");
    if (!String(job.city || "").trim()) chips.push("No city");
    if (
      !String(job.contractType || "").trim() ||
      String(job.contractType || "").toLowerCase() === "unknown"
    ) {
      chips.push("No contract");
    }
    if (chips.length === 0) return "";
    return chips.map(label => `<span class="saved-missing-chip">${escapeHtml(label)}</span>`).join("");
  }

  function renderUpdatedHint(job) {
    if (!isCustomJob(job)) return "";
    const label = String(job?.updatedBy || "").trim();
    if (!label) return "";
    const time = formatRelativeTime(job.updatedAt);
    if (label && time) {
      return `<div class="saved-updated-hint">Updated: ${escapeHtml(label)} · ${escapeHtml(time)}</div>`;
    }
    return `<div class="saved-updated-hint">Updated: ${escapeHtml(label)}</div>`;
  }

  function getJobDetailsTab(jobKey) {
    return viewState.jobDetailTabByKey.get(String(jobKey || "")) || "notes";
  }

  function setJobDetailsTab(jobKey, tab) {
    const safeTab = tab === "attachments" || tab === "history" ? tab : "notes";
    viewState.jobDetailTabByKey.set(String(jobKey || ""), safeTab);
  }

  function normalizePhase(phase) {
    return normalizePhaseValue(phase);
  }

  function normalizeOutcome(outcome) {
    return normalizeOutcomeValue(outcome);
  }

  function canTransition(currentPhase, nextPhase, outcomeStatus = "active") {
    return canTransitionPhase(currentPhase, nextPhase, outcomeStatus);
  }

  function canSetOutcome(currentOutcome, nextOutcome) {
    return canSetOutcomeValue(currentOutcome, nextOutcome);
  }

  function renderSavedJobBlock(viewOrJob) {
    const view = viewOrJob?.job ? viewOrJob : buildSavedJobViewModel(viewOrJob, {
      lifecycleOverlay: viewState.savedLifecycleOverlayByJobKey.get(
        String(viewOrJob?.jobKey || "").trim().toLowerCase()
      ) || null,
      parseIsoDate,
      currentUser: viewState.currentUser
    });
    const job = view.job;
    const lifecycleOverlay = view.lifecycleOverlay;
    return renderSavedJobBlockHtml(job, {
      isCustomJob,
      customSourceLabel,
      normalizeSavedSector,
      fullCountryName,
      sanitizeUrl,
      toContractClass,
      normalizePhase,
      expandedJobKey: viewState.expandedJobKey,
      selectedJobKey: viewState.selectedJobKey,
      getJobDetailsTab,
      renderDetailsSummary,
      getReminderMeta: reminderAt => getReminderMeta(reminderAt, {
        reminderSoonHours
      }),
      renderMissingInfoChips,
      renderUpdatedHint,
      getJobHistoryEntries: jobKey => getJobHistoryEntries(jobKey, {
        cachedActivityEntries: viewState.cachedActivityEntries,
        activityTypeLabel,
        formatPhaseTimestamp,
        formatActivityDetail
      }),
      lifecycleOverlay,
      jobView: view,
      renderWebIcon,
      phaseOptions,
      phaseLabels,
      outcomeOptions,
      outcomeLabels,
      canTransition,
      canSetOutcome,
      trackingOverrideContext: viewState.trackingOverrideContext || viewState.phaseOverrideContext,
      renderPhaseBar: (jobKey, activePhase, phaseTimestamps, savedAt) => renderPhaseBar(
        jobKey,
        activePhase,
        phaseTimestamps,
        savedAt,
        {
          phaseOptions,
          phaseLabels,
          outcomeOptions,
          outcomeLabels,
          canTransition,
          canSetOutcome,
          currentUser: viewState.currentUser,
          trackingOverrideContext: viewState.trackingOverrideContext || viewState.phaseOverrideContext
        }
      ),
      currentUser: viewState.currentUser,
      maxAttachmentsPerJob,
      maxAttachmentBytes
    });
  }

  function renderSavedJobs(jobs) {
    const { savedJobsListEl } = dom;
    if (!savedJobsListEl) return;
    const renderContext = captureRenderContext();
    const allJobs = Array.isArray(jobs) ? jobs : [];
    const allViews = allJobs.map(job => buildSavedJobViewModel(job, {
      lifecycleOverlay: viewState.savedLifecycleOverlayByJobKey.get(
        String(job?.jobKey || "").trim().toLowerCase()
      ) || null,
      parseIsoDate,
      currentUser: viewState.currentUser
    }));
    const filteredViews = sortSavedJobViews(
      filterSavedJobViews(allViews, viewState.activeSavedFilter),
      viewState.activeSavedSort
    );
    const filteredJobs = filteredViews.map(view => view.job);
    setSavedFilterBarVisible(allJobs.length > 0 && Boolean(viewState.currentUser));
    setSavedSortBarVisible(allJobs.length > 0 && Boolean(viewState.currentUser));
    renderSavedFilterMeta(allJobs.length, filteredJobs.length);
    renderReminderCounter(allJobs);
    renderWorkspaceStats(allJobs);

    if (allJobs.length === 0) {
      viewState.expandedJobKey = null;
      viewState.selectedJobKey = "";
      viewState.phaseOverrideContext = null;
      viewState.trackingOverrideContext = null;
      if (typeof setActivityPanelOpen === "function") {
        setActivityPanelOpen(false, { persist: false });
      }
      renderSelectedJobHint();
      savedJobsListEl.innerHTML = '<div class="no-results">No saved jobs yet.</div>';
      renderTimeline();
      return;
    }

    if (!allJobs.some(job => String(job?.jobKey || "").trim() === viewState.selectedJobKey)) {
      viewState.selectedJobKey = "";
      viewState.phaseOverrideContext = null;
      viewState.trackingOverrideContext = null;
      renderSelectedJobHint();
      updateTimelineScopeButtons();
      if (viewState.timelineScope === timelineScopeSelected) {
        viewState.timelineScope = timelineScopeAll;
        updateTimelineScopeButtons();
      }
    }
    if (!filteredJobs.some(job => String(job?.jobKey || "").trim() === viewState.expandedJobKey)) {
      viewState.expandedJobKey = null;
    }

    if (filteredJobs.length === 0) {
      savedJobsListEl.innerHTML = '<div class="no-results">No saved jobs match this filter.</div>';
      renderTimeline();
      return;
    }

    savedJobsListEl.innerHTML = `
      <div class="jobs-table-header">
        <div class="saved-row-header">
          <div class="col-title">Position</div>
          <div class="col-company">Company</div>
          <div class="col-location">Location</div>
          <div class="col-contract">Contract</div>
          <div class="col-type">Type</div>
          <div class="col-link">Link</div>
        </div>
      </div>
      <div class="jobs-table-body">
        ${filteredViews.map(renderSavedJobBlock).join("")}
      </div>
    `;

    bindAttachmentActionButtons();
    applyDetailsAccordion();
    renderTimeline();
    restoreRenderContext(renderContext);

    const activeAttachmentView = filteredViews.find(view => (
      view.jobKey === viewState.expandedJobKey && getJobDetailsTab(view.jobKey) === "attachments"
    ));
    if (activeAttachmentView && typeof hydrateAttachmentListForJob === "function") {
      hydrateAttachmentListForJob(activeAttachmentView.jobKey).catch(err => {
        console.error("Could not load attachment list:", err);
      });
    } else if (activeAttachmentView && typeof hydrateAttachmentLists === "function") {
      hydrateAttachmentLists([activeAttachmentView.job]).catch(err => {
        console.error("Could not load attachment lists:", err);
      });
    }
  }

  function setSelectedJobKey(jobKey, options = {}) {
    const { savedJobsListEl } = dom;
    const { rerenderTimeline = true } = options;
    const nextKey = String(jobKey || "").trim();
    if (nextKey === viewState.selectedJobKey) return;
    viewState.selectedJobKey = nextKey;
    renderSelectedJobHint();
    updateTimelineScopeButtons();
    if (viewState.timelineScope === timelineScopeSelected && !viewState.selectedJobKey) {
      viewState.timelineScope = timelineScopeAll;
      updateTimelineScopeButtons();
    }
    if (rerenderTimeline) {
      renderTimeline();
    }
    if (savedJobsListEl) {
      savedJobsListEl.querySelectorAll(".saved-job-block").forEach(block => {
        const isActive = String(block.dataset.jobKey || "") === viewState.selectedJobKey;
        block.dataset.selected = isActive ? "true" : "false";
      });
    }
  }

  function toggleDetailsForJob(jobKey) {
    if (!jobKey) return;
    setSelectedJobKey(jobKey, { rerenderTimeline: false });
    const nextKey = viewState.expandedJobKey === jobKey ? null : jobKey;
    if (nextKey && !viewState.jobDetailTabByKey.has(nextKey)) {
      viewState.jobDetailTabByKey.set(nextKey, "notes");
    }
    viewState.expandedJobKey = nextKey;
    applyDetailsAccordion();
    if (nextKey && getJobDetailsTab(nextKey) === "attachments" && typeof hydrateAttachmentListForJob === "function") {
      hydrateAttachmentListForJob(nextKey).catch(err => {
        console.error("Could not load attachment list:", err);
      });
    }
  }

  function applyDetailsAccordion() {
    const { savedJobsListEl } = dom;
    if (!savedJobsListEl) return;
    savedJobsListEl.querySelectorAll(".saved-job-block").forEach(block => {
      const key = block.dataset.jobKey || "";
      const expanded = Boolean(viewState.expandedJobKey) && key === viewState.expandedJobKey;
      const details = block.querySelector(".saved-details-section");
      const toggle = block.querySelector(".details-toggle-btn");
      const arrow = block.querySelector(".details-toggle-arrow");
      if (details) {
        details.classList.toggle("collapsed", !expanded);
        details.setAttribute("aria-hidden", expanded ? "false" : "true");
      }
      if (toggle) {
        toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
        toggle.setAttribute(
          "aria-label",
          `${expanded ? "Collapse" : "Expand"} notes, attachments, and history`
        );
      }
      if (arrow) {
        arrow.classList.toggle("expanded", expanded);
      }
    });
  }

  function setNoteSaveState(jobKey, state) {
    const { savedJobsListEl } = dom;
    const el = savedJobsListEl?.querySelector(`.note-save-state[data-job-key="${cssEscape(jobKey)}"]`);
    if (!el) return;
    if (state === "saving") {
      el.textContent = "Saving...";
      el.classList.add("saving");
      el.classList.remove("error");
      return;
    }
    if (state === "error") {
      el.textContent = "Error";
      el.classList.remove("saving");
      el.classList.add("error");
      return;
    }
    el.textContent = "Saved";
    el.classList.remove("saving");
    el.classList.remove("error");
  }

  function applyJobDetailsTab(jobKey, tab) {
    const { savedJobsListEl } = dom;
    if (!savedJobsListEl || !jobKey) return;
    const safeTab = tab === "attachments" || tab === "history" ? tab : "notes";
    const block = savedJobsListEl.querySelector(`.saved-job-block[data-job-key="${cssEscape(jobKey)}"]`);
    if (!(block instanceof HTMLElement)) return;
    const buttons = Array.from(block.querySelectorAll(".saved-details-tab-btn"));
    const panels = Array.from(block.querySelectorAll(".saved-details-panel"));
    buttons.forEach(btn => {
      const active = String(btn.dataset.detailsTab || "") === safeTab;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-selected", active ? "true" : "false");
    });
    panels.forEach(panel => {
      const active = String(panel.getAttribute("data-tab-panel") || "") === safeTab;
      panel.classList.toggle("hidden", !active);
    });
    if (safeTab === "attachments" && typeof hydrateAttachmentListForJob === "function") {
      hydrateAttachmentListForJob(jobKey).catch(err => {
        console.error("Could not load attachment list:", err);
      });
    }
  }

  return {
    renderAuthRequired,
    renderSavedJobs,
    getJobDetailsTab,
    setJobDetailsTab,
    normalizePhase,
    normalizeOutcome,
    canTransition,
    canSetOutcome,
    setSelectedJobKey,
    toggleDetailsForJob,
    applyDetailsAccordion,
    setNoteSaveState,
    applyJobDetailsTab
  };
}
