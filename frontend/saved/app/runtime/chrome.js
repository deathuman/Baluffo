import { setText } from "../../../shared/ui/index.js";
import { setElementText, setStatusText } from "./view.js";

export function createSavedChrome(deps) {
  function cssEscape(value) {
    if (deps.windowObject?.CSS && typeof deps.windowObject.CSS.escape === "function") {
      return deps.windowObject.CSS.escape(value);
    }
    return String(value).replace(/["\\]/g, "\\$&");
  }

  function setSavedFilter(nextFilter) {
    deps.viewState.activeSavedFilter = deps.isValidSavedFilter(nextFilter) ? nextFilter : deps.defaultSavedFilter;
    deps.dom.savedCustomFilterBtnEls.forEach(btn => {
      const isActive = String(btn.dataset.savedFilter || "").toLowerCase() === deps.viewState.activeSavedFilter;
      btn.classList.toggle("active", isActive);
    });
  }

  function setSavedSort(nextSort) {
    deps.viewState.activeSavedSort = deps.isValidSavedSort(nextSort) ? nextSort : deps.defaultSavedSort;
    deps.dom.savedSortBtnEls.forEach(btn => {
      const isActive = String(btn.dataset.savedSort || "").toLowerCase() === deps.viewState.activeSavedSort;
      btn.classList.toggle("active", isActive);
    });
  }

  function setSavedSortBarVisible(visible) {
    if (!deps.dom.savedSortBarEl) return;
    deps.dom.savedSortBarEl.classList.toggle("hidden", !visible);
    deps.dom.savedSortBarEl.setAttribute("aria-hidden", visible ? "false" : "true");
  }

  function setSavedFilterBarVisible(visible) {
    if (!deps.dom.savedCustomFilterBarEl) return;
    deps.dom.savedCustomFilterBarEl.classList.toggle("hidden", !visible);
    deps.dom.savedCustomFilterBarEl.setAttribute("aria-hidden", visible ? "false" : "true");
  }

  function renderSavedFilterMeta(totalCount, filteredCount) {
    if (!deps.dom.savedCustomFilterCountEl) return;
    const safeTotal = Math.max(0, Number(totalCount) || 0);
    const safeFiltered = Math.max(0, Number(filteredCount) || 0);
    if (safeTotal <= 0) {
      deps.dom.savedCustomFilterCountEl.textContent = "";
      return;
    }
    deps.dom.savedCustomFilterCountEl.textContent = `${safeFiltered}/${safeTotal}`;
  }

  function renderReminderCounter(allJobs) {
    if (!deps.dom.savedReminderCounterEl) return;
    const rows = Array.isArray(allJobs) ? allJobs : [];
    const soonCount = rows.filter(job => deps.getReminderMeta(job?.reminderAt).isSoon).length;
    deps.dom.savedReminderCounterEl.textContent = soonCount > 0 ? `${soonCount} due soon` : "";
  }

  function setSourceStatus(text) {
    setStatusText(setText, deps.dom.savedSourceStatusEl, text);
  }

  function setActivityStatus(text) {
    setElementText(deps.dom.activityPanelStatusEl, text);
  }

  function setBackupButtonsEnabled(enabled) {
    if (deps.dom.exportBackupBtnEl) deps.dom.exportBackupBtnEl.disabled = !enabled;
    if (deps.dom.exportIncludeFilesEl) deps.dom.exportIncludeFilesEl.disabled = !enabled;
    if (deps.dom.importBackupBtnEl) deps.dom.importBackupBtnEl.disabled = !enabled;
    if (deps.dom.globalPhaseOverrideBtnEl) deps.dom.globalPhaseOverrideBtnEl.disabled = !enabled;
    updateGlobalOverrideButton();
  }

  function updateGlobalOverrideButton() {
    if (!deps.dom.globalPhaseOverrideBtnEl) return;
    deps.dom.globalPhaseOverrideBtnEl.classList.toggle("active", deps.viewState.phaseOverrideArmedGlobal);
    deps.dom.globalPhaseOverrideBtnEl.textContent = deps.viewState.phaseOverrideArmedGlobal
      ? "Override Armed (One Use)"
      : "Override Phase Lock";
  }

  function getLastJobsUrl() {
    return deps.readSavedLastJobsUrl(deps.jobsLastUrlKey, "jobs.html");
  }

  return {
    cssEscape,
    setSavedFilter,
    setSavedSort,
    setSavedSortBarVisible,
    setSavedFilterBarVisible,
    renderSavedFilterMeta,
    renderReminderCounter,
    setSourceStatus,
    setActivityStatus,
    setBackupButtonsEnabled,
    updateGlobalOverrideButton,
    getLastJobsUrl
  };
}
