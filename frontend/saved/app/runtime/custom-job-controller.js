import { showToast } from "../../../shared/ui/index.js";
import { updateCustomJobWarning as updateCustomJobWarningUi } from "../custom-job.js";
import { isCustomJob } from "../view-state.js";

export function createSavedCustomJobController({
  dom,
  viewState,
  savedPageService,
  normalizeCustomJobInput,
  toDatetimeLocalValue,
  savedDispatch,
  savedActions,
  queueActivityPulse,
  timelineScopeAll,
  refreshActivityLog
}) {
  function resetCustomJobForm() {
    viewState.customJobMode = "create";
    viewState.customJobTargetKey = "";
    dom.customJobFormEl?.reset();
    if (dom.customJobWorkTypeEl) dom.customJobWorkTypeEl.value = "";
    if (dom.customJobContractTypeEl) dom.customJobContractTypeEl.value = "";
    if (dom.customJobSectorEl) dom.customJobSectorEl.value = "";
    if (dom.customJobReminderEl) dom.customJobReminderEl.value = "";
    if (dom.customJobPanelTitleEl) dom.customJobPanelTitleEl.textContent = "Add Custom Job";
    if (dom.customJobPanelHintEl) {
      dom.customJobPanelHintEl.textContent = "Required: Title and Company. Job link is optional.";
    }
    if (dom.customJobSaveBtnEl) dom.customJobSaveBtnEl.textContent = "Save Custom Job";
    updateCustomJobWarning();
  }

  function updateCustomJobWarning() {
    updateCustomJobWarningUi(dom.customJobLinkEl, dom.customJobLinkWarningEl);
  }

  function setCustomJobAvailability(enabled) {
    if (!dom.addCustomJobBtnEl) return;
    dom.addCustomJobBtnEl.disabled = !enabled;
  }

  function setCustomJobPanelOpen(open) {
    viewState.customJobPanelOpen = Boolean(open);
    if (!dom.customJobPanelEl) return;
    dom.customJobPanelEl.classList.toggle("hidden", !viewState.customJobPanelOpen);
    dom.customJobPanelEl.setAttribute("aria-hidden", viewState.customJobPanelOpen ? "false" : "true");
    if (dom.addCustomJobBtnEl) {
      dom.addCustomJobBtnEl.classList.toggle("active", viewState.customJobPanelOpen);
      dom.addCustomJobBtnEl.textContent = viewState.customJobPanelOpen
        ? "Close Custom Job Form"
        : "+ Add Custom Job";
    }
    if (!viewState.customJobPanelOpen) {
      resetCustomJobForm();
    } else {
      updateCustomJobWarning();
    }
  }

  function openCustomJobEditor(jobKey, duplicate) {
    const row = viewState.lastSavedJobsByKey.get(String(jobKey || ""));
    if (!row || !isCustomJob(row)) {
      showToast("Custom job not found.", "error");
      return;
    }
    viewState.customJobMode = duplicate ? "duplicate" : "edit";
    viewState.customJobTargetKey = duplicate ? "" : String(row.jobKey || "");
    if (dom.customJobTitleEl) dom.customJobTitleEl.value = row.title || "";
    if (dom.customJobCompanyEl) dom.customJobCompanyEl.value = row.company || "";
    if (dom.customJobCityEl) dom.customJobCityEl.value = row.city || "";
    if (dom.customJobCountryEl) dom.customJobCountryEl.value = row.country || "";
    if (dom.customJobWorkTypeEl) dom.customJobWorkTypeEl.value = row.workType || "";
    if (dom.customJobContractTypeEl) dom.customJobContractTypeEl.value = row.contractType || "";
    if (dom.customJobSectorEl) dom.customJobSectorEl.value = row.sector || "";
    if (dom.customJobProfessionEl) dom.customJobProfessionEl.value = row.profession || "";
    if (dom.customJobLinkEl) dom.customJobLinkEl.value = row.jobLink || "";
    if (dom.customJobNotesEl) dom.customJobNotesEl.value = row.notes || "";
    if (dom.customJobReminderEl) dom.customJobReminderEl.value = toDatetimeLocalValue(row.reminderAt);
    if (dom.customJobPanelTitleEl) {
      dom.customJobPanelTitleEl.textContent = duplicate ? "Duplicate Custom Job" : "Edit Custom Job";
    }
    if (dom.customJobPanelHintEl) {
      dom.customJobPanelHintEl.textContent = duplicate
        ? "Create a new custom entry using this job as a template."
        : "Update this custom job while keeping its history and status.";
    }
    if (dom.customJobSaveBtnEl) {
      dom.customJobSaveBtnEl.textContent = duplicate ? "Save Duplicate" : "Update Custom Job";
    }
    setCustomJobPanelOpen(true);
    dom.customJobTitleEl?.focus();
    updateCustomJobWarning();
  }

  async function createCustomJob() {
    if (!savedPageService.isAvailable() || !viewState.currentUser) {
      showToast("Sign in required.", "error");
      return;
    }
    const normalized = normalizeCustomJobInput({
      title: dom.customJobTitleEl?.value,
      company: dom.customJobCompanyEl?.value,
      city: dom.customJobCityEl?.value,
      country: dom.customJobCountryEl?.value,
      workType: dom.customJobWorkTypeEl?.value,
      contractType: dom.customJobContractTypeEl?.value,
      sector: dom.customJobSectorEl?.value,
      profession: dom.customJobProfessionEl?.value,
      jobLink: dom.customJobLinkEl?.value,
      notes: dom.customJobNotesEl?.value,
      reminderAt: dom.customJobReminderEl?.value
    });

    if (!normalized.title || !normalized.company) {
      showToast("Title and Company are required.", "error");
      return;
    }

    try {
      let eventType = "custom_job_created";
      let message = "Custom job saved.";
      if (viewState.customJobMode === "edit") {
        normalized.jobKey = viewState.customJobTargetKey;
        normalized.updatedBy = "manual_edit";
        eventType = "custom_job_updated";
        message = "Custom job updated.";
      } else if (viewState.customJobMode === "duplicate") {
        normalized.updatedBy = "manual_duplicate";
        normalized.keySalt = String(Date.now());
        eventType = "custom_job_duplicated";
        message = "Custom job duplicated.";
      } else {
        normalized.updatedBy = "manual_create";
      }

      const saveResult = await savedPageService.saveJobForUser(
        viewState.currentUser.uid,
        normalized,
        { eventType }
      );
      if (!saveResult.ok) throw new Error(saveResult.error || "Could not save custom job.");
      showToast(message, "success");
      savedDispatch.dispatch({
        type: savedActions.CUSTOM_JOB_MUTATED,
        payload: { at: new Date().toISOString() }
      });
      setCustomJobPanelOpen(false);
      queueActivityPulse(
        String(saveResult?.data?.jobKey || normalized.jobKey || viewState.customJobTargetKey || ""),
        timelineScopeAll
      );
      await refreshActivityLog();
    } catch (err) {
      console.error("Could not save custom job:", err);
      showToast("Could not save custom job.", "error");
    }
  }

  return {
    updateCustomJobWarning,
    setCustomJobAvailability,
    setCustomJobPanelOpen,
    openCustomJobEditor,
    createCustomJob
  };
}
