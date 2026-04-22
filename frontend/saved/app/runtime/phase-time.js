import { showToast } from "../../../shared/ui/index.js";

export function createSavedPhaseTime(deps) {
  function needsInterviewTimestamp(phase) {
    const safe = deps.normalizePhase(phase);
    return safe === "interview_1" || safe === "interview_2";
  }

  function toPromptLocalDateTime(value) {
    const parsed = deps.parseIsoDate(value) || new Date();
    const yyyy = parsed.getFullYear();
    const mm = String(parsed.getMonth() + 1).padStart(2, "0");
    const dd = String(parsed.getDate()).padStart(2, "0");
    const hh = String(parsed.getHours()).padStart(2, "0");
    const min = String(parsed.getMinutes()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd} ${hh}:${min}`;
  }

  function parseScheduledTimestampInput(rawValue) {
    const raw = String(rawValue || "").trim();
    if (!raw) return "";

    const compact = raw.replace(/\s+/g, " ");
    if (/^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}$/.test(compact)) {
      const parsed = new Date(compact.replace(" ", "T") + ":00");
      return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString();
    }
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(compact)) {
      const parsed = new Date(`${compact}:00`);
      return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString();
    }

    const parsed = new Date(compact);
    return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString();
  }

  async function requestInterviewTimestamp(phase, previousTimestamp = "") {
    const phaseLabel = deps.phaseLabels[deps.normalizePhase(phase)] || "Interview";
    const promptDefault = toPromptLocalDateTime(previousTimestamp);
    const raw = await deps.requestTextInputDialog({
      title: `${phaseLabel} time`,
      description: "Enter interview time as YYYY-MM-DD HH:MM.",
      label: `${phaseLabel} time`,
      submitLabel: "Save time",
      defaultValue: promptDefault
    });
    if (raw == null) return "";
    const parsed = parseScheduledTimestampInput(raw);
    if (!parsed) {
      showToast("Invalid interview time. Use YYYY-MM-DD HH:MM.", "error");
      return "";
    }
    return parsed;
  }

  return {
    needsInterviewTimestamp,
    toPromptLocalDateTime,
    parseScheduledTimestampInput,
    requestInterviewTimestamp
  };
}
