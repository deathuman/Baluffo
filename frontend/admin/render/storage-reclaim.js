/**
 * @fileoverview Measured-gate health for the Runtime Database Storage panel.
 *
 * The reclaim verdict dates the copy its free-space gate is built from
 * (`measuredStagingStale`) and remembers whether the last rehearsal's own copy
 * fit the gate it started on (`lastRehearsalGateAdequate`). Both would otherwise
 * be fields nobody shows: the panel warns here, before the next rehearsal has to
 * fail to make the point.
 */

const DAY_SECONDS = 24 * 60 * 60;

function formatAgeDays(seconds) {
  const days = Math.max(1, Math.round(seconds / DAY_SECONDS));
  return days === 1 ? "1 day" : `${days} days`;
}

/**
 * The warnings a verdict earns, in reading order: the gate's age first, because
 * a stale gate is a statement about the feed, then a margin the last rehearsal
 * already proved too thin.
 *
 * Only an explicit `true`/`false` warns. `null` means nothing has been measured
 * yet, which is a normal first-run state, not a defect.
 */
export function storageReclaimWarnings(verdict) {
  if (!verdict || typeof verdict !== "object") return [];
  const warnings = [];
  if (verdict.measuredStagingStale === true) {
    const age = Number(verdict.measuredStagingAgeS);
    const measured = Number.isFinite(age) && age > 0
      ? ` measured ${formatAgeDays(age)} ago`
      : "";
    warnings.push({
      code: "stale-anchor",
      text: `The free-space gate comes from a copy${measured}; the feed has grown since. Run a rehearsal to re-measure it.`
    });
  }
  if (verdict.lastRehearsalGateAdequate === false) {
    warnings.push({
      code: "thin-margin",
      text: "The last rehearsal's copy was larger than the free-space gate it started on, so the next reclaim is likely to be refused for space. Free disk space before it starts."
    });
  }
  return warnings;
}

export function renderStorageReclaimHealthHtml(verdict) {
  const warnings = storageReclaimWarnings(verdict);
  if (!warnings.length) return "";
  const items = warnings
    .map(warning => `<p class="admin-storage-reclaim-warning" data-warning="${warning.code}">${warning.text}</p>`)
    .join("");
  return `<div class="admin-storage-reclaim-health-body" data-tone="warning">${items}</div>`;
}
