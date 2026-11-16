/**
 * @fileoverview The Runtime Database Storage reclaim panel: one payload, two scopes.
 *
 * The run is a bridge thread keyed by data directory, so the panel is a view of
 * the route's `storageReclaim` payload, never its owner. Everything here renders
 * from that one object: the status line, the phase, the button states, and the
 * container's modal. `scope` is the only difference between the presentations —
 * a container blocks on the copy, desktop shows it inline.
 *
 * The measured gate's thin/stale warnings come from `storage-reclaim.js`, which
 * the panel renders beside the same status so the two cannot disagree.
 */

export const STORAGE_RECLAIM_ROUTE = "/ops/storage-reclaim";

const PHASE_LABELS = {
  evaluating: "checking the verdict",
  "stopping-retention": "stopping retention",
  "checking-space": "checking free space",
  copying: "copying the database",
  verifying: "verifying the copy",
  "deleting-backup": "deleting the old database",
  complete: "complete"
};

const TERMINAL_COPY = {
  complete: { text: "Complete", tone: "" },
  failed: { text: "Failed", tone: "warning" },
  cancelled: { text: "Stopped", tone: "warning" },
  interrupted: { text: "Interrupted by a restart", tone: "warning" },
  superseded: { text: "Superseded by a database write", tone: "warning" }
};

// Safety blockers refuse a click as well as an automatic start; `cooldown` and
// `auto-disabled` do not, because Run is the explicit override for both.
const SAFETY_BLOCKERS = [
  "no-current-generation",
  "active-task",
  "compaction-running",
  "backup-exists",
  "free-space",
  "store-unavailable",
  "sample-unavailable"
];

const BLOCKER_TEXT = {
  "no-current-generation": "There is no published feed to shrink yet.",
  "active-task": "A task is running. Run refuses until it finishes.",
  "compaction-running": "Compaction is running; it cannot be safely interrupted.",
  "backup-exists": "A rollback copy exists. Roll back or delete it before shrinking again.",
  "free-space": "Free space is below the copy gate.",
  "store-unavailable": "The database could not be read.",
  "sample-unavailable": "The stored history could not be sampled.",
  "auto-disabled": "Automatic starts are not armed; Run is still available.",
  cooldown: "A completed reclaim is inside its anti-thrash window; Run is still available."
};

const AUTO_REASON_TEXT = {
  "rehearsal-inadequate": "the last rehearsal's copy outgrew its gate.",
  "rehearsal-failed": "the last rehearsal failed.",
  "anchor-stale": "the last measurement is older than the feed it describes."
};

/**
 * Why automatic starts are allowed or refused, in one plain sentence. The verdict
 * carries the arm decision so this line never has to read the raw journal.
 */
function reclaimAutoStartLine(verdict, formatBytes) {
  const auto = verdict?.autoStart;
  if (!auto || typeof auto !== "object") return { text: "", tone: "", source: "" };
  const reason = String(auto.reason || "");
  const source = String(auto.source || "");
  const decided = String(auto.decidedAt || "");
  const decidedNote = decided ? ` Decided ${decided.slice(0, 10)}.` : "";
  const staging = Number(auto.evidence?.stagingBytes || 0);
  const measured = staging > 0
    ? ` The recorded copy was ${formatBytes ? formatBytes(staging) : `${staging} B`}.`
    : "";
  if (auto.enabled === true && source === "rehearsal") {
    return {
      text: `Auto-reclaim is armed: the last rehearsal measured an adequate copy.${decidedNote}${measured}`,
      tone: "",
      source
    };
  }
  if (auto.enabled === true) {
    return { text: "Auto-reclaim is armed by BALUFFO_RECLAIM_AUTO_ENABLED.", tone: "", source };
  }
  if (reason === "kill-switch") {
    return {
      text: auto.armed === true
        ? "Auto-reclaim is off: BALUFFO_RECLAIM_AUTO_ENABLED is set to off, even though a rehearsal measured this host."
        : "Auto-reclaim is off: BALUFFO_RECLAIM_AUTO_ENABLED is set to off.",
      tone: "warning",
      source
    };
  }
  if (reason === "not-armed") {
    return {
      text: "Auto-reclaim is not armed yet: a rehearsal has to measure this host first.",
      tone: "warning",
      source
    };
  }
  return {
    text: `Auto-reclaim is not armed: ${AUTO_REASON_TEXT[reason] || "the last rehearsal did not qualify."}${decidedNote}`,
    tone: "warning",
    source
  };
}

export function reclaimPhaseLabel(phase) {
  const key = String(phase || "").trim().toLowerCase();
  return PHASE_LABELS[key] || (key ? key.replace(/-/g, " ") : "");
}

export function reclaimRunning(reclaim) {
  const status = String(reclaim?.status || "").trim();
  return status === "running" || status === "cancelling";
}

function reclaimSafetyBlockers(verdict) {
  const blockers = Array.isArray(verdict?.blockers) ? verdict.blockers : [];
  return blockers.filter(blocker => SAFETY_BLOCKERS.includes(String(blocker)));
}

/**
 * The one line the panel and the modal show. A run's own phase outranks the
 * verdict's blockers, because while it runs nothing else can start anyway.
 */
export function reclaimStatusText(reclaim) {
  if (!reclaim || typeof reclaim !== "object") return { text: "Not measured", tone: "" };
  const status = String(reclaim.status || "idle").trim();
  const rehearsal = String(reclaim.mode || "") === "rehearsal";
  const verb = rehearsal ? "Rehearsal" : "Reclaim";
  if (status === "running") {
    const label = reclaimPhaseLabel(reclaim.phase);
    return { text: `${verb} running${label ? ` — ${label}` : ""}`, tone: "" };
  }
  if (status === "cancelling") return { text: `${verb} stopping after the current step…`, tone: "warning" };
  if (status === "idle" || !status) return { text: "Not started", tone: "" };
  const terminal = TERMINAL_COPY[status];
  if (terminal) {
    const error = String(reclaim.error || "").trim();
    const text = error ? `${verb} ${terminal.text.toLowerCase()}: ${error}` : `${verb} ${terminal.text.toLowerCase()}`;
    return { text, tone: terminal.tone || "" };
  }
  return { text: `${verb} ${status}`, tone: "" };
}

/**
 * Button state for both presentations. `busy` names the request in flight, so a
 * double click cannot start two runs and Stop is not offered against no run.
 */
export function reclaimButtonState(reclaim, { busy = "", verdict = null } = {}) {
  const running = reclaimRunning(reclaim);
  const safety = reclaimSafetyBlockers(verdict);
  const reason = safety.length ? BLOCKER_TEXT[safety[0]] || safety[0] : "";
  const rehearsal = String(reclaim?.mode || "") === "rehearsal";
  return {
    rehearse: {
      disabled: Boolean(busy) || running,
      label: busy === "rehearse" ? "Starting rehearsal…" : running && rehearsal ? "Rehearsing…" : "Rehearse",
      tooltip: "Copies the database once to measure what a reclaim would cost, validates the copy, then discards it. Installs and deletes nothing."
    },
    run: {
      disabled: Boolean(busy) || running || safety.length > 0,
      label: busy === "run" ? "Starting…" : running && !rehearsal ? "Running…" : "Run",
      tooltip: reason
        || "Rebuilds around the published feed and installs the shrunken database. Refused if a write commits mid-copy."
    },
    stop: {
      hidden: !running,
      disabled: Boolean(busy) || String(reclaim?.status || "") === "cancelling" || !String(reclaim?.runId || ""),
      label: String(reclaim?.status || "") === "cancelling" ? "Stopping…" : "Stop",
      tooltip: "Stops after the current database step. The live database is untouched until install."
    }
  };
}

function formatBytesText(value, formatBytes) {
  if (value === undefined || value === null || value === "") return "";
  return formatBytes ? formatBytes(Number(value) || 0) : `${Number(value) || 0} B`;
}

/**
 * The panel body: what is happening, why a start is refused, and the copies
 * involved. Numbers only — no ETA, because a stalled disk would make it a lie.
 */
export function renderStorageReclaimDialogHtml(reclaim, formatBytes) {
  if (!reclaim || typeof reclaim !== "object") {
    return "<p class=\"admin-storage-reclaim-note\">Run <strong>Rehearse</strong> to measure what a shrink would cost before anything is installed.</p>";
  }
  const parts = [];
  const status = reclaimStatusText(reclaim);
  parts.push(`<p class="admin-storage-reclaim-line" data-tone="${status.tone || ""}">${status.text}</p>`);
  const auto = reclaimAutoStartLine(reclaim.verdict || null, formatBytes);
  if (auto.text) {
    parts.push(
      `<p class="admin-storage-reclaim-auto" data-auto-source="${auto.source}" data-tone="${auto.tone}">${auto.text}</p>`
    );
  }
  const verdict = reclaim.verdict || null;
  const safety = reclaimSafetyBlockers(verdict);
  if (safety.length) {
    parts.push(
      `<p class="admin-storage-reclaim-blocked">${BLOCKER_TEXT[safety[0]] || safety[0]}</p>`
    );
  } else if (String(verdict?.verdict || "") === "ok") {
    parts.push("<p class=\"admin-storage-reclaim-note\">The database is not bloated enough to shrink right now.</p>");
  }
  const result = reclaim.result || null;
  if (result) {
    const staging = formatBytesText(result.stagingBytes, formatBytes);
    const gate = formatBytesText(result.measuredGateBytes, formatBytes);
    const detail = gate
      ? `the copy was ${staging} against a ${gate} gate`
      : `the first measured copy was ${staging}`;
    const adequate = result.gateAdequate === false ? " The gate was too thin for it." : "";
    parts.push(`<p class="admin-storage-reclaim-detail">Rehearsal measured: ${detail}.${adequate}</p>`);
  }
  const runId = String(reclaim.runId || "").trim();
  if (runId && reclaimRunning(reclaim)) {
    parts.push(
      "<p class=\"admin-storage-reclaim-note\">The run continues in the bridge if this page closes; only <strong>Stop</strong> cancels it. Progress updates when you refresh.</p>"
    );
  }
  return parts.join("");
}

/**
 * The container modal. Desktop renders nothing here: its payload says `inline`,
 * and the same dialog body is already on the panel.
 */
export function renderStorageReclaimModalHtml(reclaim) {
  if (String(reclaim?.scope || "") !== "modal" || !reclaimRunning(reclaim)) return "";
  const status = reclaimStatusText(reclaim);
  return [
    `<p class="admin-storage-reclaim-modal-status" data-tone="${status.tone || ""}">${status.text}</p>`,
    `<p class="admin-storage-reclaim-modal-note">Closing this tab is safe: the copy runs in the bridge and only Stop cancels it. Fetch starts are refused while it holds.</p>`
  ].join("");
}
