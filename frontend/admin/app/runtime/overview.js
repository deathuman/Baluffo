import { bindAsyncClick } from "../../../shared/ui/index.js?v=6";
import { UI_TOKENS, ui } from "../../../shared/ui/selectors.js";

const ADMIN_WIPE_BUTTON_SELECTOR = ui(UI_TOKENS.admin.wipeBtn);
const DEFAULT_ADMIN_OVERVIEW_TIMEOUT_MS = 5000;
const ADMIN_OVERVIEW_FULL_REFRESH_DELAY_MS = 150;

function withTimeout(promise, timeoutMs, message) {
  const waitMs = Math.max(0, Number(timeoutMs) || 0);
  if (!waitMs) return promise;
  let timeoutId = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = globalThis.setTimeout(() => {
      reject(new Error(message));
    }, waitMs);
    timeoutId?.unref?.();
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timeoutId !== null) {
      globalThis.clearTimeout(timeoutId);
    }
  });
}

export function createAdminOverviewController({
  refs,
  adminService,
  requestConfirmationDialog,
  showToast,
  getErrorMessage,
  setSourceStatus,
  adminDispatch,
  adminActions,
  formatBytes,
  renderTotalsHtml,
  renderUsersTableHtml,
  renderUsersEmptyHtml,
  overviewTimeoutMs = DEFAULT_ADMIN_OVERVIEW_TIMEOUT_MS
}) {
  let scheduledFullRefreshId = null;
  let lastKnownUsers = [];
  let lastKnownTotals = null;

  function normalizeOverviewDetail(detail) {
    const value = String(detail || "full").trim().toLowerCase();
    return ["summary", "full"].includes(value) ? value : "full";
  }

  function clearScheduledFullRefresh() {
    if (scheduledFullRefreshId !== null) {
      globalThis.clearTimeout(scheduledFullRefreshId);
      scheduledFullRefreshId = null;
    }
  }

  function scheduleFullRefresh() {
    clearScheduledFullRefresh();
    scheduledFullRefreshId = globalThis.setTimeout(() => {
      scheduledFullRefreshId = null;
      refreshOverview({ detail: "full", background: true });
    }, ADMIN_OVERVIEW_FULL_REFRESH_DELAY_MS);
    scheduledFullRefreshId?.unref?.();
  }

  function renderTotals(totals) {
    if (refs.adminTotalsEl) refs.adminTotalsEl.innerHTML = renderTotalsHtml(totals, formatBytes);
  }

  function renderUsersEmpty(message) {
    if (refs.adminUsersListEl) refs.adminUsersListEl.innerHTML = renderUsersEmptyHtml(message);
  }

  async function wipeAccount(uid, name) {
    if (!uid) {
      showToast("Missing user id for wipe.", "error");
      return;
    }
    const confirmed = await requestConfirmationDialog({
      title: "Wipe account data?",
      description: `Wipe account data for ${name || uid}? This cannot be undone.`,
      confirmLabel: "Wipe account"
    });
    if (!confirmed) return;
    try {
      const result = await adminService.wipeAccountAdmin(uid);
      if (!result.ok) throw new Error(result.error || "Could not wipe account.");
      showToast("User account wiped.", "success");
      await refreshOverview();
    } catch (err) {
      showToast(`Could not wipe account: ${getErrorMessage(err)}`, "error");
    }
  }

  function renderUsers(users) {
    if (!refs.adminUsersListEl) return;
    refs.adminUsersListEl.innerHTML = renderUsersTableHtml(users, formatBytes);
    refs.adminUsersListEl.querySelectorAll(ADMIN_WIPE_BUTTON_SELECTOR).forEach(btn => {
      bindAsyncClick(btn, async () => {
        const uid = String(btn.dataset.uid || "");
        const name = String(btn.dataset.name || uid || "this account");
        await wipeAccount(uid, name);
      });
    });
  }

  function normalizeUsers(users) {
    return Array.isArray(users)
      ? users.filter(user => user && typeof user === "object")
      : [];
  }

  function normalizeTotals(totals) {
    return totals && typeof totals === "object" && !Array.isArray(totals)
      ? { ...totals }
      : {};
  }

  function mergeOverviewState(payload) {
    const incomingUsers = normalizeUsers(payload?.users);
    const incomingTotals = normalizeTotals(payload?.totals);
    if (incomingUsers.length) {
      lastKnownUsers = incomingUsers.map(user => ({ ...user }));
      lastKnownTotals = { ...incomingTotals };
      return { users: incomingUsers, totals: incomingTotals };
    }
    const users = lastKnownUsers.length
      ? lastKnownUsers.map(user => ({ ...user }))
      : [];
    const totals = {
      ...(lastKnownTotals || {}),
      ...incomingTotals
    };
    if (users.length) {
      totals.usersCount = Math.max(Number(totals.usersCount || 0), users.length);
    }
    return { users, totals };
  }

  function renderOverview(overview = {}, options = {}) {
    const payload = overview && typeof overview === "object" && !Array.isArray(overview)
      ? overview
      : {};
    const { users, totals } = mergeOverviewState(payload);
    renderTotals(totals);
    if (users.length) {
      renderUsers(users);
    } else {
      renderUsersEmpty("No local users found.");
    }
    if (options?.updateStatus !== false) {
      setSourceStatus(`Loaded ${users.length} user account(s).`);
    }
    adminDispatch.dispatch({ type: adminActions.OVERVIEW_REFRESHED, payload: { at: new Date().toISOString() } });
    return { ...payload, users, totals };
  }

  async function refreshOverview(options = {}) {
    const detail = normalizeOverviewDetail(options?.detail);
    const background = Boolean(options?.background);
    try {
      if (detail === "full") clearScheduledFullRefresh();
      const timeoutMs = Math.max(0, Number(options?.timeoutMs ?? overviewTimeoutMs) || 0);
      const overviewResult = await withTimeout(
        adminService.getAdminOverview({ timeoutMs, detail }),
        timeoutMs,
        "Admin overview request timed out."
      );
      if (!overviewResult.ok) throw new Error(overviewResult.error || "Could not load admin overview.");
      const overview = renderOverview(overviewResult.data || {});
      if (options?.scheduleFullRefresh && detail === "summary") {
        scheduleFullRefresh();
      }
      return overview;
    } catch (err) {
      if (background) {
        setSourceStatus(`Admin overview exact refresh delayed: ${getErrorMessage(err)}`);
        return null;
      }
      renderUsersEmpty("Could not load admin overview.");
      setSourceStatus(`Admin overview unavailable: ${getErrorMessage(err)}`);
      showToast(`Could not load overview: ${getErrorMessage(err)}`, "error");
      return null;
    }
  }

  return {
    renderOverview,
    refreshOverview,
    renderUsersEmpty
  };
}
