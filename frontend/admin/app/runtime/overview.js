import { bindAsyncClick } from "../../../shared/ui/index.js";
import { UI_TOKENS, ui } from "../../../shared/ui/selectors.js";

const ADMIN_WIPE_BUTTON_SELECTOR = ui(UI_TOKENS.admin.wipeBtn);

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
  renderUsersEmptyHtml
}) {
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

  async function refreshOverview() {
    try {
      const overviewResult = await adminService.getAdminOverview();
      if (!overviewResult.ok) throw new Error(overviewResult.error || "Could not load admin overview.");
      const overview = overviewResult.data || {};
      renderTotals(overview?.totals || {});
      const users = Array.isArray(overview?.users) ? overview.users : [];
      if (users.length) {
        renderUsers(users);
      } else {
        renderUsersEmpty("No local users found.");
      }
      setSourceStatus(`Loaded ${users.length} user account(s).`);
      adminDispatch.dispatch({ type: adminActions.OVERVIEW_REFRESHED, payload: { at: new Date().toISOString() } });
    } catch (err) {
      renderUsersEmpty("Could not load admin overview.");
      setSourceStatus(`Admin overview unavailable: ${getErrorMessage(err)}`);
      showToast(`Could not load overview: ${getErrorMessage(err)}`, "error");
    }
  }

  return {
    refreshOverview,
    renderUsersEmpty
  };
}
