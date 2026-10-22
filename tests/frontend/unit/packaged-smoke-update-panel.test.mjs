import test from "node:test";
import assert from "node:assert/strict";
import { assertNoDesktopUpdatePanelDivergence, openDesktopUpdatePanel } from "../helpers/packaged-smoke-shared.mjs";

const TOGGLE_SELECTOR = "#desktop-update-toggle-btn";
const PANEL_SELECTOR = "#desktop-update-panel";

// Minimal page double: the toggle is what decides whether the panel opens, the
// panel reports visibility, and evaluate() reports the DOM snapshot the helper
// attaches to its failures.
function createUpdatePanelPage({
  opensOnClicks = [1],
  expandedWhileHidden = false,
  expandedAfterClick = false,
  openInitially = false,
  diagnosticsRows = [],
  diagnosticsUnreadable = false
} = {}) {
  const page = {
    clicks: 0,
    panelOpen: openInitially,
    async waitForTimeout() {},
    async evaluate(...args) {
      // evaluate(fn) reads the DOM snapshot; evaluate(fn, payload) fetches rows.
      if (args.length > 1) {
        if (diagnosticsUnreadable) throw new Error("startup metrics request failed");
        return diagnosticsRows;
      }
      return {
        togglePresent: true,
        toggleHidden: false,
        toggleExpanded: page.panelOpen || expandedWhileHidden || (expandedAfterClick && page.clicks > 0)
          ? "true"
          : "false",
        toggleState: page.panelOpen ? "up_to_date" : "unknown",
        panelPresent: true,
        panelHidden: !page.panelOpen,
        panelState: page.panelOpen ? "up_to_date" : "unknown",
        panelTitle: page.panelOpen ? "Baluffo is up to date" : "Desktop updates",
        panelBody: ""
      };
    },
    locator(selector) {
      if (selector === PANEL_SELECTOR) {
        return {
          isVisible: async () => page.panelOpen,
          waitFor: async ({ state }) => {
            if (state === "visible" && !page.panelOpen) throw new Error("panel not visible");
          }
        };
      }
      assert.equal(selector, TOGGLE_SELECTOR);
      return {
        waitFor: async () => {},
        isEnabled: async () => true,
        click: async () => {
          page.clicks += 1;
          if (opensOnClicks.includes(page.clicks)) {
            page.panelOpen = true;
          }
        }
      };
    }
  };
  return page;
}

test("openDesktopUpdatePanel clicks the toggle once when the panel opens", async () => {
  const page = createUpdatePanelPage();
  const opened = await openDesktopUpdatePanel(page, { timeout: 10 });
  assert.equal(opened.attempts, 1);
  assert.equal(page.clicks, 1);
  assert.equal(opened.state.toggleExpanded, "true");
  assert.equal(opened.state.panelTitle, "Baluffo is up to date");
});

test("openDesktopUpdatePanel leaves an already open panel alone", async () => {
  const page = createUpdatePanelPage({ openInitially: true });
  const opened = await openDesktopUpdatePanel(page, { timeout: 10 });
  assert.equal(opened.attempts, 1);
  assert.equal(page.clicks, 0, "clicking an open panel would close it");
});

test("openDesktopUpdatePanel retries a lost toggle click", async () => {
  const page = createUpdatePanelPage({ opensOnClicks: [2] });
  const opened = await openDesktopUpdatePanel(page, { timeout: 10 });
  assert.equal(opened.attempts, 2);
  assert.equal(page.clicks, 2);
  assert.equal(page.panelOpen, true);
});

test("openDesktopUpdatePanel reports the DOM state and the page's own click diagnostics when every click is lost", async () => {
  const page = createUpdatePanelPage({
    opensOnClicks: [],
    diagnosticsRows: [
      { event: "jobs_desktop_update_click_dispatched", clickId: "b-1", targetMatchesBoundToggle: false },
      { event: "jobs_desktop_update_click_dropped", clickId: "b-1", targetMatchesBoundToggle: false }
    ]
  });
  await assert.rejects(
    () => openDesktopUpdatePanel(page, { attempts: 3, timeout: 10 }),
    error => {
      assert.match(error.message, /did not open after 3 toggle click\(s\)/);
      assert.match(error.message, /"toggleState":"unknown"/);
      assert.match(error.message, /"panelHidden":true/);
      assert.match(error.message, /Click diagnostics: .*jobs_desktop_update_click_dropped/);
      assert.match(error.message, /"targetMatchesBoundToggle":false/);
      return true;
    }
  );
  assert.equal(page.clicks, 3);
});

test("openDesktopUpdatePanel refuses a panel that starts hidden while its toggle claims it open", async () => {
  const page = createUpdatePanelPage({ opensOnClicks: [], expandedWhileHidden: true });
  await assert.rejects(
    () => openDesktopUpdatePanel(page, { attempts: 3, timeout: 10 }),
    /started hidden while its toggle reported the panel open, so the first click would close it/
  );
  assert.equal(page.clicks, 0, "clicking a panel the toggle already claims open would close it");
});

test("openDesktopUpdatePanel allows a coherent startup auto-open", async () => {
  const page = createUpdatePanelPage({ openInitially: true });
  const opened = await openDesktopUpdatePanel(page, { timeout: 10 });
  assert.equal(opened.startedHidden, false);
  assert.equal(page.clicks, 0);
});

test("openDesktopUpdatePanel does not retry a toggle that claims an open panel while it is hidden", async () => {
  const page = createUpdatePanelPage({ opensOnClicks: [], expandedAfterClick: true });
  await assert.rejects(
    () => openDesktopUpdatePanel(page, { attempts: 3, timeout: 10 }),
    /stayed hidden while its toggle reported aria-expanded="true"/
  );
  assert.equal(page.clicks, 1);
});

const INSTALLED_ROW = { event: "jobs_desktop_update_click_diagnostics_installed", clickId: "" };

test("assertNoDesktopUpdatePanelDivergence accepts a recorded run with no divergence", async () => {
  const page = createUpdatePanelPage({ diagnosticsRows: [INSTALLED_ROW] });
  const result = await assertNoDesktopUpdatePanelDivergence(page, { timeoutMs: 0 });
  assert.deepEqual(result, { diagnosticsEvents: 1, divergences: 0 });
});

test("assertNoDesktopUpdatePanelDivergence fails on a panel-open-but-hidden metric", async () => {
  const page = createUpdatePanelPage({
    diagnosticsRows: [
      INSTALLED_ROW,
      {
        event: "jobs_desktop_update_panel_state_diverged",
        reason: "status_not_exposed",
        panelOpen: true,
        panelHidden: true,
        availability: "checking"
      }
    ]
  });
  await assert.rejects(
    () => assertNoDesktopUpdatePanelDivergence(page, { timeoutMs: 0 }),
    error => {
      assert.match(error.message, /reported itself open while it rendered hidden/);
      assert.match(error.message, /"reason":"status_not_exposed"/);
      assert.match(error.message, /"panelHidden":true/);
      return true;
    }
  );
});

test("assertNoDesktopUpdatePanelDivergence fails when the recorder never installed", async () => {
  const page = createUpdatePanelPage({ diagnosticsRows: [] });
  await assert.rejects(
    () => assertNoDesktopUpdatePanelDivergence(page, { timeoutMs: 0 }),
    /never installed, so a clean divergence count here would be an absence of data/
  );
});

test("assertNoDesktopUpdatePanelDivergence fails when the metrics cannot be read", async () => {
  const page = createUpdatePanelPage({ diagnosticsUnreadable: true });
  await assert.rejects(
    () => assertNoDesktopUpdatePanelDivergence(page, { timeoutMs: 0 }),
    /click diagnostics could not be read/
  );
});
