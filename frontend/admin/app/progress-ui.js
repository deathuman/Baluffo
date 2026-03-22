function clampRatio(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(1, numeric));
}

/**
 * Helper function to ensure progress elements are visible and properly initialized
 */
export function ensureProgressElementVisible(rootEl) {
  if (!rootEl) return false;
  
  // Check if element is hidden and make it visible
  if (rootEl.classList.contains('hidden')) {
    console.debug("[Admin Progress] Unhiding progress element");
    rootEl.classList.remove('hidden');
  }
  
  // Ensure proper ARIA attributes for accessibility
  rootEl.setAttribute('role', 'progressbar');
  rootEl.setAttribute('aria-valuemin', '0');
  rootEl.setAttribute('aria-valuemax', '100');
  
  return true;
}

/**
 * Test function to verify progress bar functionality (development only)
 */
export function testProgressBars() {
  console.debug("[Admin Progress] Testing progress bars...");
  
  const fetcherProgress = document.querySelector('[data-ui="admin-fetcher-progress"]');
  const fetcherBar = document.querySelector('[data-ui="admin-fetcher-progress-bar"]');
  const fetcherLabel = document.querySelector('[data-ui="admin-fetcher-progress-label"]');
  
  if (fetcherProgress && fetcherBar && fetcherLabel) {
    // Test indeterminate progress
    applyAdminTaskProgress(fetcherProgress, fetcherBar, fetcherLabel, {
      active: true,
      determinate: false,
      label: "Test: Indeterminate progress..."
    });
    
    setTimeout(() => {
      // Test determinate progress
      applyAdminTaskProgress(fetcherProgress, fetcherBar, fetcherLabel, {
        active: true,
        determinate: true,
        ratio: 0.65,
        label: "Test: 65% complete"
      });
      
      setTimeout(() => {
        // Reset to hidden
        applyAdminTaskProgress(fetcherProgress, fetcherBar, fetcherLabel, {
          active: false
        });
        console.debug("[Admin Progress] Test completed successfully");
      }, 2000);
    }, 2000);
    
    return true;
  }
  
  console.error("[Admin Progress] Test failed: progress elements not found");
  return false;
}

export function applyAdminTaskProgress(rootEl, barEl, labelEl, view = {}) {
  // Enhanced element validation with debugging
  if (!rootEl) {
    console.warn("[Admin Progress] Root element missing, cannot update progress");
    return;
  }
  if (!barEl) {
    console.warn("[Admin Progress] Progress bar element missing, cannot update progress");
    return;
  }
  if (!labelEl) {
    console.warn("[Admin Progress] Progress label element missing, cannot update progress");
    return;
  }

  // Debug logging for progress state changes
  const active = Boolean(view?.active);
  const determinate = active && Boolean(view?.determinate);
  const ratio = clampRatio(view?.ratio);
  
  if (active) {
    console.debug("[Admin Progress] Showing progress:", { active, determinate, ratio, label: view?.label });
    // Ensure progress element is visible when active
    ensureProgressElementVisible(rootEl);
  }

  rootEl.classList.toggle("hidden", !active);
  rootEl.classList.toggle("indeterminate", active && !determinate);
  rootEl.classList.toggle("determinate", determinate);
  rootEl.classList.toggle("complete", active && ratio >= 1);

  if (!active) {
    labelEl.textContent = "";
    barEl.style.width = "0%";
    rootEl.setAttribute("aria-hidden", "true");
    rootEl.removeAttribute("aria-valuenow");
    rootEl.removeAttribute("aria-valuetext");
    return;
  }

  rootEl.setAttribute("aria-hidden", "false");
  labelEl.textContent = String(view?.label || "");
  if (determinate) {
    const percent = Math.round(ratio * 100);
    barEl.style.width = `${percent}%`;
    rootEl.setAttribute("aria-valuenow", String(percent));
    rootEl.setAttribute("aria-valuetext", String(view?.label || `${percent}%`));
    return;
  }

  barEl.style.width = "36%";
  rootEl.removeAttribute("aria-valuenow");
  rootEl.setAttribute("aria-valuetext", String(view?.label || "In progress"));
}
