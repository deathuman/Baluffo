function bindWindowResize(windowObject, handler) {
  windowObject.addEventListener("resize", handler);
}

function isCheckboxTarget(windowObject, target) {
  if (!target || target.type !== "checkbox") return false;
  const htmlInputCtor = windowObject.HTMLInputElement || globalThis.HTMLInputElement;
  return htmlInputCtor ? target instanceof htmlInputCtor : true;
}

export function createJobsEventsController({
  dom,
  pageState,
  runtimeState,
  filtersController,
  authController,
  rememberCurrentJobsUrl,
  navigateDesktopPage,
  openAdminPageFromJobs,
  refreshJobsNow,
  triggerJobsPipelineRun,
  handleAutoRefreshSignalValue,
  applyFiltersAndRender,
  bindUi,
  bindAsyncClick,
  bindHandlersMap,
  debounce,
  jobsAutoRefreshSignalKey,
  jobsListDelegation,
  goToPage,
  windowObject = globalThis.window,
  documentObject = globalThis.document
}) {
  function setupJobsListDelegation() {
    if (typeof jobsListDelegation === "function") {
      jobsListDelegation();
    }
  }

  function bindCoreEvents() {
    if (runtimeState.coreEventsBound) return;
    runtimeState.coreEventsBound = true;
    const clickHandlers = new Map([
      [dom.savedJobsBtn, () => {
        rememberCurrentJobsUrl();
        navigateDesktopPage("saved.html");
      }],
      [dom.countryPickerClearBtn, () => {
        pageState.filters.countries = [];
        filtersController.applyStateToFilters();
        applyFiltersAndRender({ resetPage: true });
      }],
      [dom.quickFiltersResetBtn, () => {
        filtersController.resetQuickFilterPreferences();
      }]
    ]);
    bindHandlersMap(clickHandlers);

    bindAsyncClick(dom.authSignInBtn, () => authController.signInUser());
    bindAsyncClick(dom.authSignOutBtn, () => authController.signOutUser());
    bindAsyncClick(dom.adminPageBtn, openAdminPageFromJobs);
    bindAsyncClick(dom.refreshJobsBtn, () => refreshJobsNow({ manual: true }));
    bindAsyncClick(dom.jobsPipelineRunBtn, triggerJobsPipelineRun);
  }

  function recalculateItemsPerPage() {
    if (!dom.jobsList) return false;

    const top = dom.jobsList.getBoundingClientRect().top;
    const viewportHeight = windowObject.innerHeight;
    const reservedSpace = 140;
    const availableHeight = Math.max(260, viewportHeight - top - reservedSpace);
    const rowHeight = windowObject.innerWidth <= 900 ? 136 : 52;
    const next = Math.max(4, Math.min(25, Math.floor(availableHeight / rowHeight)));

    if (next !== pageState.itemsPerPage) {
      pageState.itemsPerPage = next;
      return true;
    }
    return false;
  }

  function enableKeyboardNav() {
    documentObject.addEventListener("keydown", event => {
      const isField = ["INPUT", "SELECT", "TEXTAREA"].includes(event.target.tagName) || event.target.isContentEditable;
      if (isField) return;

      if (event.key === "ArrowLeft" && pageState.currentPage > 1) {
        goToPage(pageState.currentPage - 1);
      } else if (event.key === "ArrowRight") {
        const totalPages = Math.ceil(runtimeState.filteredJobs.length / pageState.itemsPerPage);
        if (pageState.currentPage < totalPages) {
          goToPage(pageState.currentPage + 1);
        }
      }
    });
  }

  function bindEvents() {
    if (runtimeState.secondaryEventsBound) return;
    runtimeState.secondaryEventsBound = true;
    [
      dom.workTypeFilter,
      dom.lifecycleStatusFilter,
      dom.countryFilter,
      dom.cityFilter,
      dom.sectorFilter,
      dom.professionFilter,
      dom.sortFilter
    ].forEach(element => bindUi(element, "change", () => filtersController.onFilterChange()));

    if (dom.cityFilter && typeof filtersController.materializeCityOptions === "function") {
      bindUi(dom.cityFilter, "pointerdown", () => filtersController.materializeCityOptions());
      bindUi(dom.cityFilter, "focus", () => filtersController.materializeCityOptions());
    }

    if (dom.professionSearchFilter) {
      dom.professionSearchFilter.addEventListener("input", () => {
        filtersController.renderProfessionOptions(dom.professionSearchFilter.value);
      });
    }

    if (dom.countryPickerBtn) {
      dom.countryPickerBtn.addEventListener("click", event => {
        event.stopPropagation();
        filtersController.toggleCountryPickerPanel();
      });
    }
    if (dom.countryPickerSearch) {
      dom.countryPickerSearch.addEventListener("input", () => {
        filtersController.renderCountryPickerOptions(dom.countryPickerSearch.value);
      });
    }
    if (dom.countryPickerOptions) {
      dom.countryPickerOptions.addEventListener("change", event => {
        const target = event.target;
        if (!isCheckboxTarget(windowObject, target)) return;
        const current = new Set(pageState.filters.countries || []);
        if (target.checked) current.add(target.value);
        else current.delete(target.value);
        pageState.filters.countries = Array.from(current);
        filtersController.applyStateToFilters();
        applyFiltersAndRender({ resetPage: true });
      });
    }

    const handleDocumentPointerDown = event => {
      if (dom.countryPickerPanel && !dom.countryPickerPanel.classList.contains("hidden")) {
        const clickedInsidePanel = dom.countryPickerPanel.contains(event.target);
        const clickedTrigger = dom.countryPickerBtn && dom.countryPickerBtn.contains(event.target);
        if (!clickedInsidePanel && !clickedTrigger) {
          filtersController.closeCountryPickerPanel();
        }
      }

      if (dom.quickFiltersPanel && !dom.quickFiltersPanel.classList.contains("hidden")) {
        const clickedInsideQuickPanel = dom.quickFiltersPanel.contains(event.target);
        const clickedQuickTrigger = dom.customizeQuickFiltersBtn && dom.customizeQuickFiltersBtn.contains(event.target);
        if (!clickedInsideQuickPanel && !clickedQuickTrigger) {
          filtersController.closeQuickFiltersPanel();
        }
      }
    };
    documentObject.addEventListener("pointerdown", handleDocumentPointerDown, true);
    documentObject.addEventListener("mousedown", handleDocumentPointerDown, true);
    documentObject.addEventListener("keydown", event => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      filtersController.closeCountryPickerPanel();
      filtersController.closeQuickFiltersPanel();
    }, true);

    if (dom.searchFilter) {
      bindUi(dom.searchFilter, "input", debounce(() => {
        filtersController.onFilterChange();
      }, 180));
    }

    bindWindowResize(windowObject, debounce(() => {
      if (!runtimeState.allJobs.length) return;
      const changed = recalculateItemsPerPage();
      if (changed) {
        applyFiltersAndRender({ resetPage: false });
      }
    }, 150));

    if (dom.quickActionsEl) {
      dom.quickActionsEl.addEventListener("click", event => {
        const button = event.target.closest(".quick-btn");
        if (!button) return;
        const quick = button.dataset.quick;
        if (!quick) return;
        filtersController.applyQuickFilter(quick);
        filtersController.applyStateToFilters();
        applyFiltersAndRender({ resetPage: true });
      });
    }

    if (dom.customizeQuickFiltersBtn) {
      dom.customizeQuickFiltersBtn.addEventListener("click", event => {
        event.stopPropagation();
        filtersController.toggleQuickFiltersPanel();
      });
    }

    if (dom.quickFiltersOptionsEl) {
      dom.quickFiltersOptionsEl.addEventListener("change", event => {
        const target = event.target;
        if (!isCheckboxTarget(windowObject, target)) return;
        const { quick } = target.dataset;
        if (!quick) return;
        filtersController.setQuickFilterVisibility(quick, target.checked);
      });
    }

    windowObject.addEventListener("storage", event => {
      if (event.key !== jobsAutoRefreshSignalKey) return;
      if (!event.newValue) return;
      handleAutoRefreshSignalValue(event.newValue);
    });

    enableKeyboardNav();
  }

  return {
    setupJobsListDelegation,
    bindCoreEvents,
    bindEvents,
    recalculateItemsPerPage
  };
}
