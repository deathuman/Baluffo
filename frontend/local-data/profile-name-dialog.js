import { presentPopup } from "../shared/ui/popup-presentation.js";

function getDocumentTarget() {
  if (typeof document === "undefined" || !document || !document.body) {
    return null;
  }
  return document;
}

function isFocusableElement(value) {
  return Boolean(value && typeof value === "object" && typeof value.focus === "function");
}

function normalizeExistingProfiles(existingProfiles) {
  const seen = new Set();
  return (Array.isArray(existingProfiles) ? existingProfiles : [])
    .map(profile => {
      const name = String(profile?.name || profile?.displayName || "").trim();
      if (!name) return null;
      const normalizedName = name.toLowerCase();
      if (seen.has(normalizedName)) return null;
      seen.add(normalizedName);
      return {
        ...profile,
        name
      };
    })
    .filter(Boolean);
}

function buildProfilePromptLabel(existingProfiles) {
  const profiles = normalizeExistingProfiles(existingProfiles);
  return profiles.length
    ? `Sign in with a local profile. Existing profiles: ${profiles.map(profile => profile?.name).join(", ")}`
    : "Create a local profile to sign in.";
}

function buildModalButtonClassName(variant, ...names) {
  return ["btn", "back-btn", variant, ...names].filter(Boolean).join(" ");
}

function setModeToggleButtonClassName(button, createMode) {
  if (!button) return;
  button.className = buildModalButtonClassName(
    createMode ? "popup-btn-tertiary" : "popup-btn-secondary",
    "local-auth-dialog-secondary"
  );
}

export async function requestTextInputDialog({
  title = "Sign in",
  description = "",
  label = "Value",
  submitLabel = "Continue",
  existingProfiles = [],
  defaultValue = ""
} = {}) {
  const doc = getDocumentTarget();
  if (!doc) {
    const fallbackPrompt = typeof globalThis?.window?.prompt === "function" ? globalThis.window.prompt.bind(globalThis.window) : null;
    if (!fallbackPrompt) return null;
    const fallbackLabel = [title, description || buildProfilePromptLabel(existingProfiles)].filter(Boolean).join("\n\n");
    const fallbackValue = fallbackPrompt(fallbackLabel, String(defaultValue || ""));
    return typeof fallbackValue === "string" ? fallbackValue : null;
  }
  return new Promise(resolve => {
    const overlay = doc.createElement("div");
    overlay.className = "popup-overlay local-auth-dialog-overlay";
    overlay.dataset.localAuthDialog = "true";

    const panel = doc.createElement("div");
    panel.className = "popup local-auth-dialog";
    panel.setAttribute("role", "dialog");
    panel.setAttribute("aria-modal", "true");
    panel.setAttribute("aria-labelledby", "local-auth-dialog-title");

    const heading = doc.createElement("h2");
    heading.id = "local-auth-dialog-title";
    heading.className = "local-auth-dialog-title";
    heading.textContent = String(title || "Sign in");

    const descriptionEl = doc.createElement("p");
    descriptionEl.className = "local-auth-dialog-description";
    descriptionEl.textContent = String(description || buildProfilePromptLabel(existingProfiles));

    const form = doc.createElement("form");
    form.className = "local-auth-dialog-form";

    const labelEl = doc.createElement("label");
    labelEl.className = "local-auth-dialog-label";
    labelEl.setAttribute("for", "local-auth-name-input");
    labelEl.textContent = String(label || "Value");

    const input = doc.createElement("input");
    input.id = "local-auth-name-input";
    input.className = "local-auth-dialog-input";
    input.name = "profileName";
    input.type = "text";
    input.maxLength = 120;
    input.autocomplete = "off";
    input.required = true;
    input.value = String(defaultValue || "");
    input.placeholder = "Enter profile name";

    const actions = doc.createElement("div");
    actions.className = "local-auth-dialog-actions";

    const cancelBtn = doc.createElement("button");
    cancelBtn.id = "local-auth-cancel-btn";
    cancelBtn.className = buildModalButtonClassName("popup-btn-secondary", "local-auth-dialog-cancel");
    cancelBtn.type = "button";
    cancelBtn.textContent = "Cancel";

    const submitBtn = doc.createElement("button");
    submitBtn.id = "local-auth-submit-btn";
    submitBtn.className = buildModalButtonClassName("popup-btn-primary", "local-auth-dialog-submit");
    submitBtn.type = "submit";
    submitBtn.textContent = String(submitLabel || "Continue");

    const listId = "local-auth-profile-list";
    const profileNames = normalizeExistingProfiles(existingProfiles)
      .map(profile => String(profile?.name || "").trim())
      .filter(Boolean);
    if (profileNames.length) {
      input.setAttribute("list", listId);
      const datalist = doc.createElement("datalist");
      datalist.id = listId;
      profileNames.forEach(name => {
        const option = doc.createElement("option");
        option.value = name;
        datalist.appendChild(option);
      });
      form.appendChild(datalist);
    }

    let finished = false;
    let previousActiveElement = isFocusableElement(doc.activeElement) ? doc.activeElement : null;

    function cleanup(result) {
      if (finished) return;
      finished = true;
      doc.removeEventListener("keydown", onKeyDown, true);
      overlay.remove();
      if (previousActiveElement && doc.contains(previousActiveElement)) {
        try {
          previousActiveElement.focus({ preventScroll: true });
        } catch {
          previousActiveElement.focus();
        }
      }
      resolve(result);
    }

    function onKeyDown(event) {
      if (event.key !== "Escape") return;
      event.preventDefault();
      cleanup(null);
    }

    cancelBtn.addEventListener("click", () => cleanup(null));
    overlay.addEventListener("click", event => {
      if (event.target === overlay) {
        cleanup(null);
      }
    });
    form.addEventListener("submit", event => {
      event.preventDefault();
      cleanup(input.value);
    });
    doc.addEventListener("keydown", onKeyDown, true);

    actions.append(cancelBtn, submitBtn);
    form.append(labelEl, input, actions);
    panel.append(heading, descriptionEl, form);
    overlay.appendChild(panel);
    doc.body.appendChild(overlay);
    presentPopup(overlay, panel, { windowTarget: doc.defaultView });

    const focusInput = () => {
      try {
        input.focus({ preventScroll: true });
      } catch {
        input.focus();
      }
      input.select();
    };
    if (typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(focusInput);
    } else {
      setTimeout(focusInput, 0);
    }
  });
}

export async function requestProfileName({
  title = "Sign in",
  description = "",
  existingProfiles = [],
  defaultValue = ""
} = {}) {
  const profiles = normalizeExistingProfiles(existingProfiles);
  if (!profiles.length) {
    return requestTextInputDialog({
      title,
      description,
      label: "Profile name",
      submitLabel: "Continue",
      existingProfiles,
      defaultValue
    });
  }

  const doc = getDocumentTarget();
  if (!doc) {
    const fallbackPrompt = typeof globalThis?.window?.prompt === "function"
      ? globalThis.window.prompt.bind(globalThis.window)
      : null;
    if (!fallbackPrompt) return null;
    const fallbackLabel = [title, description || buildProfilePromptLabel(profiles)]
      .filter(Boolean)
      .join("\n\n");
    const matchedDefault = profiles.find(
      profile => profile.name.toLowerCase() === String(defaultValue || "").trim().toLowerCase()
    );
    const initialValue = String(
      matchedDefault?.name
      || profiles.find(profile => Boolean(profile?.isCurrent))?.name
      || profiles[0]?.name
      || ""
    );
    const fallbackValue = fallbackPrompt(fallbackLabel, initialValue);
    return typeof fallbackValue === "string" ? fallbackValue : null;
  }

  return new Promise(resolve => {
    const overlay = doc.createElement("div");
    overlay.className = "popup-overlay local-auth-dialog-overlay";
    overlay.dataset.localAuthDialog = "true";

    const panel = doc.createElement("div");
    panel.className = "popup local-auth-dialog";
    panel.setAttribute("role", "dialog");
    panel.setAttribute("aria-modal", "true");
    panel.setAttribute("aria-labelledby", "local-auth-dialog-title");

    const heading = doc.createElement("h2");
    heading.id = "local-auth-dialog-title";
    heading.className = "local-auth-dialog-title";
    heading.textContent = String(title || "Sign in");

    const descriptionEl = doc.createElement("p");
    descriptionEl.className = "local-auth-dialog-description";
    descriptionEl.textContent = String(description || buildProfilePromptLabel(profiles));

    const form = doc.createElement("form");
    form.className = "local-auth-dialog-form";

    const helperEl = doc.createElement("p");
    helperEl.className = "local-auth-dialog-helper";

    const stack = doc.createElement("div");
    stack.className = "local-auth-dialog-stack";

    const existingLabel = doc.createElement("label");
    existingLabel.className = "local-auth-dialog-label";
    existingLabel.setAttribute("for", "local-auth-profile-select");
    existingLabel.textContent = "Choose profile";

    const selectEl = doc.createElement("select");
    selectEl.id = "local-auth-profile-select";
    selectEl.className = "local-auth-dialog-select";
    selectEl.name = "existingProfile";

    profiles.forEach(profile => {
      const option = doc.createElement("option");
      option.value = profile.name;
      option.textContent = profile.name;
      selectEl.appendChild(option);
    });

    const createLabel = doc.createElement("label");
    createLabel.className = "local-auth-dialog-label";
    createLabel.setAttribute("for", "local-auth-name-input");
    createLabel.textContent = "New profile name";

    const inputEl = doc.createElement("input");
    inputEl.id = "local-auth-name-input";
    inputEl.className = "local-auth-dialog-input";
    inputEl.name = "profileName";
    inputEl.type = "text";
    inputEl.maxLength = 120;
    inputEl.autocomplete = "off";
    inputEl.required = true;
    inputEl.placeholder = "Enter new profile name";

    const actions = doc.createElement("div");
    actions.className = "local-auth-dialog-actions";

    const actionCluster = doc.createElement("div");
    actionCluster.className = "local-auth-dialog-action-cluster";

    const createToggleBtn = doc.createElement("button");
    createToggleBtn.id = "local-auth-create-btn";
    createToggleBtn.className = buildModalButtonClassName("popup-btn-secondary", "local-auth-dialog-secondary");
    createToggleBtn.type = "button";

    const cancelBtn = doc.createElement("button");
    cancelBtn.id = "local-auth-cancel-btn";
    cancelBtn.className = buildModalButtonClassName("popup-btn-secondary", "local-auth-dialog-cancel");
    cancelBtn.type = "button";
    cancelBtn.textContent = "Cancel";

    const submitBtn = doc.createElement("button");
    submitBtn.id = "local-auth-submit-btn";
    submitBtn.className = buildModalButtonClassName("popup-btn-primary", "local-auth-dialog-submit");
    submitBtn.type = "submit";

    const requestedDefault = String(defaultValue || "").trim();
    const matchedDefault = profiles.find(
      profile => profile.name.toLowerCase() === requestedDefault.toLowerCase()
    );
    let createMode = Boolean(requestedDefault) && !matchedDefault;
    let selectedProfileName = String(
      matchedDefault?.name
      || profiles.find(profile => Boolean(profile?.isCurrent))?.name
      || profiles[0]?.name
      || ""
    );
    let newProfileName = createMode ? requestedDefault : "";

    if (selectedProfileName) {
      selectEl.value = selectedProfileName;
    }
    inputEl.value = newProfileName;

    let finished = false;
    let previousActiveElement = isFocusableElement(doc.activeElement) ? doc.activeElement : null;

    function cleanup(result) {
      if (finished) return;
      finished = true;
      doc.removeEventListener("keydown", onKeyDown, true);
      overlay.remove();
      if (previousActiveElement && doc.contains(previousActiveElement)) {
        try {
          previousActiveElement.focus({ preventScroll: true });
        } catch {
          previousActiveElement.focus();
        }
      }
      resolve(result);
    }

    function focusCurrentField() {
      const target = createMode ? inputEl : selectEl;
      try {
        target.focus({ preventScroll: true });
      } catch {
        target.focus();
      }
      if (createMode && typeof inputEl.select === "function") {
        inputEl.select();
      }
    }

    function renderMode() {
      stack.replaceChildren();
      if (createMode) {
        actions.className = "local-auth-dialog-actions local-auth-dialog-actions-create-mode";
        setModeToggleButtonClassName(createToggleBtn, true);
        helperEl.textContent = "Create a new local profile for this device.";
        form.prepend(helperEl);
        stack.append(createLabel, inputEl);
        createToggleBtn.textContent = "Use existing profile";
        submitBtn.textContent = "Create profile";
      } else {
        actions.className = "local-auth-dialog-actions";
        setModeToggleButtonClassName(createToggleBtn, false);
        helperEl.remove();
        stack.append(existingLabel, selectEl);
        createToggleBtn.textContent = "Create new profile";
        submitBtn.textContent = "Continue";
      }
      queueMicrotask(focusCurrentField);
    }

    function onKeyDown(event) {
      if (event.key !== "Escape") return;
      event.preventDefault();
      cleanup(null);
    }

    createToggleBtn.addEventListener("click", () => {
      if (createMode) {
        newProfileName = inputEl.value;
      } else {
        selectedProfileName = selectEl.value;
      }
      createMode = !createMode;
      if (createMode) {
        inputEl.value = newProfileName || "";
      } else if (selectedProfileName) {
        selectEl.value = selectedProfileName;
      }
      renderMode();
    });
    cancelBtn.addEventListener("click", () => cleanup(null));
    overlay.addEventListener("click", event => {
      if (event.target === overlay) {
        cleanup(null);
      }
    });
    form.addEventListener("submit", event => {
      event.preventDefault();
      cleanup(createMode ? inputEl.value : selectEl.value);
    });
    doc.addEventListener("keydown", onKeyDown, true);

    actionCluster.append(cancelBtn, submitBtn);
    actions.append(createToggleBtn, actionCluster);
    form.append(helperEl, stack, actions);
    panel.append(heading, descriptionEl, form);
    overlay.appendChild(panel);
    doc.body.appendChild(overlay);
    presentPopup(overlay, panel, { windowTarget: doc.defaultView });
    renderMode();
  });
}

export async function requestProfileLoadFailureAction({
  title = "Sign in",
  description = ""
} = {}) {
  const fallbackDescription = String(
    description
    || "Could not load existing local profiles. Retry to load them again, create a new local profile, or cancel sign-in."
  );
  const doc = getDocumentTarget();
  if (!doc) {
    const fallbackPrompt = typeof globalThis?.window?.prompt === "function"
      ? globalThis.window.prompt.bind(globalThis.window)
      : null;
    if (!fallbackPrompt) return null;
    const fallbackLabel = [
      title,
      fallbackDescription,
      "Type retry to try again, create to create a new profile, or cancel to abort."
    ].filter(Boolean).join("\n\n");
    const fallbackValue = String(fallbackPrompt(fallbackLabel, "retry") || "").trim().toLowerCase();
    if (fallbackValue === "retry") return "retry";
    if (fallbackValue === "create") return "create";
    return null;
  }
  return new Promise(resolve => {
    const overlay = doc.createElement("div");
    overlay.className = "popup-overlay local-auth-dialog-overlay";
    overlay.dataset.localAuthDialog = "true";

    const panel = doc.createElement("div");
    panel.className = "popup local-auth-dialog";
    panel.setAttribute("role", "dialog");
    panel.setAttribute("aria-modal", "true");
    panel.setAttribute("aria-labelledby", "local-auth-dialog-title");

    const heading = doc.createElement("h2");
    heading.id = "local-auth-dialog-title";
    heading.className = "local-auth-dialog-title";
    heading.textContent = String(title || "Sign in");

    const descriptionEl = doc.createElement("p");
    descriptionEl.className = "local-auth-dialog-description";
    descriptionEl.textContent = fallbackDescription;

    const actions = doc.createElement("div");
    actions.className = "local-auth-dialog-actions";

    const cancelBtn = doc.createElement("button");
    cancelBtn.id = "local-auth-cancel-btn";
    cancelBtn.className = buildModalButtonClassName("popup-btn-secondary", "local-auth-dialog-cancel");
    cancelBtn.type = "button";
    cancelBtn.textContent = "Cancel";

    const retryBtn = doc.createElement("button");
    retryBtn.id = "local-auth-retry-btn";
    retryBtn.className = buildModalButtonClassName("popup-btn-secondary", "local-auth-dialog-secondary");
    retryBtn.type = "button";
    retryBtn.textContent = "Retry";

    const createBtn = doc.createElement("button");
    createBtn.id = "local-auth-create-fallback-btn";
    createBtn.className = buildModalButtonClassName("popup-btn-primary", "local-auth-dialog-submit");
    createBtn.type = "button";
    createBtn.textContent = "Create new profile";

    let finished = false;
    let previousActiveElement = isFocusableElement(doc.activeElement) ? doc.activeElement : null;

    function cleanup(result) {
      if (finished) return;
      finished = true;
      doc.removeEventListener("keydown", onKeyDown, true);
      overlay.remove();
      if (previousActiveElement && doc.contains(previousActiveElement)) {
        try {
          previousActiveElement.focus({ preventScroll: true });
        } catch {
          previousActiveElement.focus();
        }
      }
      resolve(result);
    }

    function onKeyDown(event) {
      if (event.key !== "Escape") return;
      event.preventDefault();
      cleanup(null);
    }

    cancelBtn.addEventListener("click", () => cleanup(null));
    retryBtn.addEventListener("click", () => cleanup("retry"));
    createBtn.addEventListener("click", () => cleanup("create"));
    overlay.addEventListener("click", event => {
      if (event.target === overlay) cleanup(null);
    });
    doc.addEventListener("keydown", onKeyDown, true);

    actions.append(cancelBtn, retryBtn, createBtn);
    panel.append(heading, descriptionEl, actions);
    overlay.appendChild(panel);
    doc.body.appendChild(overlay);
    presentPopup(overlay, panel, { windowTarget: doc.defaultView });

    const focusRetry = () => {
      try {
        retryBtn.focus({ preventScroll: true });
      } catch {
        retryBtn.focus();
      }
    };
    if (typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(focusRetry);
    } else {
      setTimeout(focusRetry, 0);
    }
  });
}

export async function requestConfirmationDialog({
  title = "Confirm",
  description = "",
  confirmLabel = "Confirm",
  cancelLabel = "Cancel"
} = {}) {
  const doc = getDocumentTarget();
  if (!doc) {
    const fallbackConfirm = typeof globalThis?.window?.confirm === "function" ? globalThis.window.confirm.bind(globalThis.window) : null;
    if (!fallbackConfirm) return false;
    const fallbackLabel = [title, description].filter(Boolean).join("\n\n");
    return Boolean(fallbackConfirm(fallbackLabel));
  }
  return new Promise(resolve => {
    const overlay = doc.createElement("div");
    overlay.className = "popup-overlay local-auth-dialog-overlay";
    overlay.dataset.localAuthDialog = "true";

    const panel = doc.createElement("div");
    panel.className = "popup local-auth-dialog";
    panel.setAttribute("role", "dialog");
    panel.setAttribute("aria-modal", "true");
    panel.setAttribute("aria-labelledby", "local-auth-dialog-title");

    const heading = doc.createElement("h2");
    heading.id = "local-auth-dialog-title";
    heading.className = "local-auth-dialog-title";
    heading.textContent = String(title || "Confirm");

    const descriptionEl = doc.createElement("p");
    descriptionEl.className = "local-auth-dialog-description";
    descriptionEl.textContent = String(description || "");

    const actions = doc.createElement("div");
    actions.className = "local-auth-dialog-actions";

    const cancelBtn = doc.createElement("button");
    cancelBtn.id = "local-auth-cancel-btn";
    cancelBtn.className = buildModalButtonClassName("popup-btn-secondary", "local-auth-dialog-cancel");
    cancelBtn.type = "button";
    cancelBtn.textContent = String(cancelLabel || "Cancel");

    const confirmBtn = doc.createElement("button");
    confirmBtn.id = "local-auth-confirm-btn";
    confirmBtn.className = buildModalButtonClassName("popup-btn-primary", "local-auth-dialog-submit");
    confirmBtn.type = "button";
    confirmBtn.textContent = String(confirmLabel || "Confirm");

    let finished = false;
    let previousActiveElement = isFocusableElement(doc.activeElement) ? doc.activeElement : null;

    function cleanup(result) {
      if (finished) return;
      finished = true;
      doc.removeEventListener("keydown", onKeyDown, true);
      overlay.remove();
      if (previousActiveElement && doc.contains(previousActiveElement)) {
        try {
          previousActiveElement.focus({ preventScroll: true });
        } catch {
          previousActiveElement.focus();
        }
      }
      resolve(Boolean(result));
    }

    function onKeyDown(event) {
      if (event.key === "Escape") {
        event.preventDefault();
        cleanup(false);
        return;
      }
      if (event.key === "Enter") {
        event.preventDefault();
        cleanup(true);
      }
    }

    cancelBtn.addEventListener("click", () => cleanup(false));
    confirmBtn.addEventListener("click", () => cleanup(true));
    overlay.addEventListener("click", event => {
      if (event.target === overlay) cleanup(false);
    });
    doc.addEventListener("keydown", onKeyDown, true);

    actions.append(cancelBtn, confirmBtn);
    panel.append(heading, descriptionEl, actions);
    overlay.appendChild(panel);
    doc.body.appendChild(overlay);
    presentPopup(overlay, panel, { windowTarget: doc.defaultView });

    const focusConfirm = () => {
      try {
        confirmBtn.focus({ preventScroll: true });
      } catch {
        confirmBtn.focus();
      }
    };
    if (typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(focusConfirm);
    } else {
      setTimeout(focusConfirm, 0);
    }
  });
}
