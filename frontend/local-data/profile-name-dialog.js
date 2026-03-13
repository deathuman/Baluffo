function getDocumentTarget() {
  if (typeof document === "undefined" || !document || !document.body) {
    return null;
  }
  return document;
}

function isFocusableElement(value) {
  return Boolean(value && typeof value === "object" && typeof value.focus === "function");
}

function buildProfilePromptLabel(existingProfiles) {
  const profiles = Array.isArray(existingProfiles) ? existingProfiles : [];
  return profiles.length
    ? `Sign in with a local profile. Existing profiles: ${profiles.map(profile => profile?.name).filter(Boolean).join(", ")}`
    : "Create a local profile to sign in.";
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
    cancelBtn.className = "btn back-btn local-auth-dialog-cancel";
    cancelBtn.type = "button";
    cancelBtn.textContent = "Cancel";

    const submitBtn = doc.createElement("button");
    submitBtn.id = "local-auth-submit-btn";
    submitBtn.className = "btn back-btn local-auth-dialog-submit";
    submitBtn.type = "submit";
    submitBtn.textContent = String(submitLabel || "Continue");

    const listId = "local-auth-profile-list";
    const profileNames = (Array.isArray(existingProfiles) ? existingProfiles : [])
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
  return requestTextInputDialog({
    title,
    description,
    label: "Profile name",
    submitLabel: "Continue",
    existingProfiles,
    defaultValue
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
    cancelBtn.className = "btn back-btn local-auth-dialog-cancel";
    cancelBtn.type = "button";
    cancelBtn.textContent = String(cancelLabel || "Cancel");

    const confirmBtn = doc.createElement("button");
    confirmBtn.id = "local-auth-confirm-btn";
    confirmBtn.className = "btn back-btn local-auth-dialog-submit";
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
