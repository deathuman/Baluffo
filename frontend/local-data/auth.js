import { requestProfileName } from "./profile-name-dialog.js";

export function createAuthDomain(deps) {
  const {
    listeners,
    getCurrentUser,
    setCurrentUser,
    makeUser,
    readProfiles,
    writeProfiles,
    hashFNV1a,
    sessionKey
  } = deps;

  function notifyAuthChanged() {
    const currentUser = getCurrentUser();
    listeners.forEach(l => {
      if (l.type === "auth") l.callback(currentUser);
    });
  }

  function chooseProfile(existingProfiles, input) {
    const trimmed = (input || "").trim();
    if (!trimmed) return null;

    const existing = existingProfiles.find(p => p.name.toLowerCase() === trimmed.toLowerCase());
    if (existing) return existing;

    const slug = trimmed.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
    const id = `local_${slug || hashFNV1a(trimmed)}`;
    return { id, name: trimmed, email: "" };
  }

  async function signIn() {
    const profiles = readProfiles();
    const input = await requestProfileName({
      title: "Sign in",
      description: profiles.length
        ? `Choose an existing profile or create a new one. Existing: ${profiles.map(profile => profile.name).join(", ")}`
        : "Enter a profile name to create local sign-in.",
      existingProfiles: profiles,
      defaultValue: profiles[0]?.name || ""
    });
    const profile = chooseProfile(profiles, input);
    if (!profile) throw new Error("Sign-in cancelled.");

    const alreadyExists = profiles.some(p => p.id === profile.id);
    if (!alreadyExists) {
      profiles.push(profile);
      writeProfiles(profiles);
    }

    localStorage.setItem(sessionKey, profile.id);
    const nextUser = makeUser(profile);
    setCurrentUser(nextUser);
    notifyAuthChanged();
    return { user: nextUser };
  }

  async function signOut() {
    localStorage.removeItem(sessionKey);
    setCurrentUser(null);
    notifyAuthChanged();
  }

  function onAuthStateChanged(callback) {
    const entry = { type: "auth", callback };
    listeners.add(entry);
    callback(getCurrentUser());
    return () => listeners.delete(entry);
  }

  function bindStorageSync(getStoredSessionUser) {
    window.addEventListener("storage", event => {
      if (event.key === sessionKey) {
        setCurrentUser(getStoredSessionUser());
        notifyAuthChanged();
      }
    });
  }

  return {
    notifyAuthChanged,
    signIn,
    signOut,
    onAuthStateChanged,
    bindStorageSync
  };
}
