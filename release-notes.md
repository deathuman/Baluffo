## [0.3.002] - 2026-10-01
> Housekeeping release. Nothing you can see looks different: this removes
> styling that no page could ever display, and fixes a test that was failing on
> its own about one run in four. It exists so the cleanup below can actually
> reach you instead of sitting on the development branch.

### Changed

- **Removed styling that nothing was ever able to display.** The stylesheets carried rules for 18 classes that no part of the app could put on a page, plus two colour settings nothing referred to. Nothing you saw was affected and nothing you see will change — the pages are pixel-for-pixel identical, checked on all three main screens. Removing them makes the stylesheet smaller and easier to trust: what is left in it is styling that is genuinely in use.

- **Container updates no longer rebuild for tool-only dependency bumps.** Updating a developer-only tool — a linter, a type checker — rebuilt the container image and republished the current version under the same version number. Nothing about the running app changed, but the image behind a version you already had was quietly replaced. Those updates no longer trigger a rebuild. Real changes to what the app runs still do.

### Fixed

- **A check that was failing intermittently for no reason.** One of the Admin interface tests failed roughly one time in four depending on how busy the machine was, which was making it hard to trust the test suite. It was timing its own observation rather than recording it, so it could miss the thing it was looking at. It now records what it saw and checks that, and passed ten consecutive full runs.

### Notes

- **No behaviour, data, or upgrade changes.** No saved jobs, settings, or tracked applications are touched, and there is no migration to run.

- **Distribution surfaces are unchanged.** The same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all behave exactly as they did in 0.3.001. Install and update paths are the same, and the Linux AppImage, portable Windows app, and recovery bundle are all published as before.

- **What changed about how releases are guarded.** Because a stylesheet edit is part of what ships, a change of this kind can no longer land while an already-published version is current — it would replace the image behind a version you have. That is now caught automatically, with a clear explanation, before it reaches you.
