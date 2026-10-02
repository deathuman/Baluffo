## [0.3.006] - 2026-10-02
### Changed

- **A tooltip you dismissed with Escape now stays dismissed.** Admin refreshes itself in the background every thirty seconds. If one of those refreshes redrew the button your mouse was resting on, the browser re-checked the hover and the tooltip came back on its own. Escape now holds until you actually move the pointer away, after which hovering behaves exactly as before.

- **The Attachments panel no longer claims to be empty before it has looked.** On first open it read "No attachments yet." when nothing had actually been loaded. It now says the list has not been loaded and points at Refresh.

- **Agent skills and MCP configuration no longer ship inside the container image.** That folder was reaching the image, so editing a skill was treated as a change to the app itself — which could quietly replace the image behind a version you already had. Nothing in the running app reads those files.

- **The two container ignore lists can no longer half-register.** The rule that decides when a change counts as "shipped" and the rule that decides what goes into the image are now asserted to agree, so a directory cannot be counted as shipped while being absent from the image.

### Added

- **You can now reload a job's attachments on demand.** The Attachments panel has a Refresh button beside Upload, matching the one already on the History tab.

### Fixed

- **A failed attachment load no longer looks like an empty one.** If the app could not reach your saved files, the panel used to say "No attachments yet." as though you had never added anything. It now reports that the list could not be loaded and offers to try again, so a problem is never disguised as a fact about your data.

### Notes

- **Nothing stored is touched.** No saved jobs, settings, or tracked applications are read or written differently, and there is no upgrade step. Attachment storage, the backup format, and attachment counts are all exactly as before.

- **Some intermittent test failures were fixed at their source.** One check was measuring every pause in the whole system instead of its own retry delay; another let two test runs started by accident delete each other's working files. These affected development confidence, not anything you can see.

- **A developer-only report script no longer advertises an option it never had.** It accepted a time filter, ignored it, and documented it in its usage line. The option is gone and the usage line now matches what the script does.

- **Distribution surfaces are unchanged.** The same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all behave exactly as they did in 0.3.002. The only user-visible difference is that a tooltip you dismissed with Escape stays dismissed, and that a failed attachment load now says so.
