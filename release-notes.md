## [0.3.003] - 2026-10-02
### Changed

- **Agent skills and MCP config no longer ship inside the container.** The `.agents/` folder was reaching the image because the container build's ignore list never excluded it, and an edit to a skill file was therefore treated as shipped container code -- so editing a skill could overwrite a released version tag. Nothing in the running app reads those files. They are now excluded from the image and treated as non-shipped, matching the rule the AI continuity notes already had.

- **The two container ignore lists can no longer half-register.** The workflow trigger list and the shipped-path list were already asserted equal in length and order; the image ignore list was only spot-checked for a few patterns, which is how `.agents/` slipped through. A new invariant requires every dev and agent tooling directory to be absent from both the image and the shipped-path list.

### Notes

- **No behaviour, data, or upgrade changes.** Nothing you can see looks different, and no saved jobs, settings, or tracked applications are touched.

- **Distribution surfaces are unchanged.** The same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all behave exactly as they did in 0.3.002. The only difference is that agent tooling no longer rides along inside the image.
