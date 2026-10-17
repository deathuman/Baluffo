## [0.3.020] - 2026-10-17
### Added

- **Openings that come from Dover and Studio Gloam are collected directly.** Dover has its own provider
  adapter instead of being read through a guessed careers page, and Studio Gloam's openings are listed
  directly, so boards on either no longer depend on discovery finding them first.
- **Saving a job, or starting to track one, now says so immediately.** The control shows the action as
  pending until the answer comes back, instead of appearing to do nothing while the request is in flight.

### Changed

- **A provider response can no longer retire a studio's static board list unless it actually delivered
  openings.** An empty or failed answer used to be enough to drop the fallback list, which could hide
  jobs that were still live on the static board.
- **Saved-job state is fenced by the session that asked for it.** A request still in flight when a
  profile changed could land in the wrong account's list. Attachments, availability summaries, tracking
  reverts, lifecycle overlays, feedback acknowledgements and the custom-jobs refresh are now all bound
  to the session that started them, so a late answer is ignored rather than applied.
- **Admin review actions no longer apply twice.** A control clicked again while the first request was
  open, or a status card whose answer arrived after the view had moved on, is treated as the same
  action instead of a new one.
- **The runtime database reclaims space instead of growing without bound.** Old generations of the
  runtime store are cleaned up and their lifetime is bounded.
- **Job titles read from Teamtailor keep their real text** instead of showing the slug id in front of
  the title.
- **The portable build carries the public-suffix list it needs.** Without it, the build treated some
  studio addresses as a single domain, so a few job links were judged against the wrong site.
- **For contributors:** the Lint workflow now runs for a pull request against any branch, so a stacked
  pull request gets the full gate rather than only the workflow check, and a documentation link into
  the git-ignored run-evidence directory is no longer reported as broken.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.019:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
