## [0.3.011] - 2026-10-06
### Added

- **Twenty-seven more studio boards are now part of what an install watches.** Each was added
  only after the app's real fetch for that board returned live listings, not because the
  address looked right. The biggest are Voodoo's, 2K's and Hangar 13's, alongside a long tail
  of smaller studios.
- **Those boards carry 486 listings between them, and 149 of them are game roles.** The other
  337 are the finance, marketing, legal and support vacancies that mixed companies post
  alongside their games work, and the app still declines to collect them. That split is
  deliberate: a board is worth watching, but only its game openings are worth showing.

### Fixed

- **A board can no longer be added to the registry in a way that quietly collects nothing.**
  The tool that adds boards was writing them without the field that says whether a board is
  active, so they were loaded as inactive: they appeared in the registry, they counted toward
  every total, and no fetch ever looked at them. Nothing in the app reported a problem. The
  field is now set through the same code path the running app uses, and the build refuses to
  accept a board row that would collect nothing.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.010:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
