## [0.3.027] - 2026-10-28

### Changed

- **An empty board is asked before it is downloaded, even on its first check.** A board with no
  jobs used to be read in full one more time before the fetcher trusted that it still had nothing.
  When the previous read stored the page's version tag, the board now asks "has this changed?" at
  its first check instead: "not modified" is the same evidence a second full read gave, and it
  starts the board's empty window. Boards that publish no tag keep the old behaviour, as do boards
  whose last check failed.

- **Boards split across several listing pages skip unchanged pages too.** A board with more than one
  listing page used to be re-read in full every time, because one page's "not modified" answer could
  not vouch for the pages behind it. Each page's version tag is now remembered separately and the
  fetcher asks every page, skipping the download, parsing and detail pages only when all of them say
  nothing has changed; a page that changed still triggers the full read, and a page that stops
  publishing a tag drops only its own entry. The same change stops a multi-page board from reading
  its later pages over and over inside one run — a five-page board used to read fifteen pages, and
  now reads five.

- **A board's version tag is kept from the ordinary read, so its first re-check can already ask
  "has this changed?"** The tag used to be learned only by the first check after a board's window
  expired, which meant every board waited another full window before an unchanged page could be
  skipped. The read that produces the jobs now stores the tag as well — one for a single-page
  board, one per page for a board split across several. A page that stops publishing a tag drops
  it, and a page a run could not read keeps the tag it had. Boards whose reads go through a custom
  fetcher still learn their tag at the first check, as before.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.026:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
