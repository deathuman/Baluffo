## [0.3.067] - 2026-11-18

### Fixed

- Admin stops reporting a WAL checkpoint that already succeeded. The storage warning followed the last maintenance record, and only a later maintenance attempt rewrote that record, so a database whose write-ahead log had been emptied by a vacuum, a rebuild or a bulk-delete checkpoint kept showing "WAL checkpoint could not finish" next to a log that no longer existed. The warning now follows the log's live size: it clears once the file is back under the checkpoint threshold, and it stays up when either the size or the threshold is missing, because an unmeasured file is not a fixed one.
- The storage panel quotes the sizes currently on disk instead of the sizes recorded before the last attempt, naming that attempt's own reading only when the two disagree. A resolved checkpoint no longer reads as a gigabyte log beside the database it was folded into.
- A checkpoint that completes outside the scheduled maintenance pass now updates the maintenance record: the bulk-delete and bulk-insert passes, a backup, an install, and a vacuum that leaves the log empty. The panel can no longer show a busy attempt that already finished, and the record carries the age of that attempt, shown as "just now" for anything under a minute instead of rounding to "0m ago".

### Notes

- This release changes how Admin reports storage maintenance, not what the app serves. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all stay as they are, and no job feed, saved job, note, or sync behaves differently.
