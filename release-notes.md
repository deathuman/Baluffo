## [0.2.143] - 2026-09-02
### Fixed

- Bridge task lifecycle: history mirror no longer creates owner-less active rows.
  `mirror_history_row` mirrored an unfinished history entry via `start_run` without
  owner fields; if the real worker never terminalized it (crash between history write
  and lifecycle finish), the mirrored row was an unreapable zombie of the same shape
  as the event-only stubs. Mirrored running rows now carry `owner_kind='bridge_thread'`,
  which the startup reaper already treats as stale after restart.
