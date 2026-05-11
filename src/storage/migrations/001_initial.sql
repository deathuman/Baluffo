CREATE TABLE IF NOT EXISTS storage_authority_modes (
    surface TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    canonical_key TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    name TEXT NOT NULL DEFAULT '',
    adapter TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_sources_status ON sources(status);

CREATE TABLE IF NOT EXISTS source_health (
    source_id TEXT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT '',
    last_run_id TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    last_success_at TEXT NOT NULL DEFAULT '',
    last_failure_at TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS task_runs (
    run_id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL,
    owner_pid INTEGER,
    started_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    finished_at TEXT NOT NULL DEFAULT '',
    progress_json TEXT NOT NULL DEFAULT '{}',
    summary_json TEXT NOT NULL DEFAULT '{}',
    error TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_task_runs_status ON task_runs(status);
CREATE INDEX IF NOT EXISTS idx_task_runs_updated ON task_runs(updated_at);

CREATE TABLE IF NOT EXISTS sync_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT '',
    finished_at TEXT NOT NULL DEFAULT '',
    size_bytes INTEGER NOT NULL DEFAULT 0,
    max_snapshot_size_bytes INTEGER NOT NULL DEFAULT 0,
    size_warning INTEGER NOT NULL DEFAULT 0,
    shard_count INTEGER NOT NULL DEFAULT 0,
    changed_shard_count INTEGER NOT NULL DEFAULT 0,
    shards_pushed_bytes INTEGER NOT NULL DEFAULT 0,
    manifest_size_bytes INTEGER NOT NULL DEFAULT 0,
    shard_cap_bytes INTEGER NOT NULL DEFAULT 0,
    snapshot_schema_version INTEGER NOT NULL DEFAULT 0,
    summary_json TEXT NOT NULL DEFAULT '{}',
    error TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_started ON sync_runs(started_at);
