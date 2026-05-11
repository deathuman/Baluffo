CREATE TABLE IF NOT EXISTS source_runs (
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL DEFAULT '',
    finished_at TEXT NOT NULL DEFAULT '',
    duration_ms INTEGER NOT NULL DEFAULT 0,
    fetched_count INTEGER NOT NULL DEFAULT 0,
    kept_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (run_id, source_id)
);

CREATE INDEX IF NOT EXISTS idx_source_runs_run_id ON source_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_source_runs_source_id ON source_runs(source_id);
CREATE INDEX IF NOT EXISTS idx_source_runs_status ON source_runs(status);
