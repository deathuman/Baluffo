CREATE TABLE IF NOT EXISTS task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES task_runs(run_id) ON DELETE CASCADE,
    level TEXT NOT NULL DEFAULT 'info',
    event TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    fields_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_task_events_run_id ON task_events(run_id);
CREATE INDEX IF NOT EXISTS idx_task_events_created ON task_events(created_at);
