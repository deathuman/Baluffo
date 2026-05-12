ALTER TABLE task_runs ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE task_runs ADD COLUMN parent_run_id TEXT NOT NULL DEFAULT '';
ALTER TABLE task_runs ADD COLUMN parent_task_type TEXT NOT NULL DEFAULT '';
ALTER TABLE task_runs ADD COLUMN stage TEXT NOT NULL DEFAULT '';
ALTER TABLE task_runs ADD COLUMN heartbeat_at TEXT NOT NULL DEFAULT '';
ALTER TABLE task_runs ADD COLUMN terminal_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE task_runs ADD COLUMN owner_kind TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_task_runs_type_status ON task_runs(task_type, status);
CREATE INDEX IF NOT EXISTS idx_task_runs_heartbeat ON task_runs(heartbeat_at);

ALTER TABLE task_events ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE task_events ADD COLUMN task_type TEXT NOT NULL DEFAULT '';
ALTER TABLE task_events ADD COLUMN work_item_id TEXT NOT NULL DEFAULT '';
ALTER TABLE task_events ADD COLUMN phase_key TEXT NOT NULL DEFAULT '';
ALTER TABLE task_events ADD COLUMN target TEXT NOT NULL DEFAULT '';
ALTER TABLE task_events ADD COLUMN target_url TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_task_events_type_run_created
    ON task_events(task_type, run_id, created_at);

ALTER TABLE sync_runs ADD COLUMN action TEXT NOT NULL DEFAULT '';
ALTER TABLE sync_runs ADD COLUMN duration_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sync_runs ADD COLUMN snapshot_format TEXT NOT NULL DEFAULT '';
ALTER TABLE sync_runs ADD COLUMN shard_hashes_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE sync_runs ADD COLUMN updated_at TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_sync_runs_action_started ON sync_runs(action, started_at);
