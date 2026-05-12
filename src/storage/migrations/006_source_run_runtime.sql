ALTER TABLE source_runs ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE source_runs ADD COLUMN ordinal INTEGER NOT NULL DEFAULT 0;
ALTER TABLE source_runs ADD COLUMN source_key TEXT NOT NULL DEFAULT '';
ALTER TABLE source_runs ADD COLUMN source_name TEXT NOT NULL DEFAULT '';
ALTER TABLE source_runs ADD COLUMN adapter TEXT NOT NULL DEFAULT '';
ALTER TABLE source_runs ADD COLUMN fetch_strategy TEXT NOT NULL DEFAULT '';
ALTER TABLE source_runs ADD COLUMN studio TEXT NOT NULL DEFAULT '';
ALTER TABLE source_runs ADD COLUMN error TEXT NOT NULL DEFAULT '';
ALTER TABLE source_runs ADD COLUMN low_confidence_dropped INTEGER NOT NULL DEFAULT 0;
ALTER TABLE source_runs ADD COLUMN evidence_ref_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE source_runs ADD COLUMN updated_at TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_source_runs_run_ordinal ON source_runs(run_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_source_runs_key ON source_runs(source_key);
