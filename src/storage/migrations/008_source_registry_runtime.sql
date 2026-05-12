CREATE TABLE IF NOT EXISTS source_registry_rows (
    registry_generation TEXT NOT NULL,
    bucket TEXT NOT NULL CHECK (bucket IN ('active', 'pending', 'rejected')),
    source_identity TEXT NOT NULL,
    row_ordinal INTEGER NOT NULL,
    row_hash TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    schema_version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (registry_generation, bucket, source_identity)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_source_registry_rows_generation_bucket_ordinal
    ON source_registry_rows(registry_generation, bucket, row_ordinal);
CREATE INDEX IF NOT EXISTS idx_source_registry_rows_generation_identity
    ON source_registry_rows(registry_generation, source_identity);

CREATE TABLE IF NOT EXISTS source_registry_tombstones (
    registry_generation TEXT NOT NULL,
    tombstone_key TEXT NOT NULL,
    row_ordinal INTEGER NOT NULL,
    row_hash TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    schema_version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (registry_generation, tombstone_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_source_registry_tombstones_generation_ordinal
    ON source_registry_tombstones(registry_generation, row_ordinal);

CREATE TABLE IF NOT EXISTS source_registry_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_generation TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    active_count INTEGER NOT NULL DEFAULT 0,
    pending_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    tombstone_count INTEGER NOT NULL DEFAULT 0,
    state_hash TEXT NOT NULL DEFAULT '',
    tombstone_hash TEXT NOT NULL DEFAULT '',
    schema_version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT '',
    published_at TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}'
);

INSERT OR IGNORE INTO source_registry_state(
    id, current_generation, reason, active_count, pending_count, rejected_count,
    tombstone_count, state_hash, tombstone_hash, schema_version, updated_at,
    published_at, payload_json
)
VALUES (1, '', '', 0, 0, 0, 0, '', '', 1, '', '', '{}');
