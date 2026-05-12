DROP TABLE IF EXISTS job_sources;
DROP TABLE IF EXISTS jobs;

CREATE TABLE IF NOT EXISTS jobs (
    feed_generation TEXT NOT NULL,
    job_key TEXT NOT NULL,
    row_ordinal INTEGER NOT NULL,
    run_id TEXT NOT NULL DEFAULT '',
    row_hash TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    company TEXT NOT NULL DEFAULT '',
    location TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    first_seen_at TEXT NOT NULL DEFAULT '',
    last_seen_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    schema_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (feed_generation, job_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_generation_ordinal
    ON jobs(feed_generation, row_ordinal);
CREATE INDEX IF NOT EXISTS idx_jobs_generation_status
    ON jobs(feed_generation, status);
CREATE INDEX IF NOT EXISTS idx_jobs_generation_updated
    ON jobs(feed_generation, updated_at);

CREATE TABLE IF NOT EXISTS job_sources (
    feed_generation TEXT NOT NULL,
    job_key TEXT NOT NULL,
    source_ordinal INTEGER NOT NULL,
    source_id TEXT NOT NULL DEFAULT '',
    source_job_id TEXT NOT NULL DEFAULT '',
    source_name TEXT NOT NULL DEFAULT '',
    source_key TEXT NOT NULL DEFAULT '',
    row_hash TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    schema_version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (feed_generation, job_key, source_ordinal),
    FOREIGN KEY (feed_generation, job_key)
        REFERENCES jobs(feed_generation, job_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_sources_generation_job
    ON job_sources(feed_generation, job_key);
CREATE INDEX IF NOT EXISTS idx_job_sources_generation_source
    ON job_sources(feed_generation, source_id, source_job_id);

CREATE TABLE IF NOT EXISTS job_feed_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_generation TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL DEFAULT '',
    row_count INTEGER NOT NULL DEFAULT 0,
    row_hash TEXT NOT NULL DEFAULT '',
    source_count INTEGER NOT NULL DEFAULT 0,
    source_hash TEXT NOT NULL DEFAULT '',
    schema_version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT '',
    published_at TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}'
);

INSERT OR IGNORE INTO job_feed_state(
    id, current_generation, run_id, row_count, row_hash, source_count,
    source_hash, schema_version, updated_at, published_at, payload_json
)
VALUES (1, '', '', 0, '', 0, '', 1, '', '', '{}');
