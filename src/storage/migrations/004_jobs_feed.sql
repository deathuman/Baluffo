CREATE TABLE IF NOT EXISTS jobs (
    job_key TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    company TEXT NOT NULL DEFAULT '',
    location TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    first_seen_at TEXT NOT NULL DEFAULT '',
    last_seen_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs(updated_at);

CREATE TABLE IF NOT EXISTS job_sources (
    job_key TEXT NOT NULL REFERENCES jobs(job_key) ON DELETE CASCADE,
    source_id TEXT NOT NULL DEFAULT '',
    source_job_id TEXT NOT NULL DEFAULT '',
    source_name TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (job_key, source_id, source_job_id)
);

CREATE INDEX IF NOT EXISTS idx_job_sources_job_key ON job_sources(job_key);
CREATE INDEX IF NOT EXISTS idx_job_sources_source_id ON job_sources(source_id);
