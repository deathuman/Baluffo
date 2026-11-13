CREATE TABLE IF NOT EXISTS job_feed_generation_stats (
    feed_generation TEXT PRIMARY KEY,
    row_count INTEGER NOT NULL DEFAULT 0,
    source_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT ''
);
