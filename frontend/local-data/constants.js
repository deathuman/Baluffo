export const DB_NAME = "baluffo_jobs_local";
export const DB_VERSION = 2;
export const BACKUP_SCHEMA_VERSION = 3;
export const SESSION_KEY = "baluffo_current_profile_id";
export const PROFILE_KEY = "baluffo_profiles";
// Compatibility surface only. New tracking code uses PIPELINE_PHASES/OUTCOME_STATUSES.
export const APPLICATION_STATUSES = [
  "bookmark",
  "applied",
  "interview_1",
  "interview_2",
  "offer",
  "rejected"
];
