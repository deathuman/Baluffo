/**
 * @typedef {Object} CanonicalJob
 * @property {string|number} id - Unique identifier.
 * @property {string} title - Job title.
 * @property {string} company - Company name.
 * @property {string} city - City location.
 * @property {string} country - Country name.
 * @property {string} workType - Remote, Hybrid, or Onsite.
 * @property {string} contractType - Full-time, Internship, Temporary, or Unknown.
 * @property {string} jobLink - URL to apply.
 * @property {string} sector - Game or Tech.
 * @property {string} profession - Normalized profession key.
 * @property {string} companyType - Game or Tech.
 * @property {string} description - Fallback description.
 * @property {string} source - Scraper or board name.
 * @property {string} sourceJobId - Originating ATS ID.
 * @property {string} fetchedAt - ISO 8601 timestamp.
 * @property {string} postedAt - ISO 8601 timestamp (optional).
 * @property {string} status - e.g. active, likely_removed, archived.
 * @property {string} firstSeenAt - ISO 8601 timestamp.
 * @property {string} lastSeenAt - ISO 8601 timestamp.
 * @property {string} removedAt - ISO 8601 timestamp (optional).
 * @property {string} [lifecycleEvent] - e.g. reappeared, preserved.
 * @property {string} [lifecycleReason] - e.g. source_failed, source_skipped.
 * @property {string} dedupKey - Content hash for deduplication.
 * @property {number} qualityScore - [0-100] scale.
 * @property {number} focusScore - [0-100] scale.
 * @property {number} sourceBundleCount - Number of collapsed rows.
 * @property {Array<Object>} sourceBundle - Raw ATS payloads.
 * @property {string} adapter - Python adapter module.
 * @property {string} studio - Pipeline studio group.
 * @property {number} [freshnessAgeDays] - Derived field (calculated on frontend).
 * @property {number} [freshnessScore] - Derived field (calculated on frontend).
 * @property {string} [freshnessSource] - Derived field (calculated on frontend).
 */

/**
 * @typedef {Object} SavedJobSnapshot
 * @property {string} title
 * @property {string} company
 * @property {string} sector
 * @property {string} companyType
 * @property {string} city
 * @property {string} country
 * @property {string} workType
 * @property {string} contractType
 * @property {string} jobLink
 */

/**
 * @typedef {Object} SavedJob
 * @property {string} jobKey - Primary key (job_hash).
 * @property {SavedJobSnapshot} snapshot - Flattened CanonicalJob subset.
 * @property {string} createdAt - ISO 8601 timestamp.
 * @property {string} updatedAt - ISO 8601 timestamp.
 * @property {string} status - Application stage.
 * @property {string} notes - User text.
 * @property {boolean} isCustom - True if manually created.
 * @property {string} customSourceLabel - Display name for custom rows.
 * @property {string} [reminderAt] - ISO 8601 timestamp (optional).
 * @property {number} attachments - Count of local files.
 */

// Keep this file as a module for JSDoc import() consumers without exporting a dead symbol.
export {};
