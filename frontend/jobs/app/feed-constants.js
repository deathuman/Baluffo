/**
 * Jobs feed — shared constants.
 *
 * Split out of ``feed.js``; that module stays the thin coordinator owning
 * the public entrypoints.
 *
 * @module feed-constants
 */

const BOOTSTRAP_AUTO_START_KEY = "baluffo_jobs_bootstrap_auto_started";
const BOOTSTRAP_LAUNCH_COLD_START_HANDLED_KEY = "baluffo_jobs_bootstrap_launch_cold_start_handled";
const LOCAL_FEED_MISSING_MESSAGE = "Local jobs feed is missing or unreadable. Retry quick refresh or run Update jobs to rebuild the full feed.";
const JOBS_FULL_FEED_SYNC_DELAY_MS = 1200;
const EMPTY_TITLE_FEED_MESSAGE = "Jobs feed contained no displayable positions. Retry quick refresh or run Update jobs to rebuild the full feed.";
const FIRST_RUN_BOOTSTRAP_STATUS = "Refreshing first-run sheet jobs. This can take several minutes...";
const FIRST_RUN_BOOTSTRAP_CONFIRMING_STATUS = "Confirming first-run sheet refresh started...";
const FIRST_RUN_BOOTSTRAP_UNCONFIRMED_MESSAGE = "Could not confirm first-run sheet refresh started. Retry quick refresh or open Admin.";
const FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS = 90 * 1000;
const FIRST_RUN_BOOTSTRAP_NOTICE = Object.freeze({
  title: "Preparing first-run jobs",
  body: "Baluffo is fetching the starter Google Sheets job feed. The first refresh can take several minutes. You can keep this window open; jobs will appear automatically when the refresh finishes.",
  primaryLabel: "Got it"
});

export {
  BOOTSTRAP_AUTO_START_KEY,
  BOOTSTRAP_LAUNCH_COLD_START_HANDLED_KEY,
  LOCAL_FEED_MISSING_MESSAGE,
  JOBS_FULL_FEED_SYNC_DELAY_MS,
  EMPTY_TITLE_FEED_MESSAGE,
  FIRST_RUN_BOOTSTRAP_STATUS,
  FIRST_RUN_BOOTSTRAP_CONFIRMING_STATUS,
  FIRST_RUN_BOOTSTRAP_UNCONFIRMED_MESSAGE,
  FIRST_RUN_BOOTSTRAP_PROGRESS_STALE_MS,
  FIRST_RUN_BOOTSTRAP_NOTICE
};
