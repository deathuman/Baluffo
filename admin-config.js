export const AdminConfig = {
  JOBS_LAST_URL_KEY: "baluffo_jobs_last_url",
  JOBS_FETCHER_COMMAND: "python scripts/jobs_fetcher.py",
  JOBS_FETCHER_TASK_LABEL: "Run jobs fetcher",
  JOBS_FETCH_REPORT_URL: "data/jobs-fetch-report.json",
  JOBS_AUTO_REFRESH_SIGNAL_KEY: "baluffo_jobs_auto_refresh_signal",
  FETCH_REPORT_POLL_INTERVAL_MS: 5000,
  FETCH_REPORT_POLL_TIMEOUT_MS: 10 * 60 * 1000,
  ADMIN_BRIDGE_BASE: "http://127.0.0.1:8878",
  BRIDGE_STATUS_POLL_INTERVAL_MS: 10000
};
