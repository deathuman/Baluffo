import {
  getLocalDataApi,
  authService,
  savedJobsService
} from "../local-data/services.js";

export function isJobsApiReady() {
  return authService.isReady();
}

export const jobsPageService = {
  isAvailable() {
    return Boolean(getLocalDataApi());
  },
  generateJobKey(job) {
    const api = getLocalDataApi();
    if (!api || typeof api.generateJobKey !== "function") return "";
    try {
      return String(api.generateJobKey(job) || "");
    } catch {
      return "";
    }
  }
};

export { authService as jobsAuthService, savedJobsService as jobsSavedJobsService };
