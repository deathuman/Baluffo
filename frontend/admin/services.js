import {
  getValidatedLocalDataApi,
  authService,
  adminService
} from "../local-data/services.js";

export function isAdminApiReady() {
  return authService.isReady();
}

export const adminPageService = {
  isAvailable() {
    return Boolean(getValidatedLocalDataApi("admin local data runtime"));
  }
};

export { adminService };
