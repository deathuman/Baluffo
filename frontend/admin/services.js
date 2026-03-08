import {
  getLocalDataApi,
  authService,
  adminService
} from "../local-data/services.js";

export function isAdminApiReady() {
  return authService.isReady();
}

export const adminPageService = {
  isAvailable() {
    return Boolean(getLocalDataApi());
  }
};

export { adminService };
