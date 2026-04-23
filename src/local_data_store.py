#!/usr/bin/env python3
"""Thin compatibility facade for the file-backed local desktop data store."""

from __future__ import annotations

from typing import Any

from src import local_data_store_attachments as local_data_store_attachments_mod
from src import local_data_store_backup as local_data_store_backup_mod
from src import local_data_store_profiles as local_data_store_profiles_mod
from src import local_data_store_saved_jobs as local_data_store_saved_jobs_mod
from src.local_data_store_shared import (
    APPLICATION_STATUSES,
    LOCK,
    LocalDataPaths,
    _data_url_to_bytes,
    _normalize_iso,
    _read_json,
    _write_atomic,
    _write_json,
    _bytes_to_data_url,
    generate_job_key,
    normalize_application_status,
    normalize_sector_value,
    sanitize_job_url,
    can_transition_phase,
    ensure_store_initialized,
)


class LocalDataStore:
    def __init__(self, paths: LocalDataPaths) -> None:
        self.paths = paths
        ensure_store_initialized(paths)

    def sign_in(self, name: str) -> dict[str, Any]:
        return local_data_store_profiles_mod.sign_in(self.paths, name)

    def sign_out(self) -> None:
        local_data_store_profiles_mod.sign_out(self.paths)

    def get_current_user(self) -> dict[str, Any] | None:
        return local_data_store_profiles_mod.get_current_user(self.paths)

    def list_profiles(self) -> list[dict[str, Any]]:
        return local_data_store_profiles_mod.list_profiles(self.paths)

    def list_saved_jobs(self, uid: str) -> list[dict[str, Any]]:
        return local_data_store_saved_jobs_mod.list_saved_jobs(self.paths, uid)

    def get_saved_job_keys(self, uid: str) -> list[str]:
        return local_data_store_saved_jobs_mod.get_saved_job_keys(self.paths, uid)

    def save_job_for_user(
        self, uid: str, job: dict[str, Any], options: dict[str, Any] | None = None
    ) -> str:
        return local_data_store_saved_jobs_mod.save_job_for_user(self.paths, uid, job, options)

    def remove_saved_job_for_user(self, uid: str, job_key: str) -> None:
        local_data_store_saved_jobs_mod.remove_saved_job_for_user(self.paths, uid, job_key)

    def update_application_status(
        self, uid: str, job_key: str, status: str, options: dict[str, Any] | None = None
    ) -> None:
        local_data_store_saved_jobs_mod.update_application_status(
            self.paths, uid, job_key, status, options
        )

    def update_job_notes(self, uid: str, job_key: str, notes: str) -> None:
        local_data_store_saved_jobs_mod.update_job_notes(self.paths, uid, job_key, notes)

    def list_activity_for_user(self, uid: str, limit: int = 300) -> list[dict[str, Any]]:
        return local_data_store_saved_jobs_mod.list_activity_for_user(self.paths, uid, limit)

    def list_attachments_for_job(self, uid: str, job_key: str) -> list[dict[str, Any]]:
        return local_data_store_attachments_mod.list_attachments_for_job(self.paths, uid, job_key)

    def add_attachment_for_job(
        self, uid: str, job_key: str, file_meta: dict[str, Any], blob_data_url: str
    ) -> str:
        return local_data_store_attachments_mod.add_attachment_for_job(
            self.paths, uid, job_key, file_meta, blob_data_url
        )

    def get_attachment_blob(
        self, uid: str, job_key: str, attachment_id: str
    ) -> tuple[bytes, str, str]:
        return local_data_store_attachments_mod.get_attachment_blob(
            self.paths, uid, job_key, attachment_id
        )

    def delete_attachment_for_job(self, uid: str, job_key: str, attachment_id: str) -> None:
        local_data_store_attachments_mod.delete_attachment_for_job(
            self.paths, uid, job_key, attachment_id
        )

    def export_profile_data(self, uid: str, include_files: bool = False) -> dict[str, Any]:
        return local_data_store_backup_mod.export_profile_data(self.paths, uid, include_files)

    def import_profile_data(self, uid: str, payload: dict[str, Any]) -> dict[str, Any]:
        return local_data_store_backup_mod.import_profile_data(self.paths, uid, payload)

    def get_admin_overview(self) -> dict[str, Any]:
        return local_data_store_backup_mod.get_admin_overview(self.paths)

    def wipe_account_admin(self, uid: str) -> None:
        local_data_store_backup_mod.wipe_account_admin(self.paths, uid)


__all__ = [
    "APPLICATION_STATUSES",
    "LOCK",
    "LocalDataPaths",
    "LocalDataStore",
    "_bytes_to_data_url",
    "_data_url_to_bytes",
    "_normalize_iso",
    "_read_json",
    "_write_atomic",
    "_write_json",
    "can_transition_phase",
    "generate_job_key",
    "normalize_application_status",
    "normalize_sector_value",
    "sanitize_job_url",
]
