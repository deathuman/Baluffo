"""JSON IO and path defaults for source registry files (thin coordinator).

AI boundary owns: source registry file locations, JSON/JSONL persistence, backups, and storage
metric recording; the public re-export surface and the DATA_DIR root-injection seam stay here.
AI boundary implement in: this coordinator re-exports the implementation leaves (paths, load,
journal, save); `source_registry._sync_io_paths` rebinds DATA_DIR on this module at runtime and
the leaves read it back through this coordinator at call time.
AI boundary search before contracts: source registry policy, source sync modules, bridge registry routes, and registry IO tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused source registry IO tests.
"""

from __future__ import annotations

import os as os
from importlib import reload as _reload

from src import source_registry_io_paths as _paths_mod

# source_registry._reload(_io) re-executes this coordinator on import so BALUFFO_DATA_DIR
# overrides are honored; re-derive the env-sensitive path constants by reloading the
# paths leaf (the single source of truth) before re-importing them.
_reload(_paths_mod)

from src.source_registry_io_journal import (
    _append_json_journal_record as _append_json_journal_record,
)
from src.source_registry_io_journal import (
    _compact_json_journal_if_needed as _compact_json_journal_if_needed,
)
from src.source_registry_io_journal import (
    _json_journal_payload_hash as _json_journal_payload_hash,
)
from src.source_registry_io_journal import (
    _json_journal_record_text as _json_journal_record_text,
)
from src.source_registry_io_journal import (
    _registry_journal_repair_payload as _registry_journal_repair_payload,
)
from src.source_registry_io_journal import (
    _write_text_atomic as _write_text_atomic,
)
from src.source_registry_io_journal import (
    cleanup_runtime_evidence_journals as cleanup_runtime_evidence_journals,
)
from src.source_registry_io_journal import (
    compact_registry_journals as compact_registry_journals,
)
from src.source_registry_io_load import (
    _load_json_array_from_file as _load_json_array_from_file,
)
from src.source_registry_io_load import (
    _load_json_array_from_storage as _load_json_array_from_storage,
)
from src.source_registry_io_load import (
    _load_json_journal_latest_payload as _load_json_journal_latest_payload,
)
from src.source_registry_io_load import (
    _load_json_object_from_storage as _load_json_object_from_storage,
)
from src.source_registry_io_load import (
    load_json_array as load_json_array,
)
from src.source_registry_io_load import (
    load_json_object as load_json_object,
)
from src.source_registry_io_load import (
    load_runtime_evidence as load_runtime_evidence,
)
from src.source_registry_io_load import (
    load_runtime_evidence_array as load_runtime_evidence_array,
)
from src.source_registry_io_load import (
    summarize_json_array_storage as summarize_json_array_storage,
)
from src.source_registry_io_paths import (
    _JSON_JOURNAL_COMPACT_MAX_BYTES as _JSON_JOURNAL_COMPACT_MAX_BYTES,
)
from src.source_registry_io_paths import (
    _JSON_JOURNAL_HARD_MAX_BYTES as _JSON_JOURNAL_HARD_MAX_BYTES,
)
from src.source_registry_io_paths import (
    _WRITE_POLICY_REQUIRED as _WRITE_POLICY_REQUIRED,
)
from src.source_registry_io_paths import (
    _WRITE_RETRY_ATTEMPTS as _WRITE_RETRY_ATTEMPTS,
)
from src.source_registry_io_paths import (
    _WRITE_RETRY_BACKOFF_BASE_S as _WRITE_RETRY_BACKOFF_BASE_S,
)
from src.source_registry_io_paths import (
    ACTIVE_PATH as ACTIVE_PATH,
)
from src.source_registry_io_paths import (
    ACTIVE_SEED_PATH as ACTIVE_SEED_PATH,
)
from src.source_registry_io_paths import (
    APPROVAL_STATE_PATH as APPROVAL_STATE_PATH,
)
from src.source_registry_io_paths import (
    DATA_DIR as DATA_DIR,
)
from src.source_registry_io_paths import (
    DEFAULTS_DIR as DEFAULTS_DIR,
)
from src.source_registry_io_paths import (
    DISCOVERY_CANDIDATES_PATH as DISCOVERY_CANDIDATES_PATH,
)
from src.source_registry_io_paths import (
    DISCOVERY_REPORT_PATH as DISCOVERY_REPORT_PATH,
)
from src.source_registry_io_paths import (
    M5_STRATEGIC_BACKLOG_PATH as M5_STRATEGIC_BACKLOG_PATH,
)
from src.source_registry_io_paths import (
    PENDING_PATH as PENDING_PATH,
)
from src.source_registry_io_paths import (
    PENDING_SEED_PATH as PENDING_SEED_PATH,
)
from src.source_registry_io_paths import (
    REJECTED_PATH as REJECTED_PATH,
)
from src.source_registry_io_paths import (
    TOMBSTONES_PATH as TOMBSTONES_PATH,
)
from src.source_registry_io_paths import (
    URL_PATCH_MANIFEST_PATH as URL_PATCH_MANIFEST_PATH,
)
from src.source_registry_io_paths import (
    _finish_write_failure as _finish_write_failure,
)
from src.source_registry_io_paths import (
    _replace_path_with_retry as _replace_path_with_retry,
)
from src.source_registry_io_paths import (
    ensure_data_dir as ensure_data_dir,
)
from src.source_registry_io_paths import (
    registry_seed_path_for as registry_seed_path_for,
)
from src.source_registry_io_save import (
    _write_json_payload_atomic as _write_json_payload_atomic,
)
from src.source_registry_io_save import (
    save_json_atomic as save_json_atomic,
)
from src.source_registry_io_save import (
    save_registry_state_atomic as save_registry_state_atomic,
)
