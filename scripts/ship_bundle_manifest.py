"""Shared leaf: the ship-bundle top-level ``src/`` manifest.

Single source of truth for which top-level ``src/`` files the desktop ship bundle
copies into the packaged app (``scripts/build_ship_bundle.py``): each entry is the
bare filename relative to ``src/`` (``.py`` modules plus the one ``.json`` data
file). The repo-health guardrail reads the same constants, so a new top-level
``src/`` file either ships in the bundle or is explicitly declared dev tooling —
defaulting to neither fails precommit (this is how the earlier
``source_registry_data.py`` miss was caught after the fact).

The invariant enforced by ``tools.repo_health.repo_guardrails`` is: every top-level
``src/*.py|*.json`` file must appear in :data:`APP_RUNTIME_SCRIPTS` (it is a runtime
module) **or** in :data:`NON_SHIPPING_TOP_LEVEL_MODULES` (explicit build/container/
dev-audit tooling). Conversely, every manifest entry must still exist on disk, so
renaming/removing a shipped module fails until the manifest is updated in the same
change.
"""

from __future__ import annotations

# Top-level ``src/`` files copied into the packaged app. A new runtime module added
# at the ``src/`` root MUST be registered here or the bundle omits it silently.
APP_RUNTIME_SCRIPTS: tuple[str, ...] = (
    "__init__.py",
    "admin_bridge.py",
    "app_version.py",
    "baluffo_version.py",
    "baluffo_config.py",
    "contracts.py",
    "exceptions.py",
    "fetcher_metrics.py",
    "jobs_fetcher.py",
    "jobs_fetcher_registry.py",
    "pipeline_io.py",
    "source_discovery.py",
    "source_registry_auto_approval.py",
    "source_registry_canonicalize.py",
    "source_registry_data.py",
    "source_registry_identity.py",
    "source_registry_io.py",
    "source_registry_io_journal.py",
    "source_registry_io_load.py",
    "source_registry_io_paths.py",
    "source_registry_io_save.py",
    "source_registry_policy.py",
    "source_registry_state.py",
    "source_registry.py",
    "source_sync_config.py",
    "source_sync_crypto.py",
    "source_sync_runtime.py",
    "source_sync_shard.py",
    "source_sync_snapshot.py",
    "source_sync.py",
    "storage_json_metrics.py",
    "url_hosts.py",
    "storage_metrics.py",
    "local_data_store_attachments.py",
    "local_data_store_availability.py",
    "local_data_store_backup.py",
    "local_data_store_profiles.py",
    "local_data_store_saved_jobs.py",
    "local_data_store_shared.py",
    "local_data_store_tracking.py",
    "local_data_store.py",
    "discovery_seed_catalog.json",
    "python_version_guard.py",
)

# Top-level ``src/`` files that intentionally do NOT ship in the desktop ship bundle:
# build-time seeding, container entrypoints, and dev-audit / benchmark tooling. Keep
# this the narrow exception set. A genuinely runtime module does not belong here.
NON_SHIPPING_TOP_LEVEL_MODULES: tuple[str, ...] = (
    "adapter_audit.py",
    "ashby_registry_refresh.py",
    "container_entrypoint.py",
    "container_gateway.py",
    "container_server.py",
    "dev_admin_supervisor.py",
    "discovery_sanity_benchmark.py",
    "fetch_incremental_sanity_benchmark.py",
    "packaged_desktop_smoke.py",
    "pipeline_audit.py",
    "release_repeatability.py",
    "runtime_seed.py",
    "source_audit_sweep.py",
    "source_sync_checkpoint_tags.py",
)


def every_ship_relevant_top_level_src_file() -> set[str]:
    """Return the union of shipped and explicitly non-shipping ``src/`` filenames."""
    return {*APP_RUNTIME_SCRIPTS, *NON_SHIPPING_TOP_LEVEL_MODULES}
