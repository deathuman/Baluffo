"""Static and scrapy adapters."""

from __future__ import annotations

from src.jobs.adapters import static_scrapy as _static_scrapy
from src.jobs.adapters.plugins.static import register_static_plugins
from src.jobs.adapters.static_helpers import (
    extract_rendered_card_jobs as _extract_rendered_card_jobs,
)
from src.jobs.adapters.static_helpers import (
    process_detail_html as _process_detail_html,
)

from . import static_sources as static_sources_mod

register_static_plugins()

extract_rendered_card_jobs = _extract_rendered_card_jobs
process_detail_html = _process_detail_html
run_scrapy_static_source = _static_scrapy.run_scrapy_static_source
static_source_shard = static_sources_mod.static_source_shard
run_static_source_entry_source = static_sources_mod.run_static_source_entry_source
static_source_name_for_registry_row = static_sources_mod.static_source_name_for_registry_row
build_static_source_loaders = static_sources_mod.build_static_source_loaders


run_static_studio_pages_source = static_sources_mod.run_static_studio_pages_source


run_static_studio_pages_a_i_source = static_sources_mod.build_static_shard_runner("a_i")
run_static_studio_pages_j_r_source = static_sources_mod.build_static_shard_runner("j_r")
run_static_studio_pages_s_z_source = static_sources_mod.build_static_shard_runner("s_z")
