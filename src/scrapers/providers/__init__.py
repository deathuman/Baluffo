"""Optional provider modules for career sites that do not use Scrapy (e.g. Jobylon embed)."""

from src.scrapers.providers.jobylon_v1 import extract_jobylon_v1_jobs

__all__ = ["extract_jobylon_v1_jobs"]
