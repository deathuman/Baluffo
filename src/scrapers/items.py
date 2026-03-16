"""Scrapy Items for career job extraction. Output is converted to envelope dict shape."""

from __future__ import annotations

import scrapy


class JobItem(scrapy.Item):
    """Single job row; matches envelope job dict keys for conversion."""

    sourceJobId = scrapy.Field()
    title = scrapy.Field()
    company = scrapy.Field()
    city = scrapy.Field()
    country = scrapy.Field()
    workType = scrapy.Field()
    contractType = scrapy.Field()
    jobLink = scrapy.Field()
    sector = scrapy.Field()
    postedAt = scrapy.Field()
    source = scrapy.Field()
    studio = scrapy.Field()
    adapter = scrapy.Field()
    sourceBundle = scrapy.Field()


def item_to_job_dict(item: JobItem, *, source_name: str, studio: str) -> dict:
    """Convert a loaded JobItem to the envelope job dict shape (for container jobs list)."""
    from src.scrapers.helpers import build_job, clean_text, safe_id

    title = clean_text(item.get("title"))
    company = clean_text(item.get("company")) or studio
    job_link = clean_text(item.get("jobLink"))
    source_job_id = clean_text(item.get("sourceJobId")) or (safe_id(job_link) if job_link else "")
    return build_job(
        source_name=source_name,
        studio=studio,
        title=title,
        company=company,
        job_link=job_link or "",
        source_job_id=source_job_id,
        city=clean_text(item.get("city")),
        country=clean_text(item.get("country")) or "Unknown",
        work_type=clean_text(item.get("workType")),
        contract_type=clean_text(item.get("contractType")),
        posted_at=clean_text(item.get("postedAt")),
    )
