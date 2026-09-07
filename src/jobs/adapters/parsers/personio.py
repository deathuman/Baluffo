"""Personio XML feed parser.

AI boundary owns: Personio XML feed parsing and raw job row extraction.
AI boundary implement in: this file for Personio XML parsing only; provider dispatch stays in provider_personio/provider runners.
AI boundary search before contracts: Personio provider execution, source discovery probes, and parser tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused Personio parser tests.
"""

from __future__ import annotations

import hashlib
from xml.etree import ElementTree as ET

from src.jobs.game_detection import looks_like_game_job
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

from .location import normalize_location_details, parse_generic_location_fields

# Same needle set the personio provider runner uses for its site_changed
# classification, so probe evidence and pipeline diagnostics agree on the
# same payload. Dead board slugs (e.g. innogames.jobs.personio.de) serve
# Personio's marketing HTML page instead of the workzag XML feed.
_PERSONIO_MARKETING_MARKERS = (
    "<html",
    "hr und lohnbuchhaltung endlich vereint",
    "personio homepage",
)


def looks_like_personio_marketing_html(xml_text: str) -> bool:
    """True when the payload is Personio's marketing HTML page, not a jobs XML feed."""

    text = (xml_text or "").lstrip().lower()
    if text.startswith("<!doctype html"):
        return True
    return any(marker in text for marker in _PERSONIO_MARKETING_MARKERS)


def parse_personio_feed_xml(xml_text: str, source_name: str = "") -> list[RawJob]:
    jobs: list[RawJob] = []
    if looks_like_personio_marketing_html(xml_text):
        # Marketing payload, not XML: return no rows and let the runner's
        # site_changed classification handle the error story.
        return jobs
    root: ET.Element | None = None
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        root = None
    if root is None:
        return jobs
    for posting in root.findall(".//position"):
        title = clean_text(posting.findtext("name"))
        if not title:
            continue
        company = clean_text(posting.findtext("subcompany")) or clean_text(source_name) or "Unknown"
        office = clean_text(posting.findtext("office"))
        department = clean_text(posting.findtext("department"))
        city, country, work_type = parse_generic_location_fields(office)
        location_details = normalize_location_details(office)
        job_link = clean_text(posting.findtext("url"))
        posting_id = clean_text(posting.findtext("id") or posting.get("id"))
        tags = " ".join([department, office])
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append(
            {
                "sourceJobId": f"personio:{source_name}:{posting_id or hashlib.sha1((title + office).encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": clean_text(location_details.get("city")) or city,
                "country": clean_text(location_details.get("country")) or country,
                "workType": work_type or office,
                "contractType": clean_text(posting.findtext("employmentType")),
                "jobLink": job_link,
                "sector": "Game",
                "postedAt": clean_text(posting.findtext("createdAt") or posting.findtext("date")),
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs
