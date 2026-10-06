"""Personio's XML feed carries no URL, so every parsed row was dropped downstream.

A live 0.3.008 run recorded, for `personio_sources`:

    rawFetched 54   canonicalDropped 54   canonicalKept 0   finalOutput 0
    canonicalDropReasons {missing_job_link: 54}

The adapter looked healthy while it happened: 10 of 12 tenants parsed, and the per-tenant
detail `keptCount` summed to 54, so `run_personio_sources_source` took its
`if jobs or not errors: return jobs` branch and reported success. The rows then died at
canonicalisation, which is why the provider read as **dark** -- 0 of 12 boards collecting --
rather than broken, and why a rate-limit 429 on one tenant looked like the cause.

**The feed simply has no URL element.** A live `<position>` carries `id, office, department,
recruitingCategory, name, jobDescriptions, employmentType, seniority, schedule, keywords,
occupation, occupationCategory, createdAt, yearsOfExperience` and no attributes, so
`posting.findtext("url")` was always `None`.

The public shape is `https://<tenant>.jobs.personio.<tld>/job/<id>` -- visible in that same
run's output as `https://remotecontrol.jobs.personio.com/job/2628436`. These tests pin the
derivation, the precedence when a feed *does* carry a URL, and the refusals.
"""

from __future__ import annotations

from src.jobs.adapters.parsers.personio import parse_personio_feed_xml, personio_posting_url

FEED_URL = "https://altagramgroup.jobs.personio.com/xml"
EXPECTED_HOST = "altagramgroup.jobs.personio.com"


def _feed(*positions: str) -> str:
    body = "".join(positions)
    return f'<?xml version="1.0"?><positions>{body}</positions>'


def _position(*, posting_id: str = "2812740", name: str = "Game Developer", extra: str = "") -> str:
    return (
        f"<position><id>{posting_id}</id><name>{name}</name>"
        f"<office>Berlin</office><department>Engineering</department>{extra}</position>"
    )


# --- the shape of the feed, which is the whole defect ---------------------------------


def test_a_feed_position_has_no_url_element_and_so_yields_no_link():
    """The regression in its own shape: a realistic position, no `<url>`, no link."""
    rows = parse_personio_feed_xml(_feed(_position()), source_name="Altagram", feed_url=FEED_URL)
    assert len(rows) == 1, "the row must parse -- the defect is not in parsing"
    assert rows[0]["jobLink"] == f"https://{EXPECTED_HOST}/job/2812740"


def test_the_link_is_built_on_the_tenants_own_host():
    """Not a personio.com host -- the tenant's, so the link resolves to their board."""
    rows = parse_personio_feed_xml(_feed(_position()), source_name="Altagram", feed_url=FEED_URL)
    assert rows[0]["jobLink"].startswith(f"https://{EXPECTED_HOST}/job/")


def test_a_de_domain_feed_keeps_its_own_host():
    """Personio serves both `.com` and `.de`; deriving from the feed URL handles both."""
    rows = parse_personio_feed_xml(
        _feed(_position(posting_id="99")),
        source_name="Welevel",
        feed_url="https://welevel.jobs.personio.de/xml",
    )
    assert rows[0]["jobLink"] == "https://welevel.jobs.personio.de/job/99"


def test_every_parsed_row_now_carries_a_link():
    """54 of 54 dropped on `missing_job_link`; one surviving row proves the rule."""
    positions = "".join(_position(posting_id=str(i)) for i in range(5))
    rows = parse_personio_feed_xml(_feed(positions), source_name="Travian", feed_url=FEED_URL)
    assert len(rows) == 5
    assert all(row["jobLink"] for row in rows), "no row may reach canonicalisation linkless"


# --- precedence and refusals -----------------------------------------------------------


def test_an_explicit_url_element_wins_over_the_derived_one():
    """A feed that does carry a URL is authoritative; do not overwrite it."""
    extra = "<url>https://boards.example.com/apply/xyz</url>"
    rows = parse_personio_feed_xml(
        _feed(_position(extra=extra)), source_name="Altagram", feed_url=FEED_URL
    )
    assert rows[0]["jobLink"] == "https://boards.example.com/apply/xyz"


def test_no_feed_url_leaves_the_link_empty_rather_than_inventing_one():
    """A caller that does not pass `feed_url` keeps the old behaviour, not a wrong link."""
    rows = parse_personio_feed_xml(_feed(_position()), source_name="Altagram")
    assert rows[0]["jobLink"] == ""


def test_a_position_with_no_id_gets_no_derived_link():
    rows = parse_personio_feed_xml(
        _feed(_position(posting_id="")), source_name="Altagram", feed_url=FEED_URL
    )
    assert rows[0]["jobLink"] == ""


def test_the_url_is_an_id_attribute_when_there_is_no_id_element():
    """`<position id="...">` is the other shape the parser already accepted."""
    xml = (
        '<?xml version="1.0"?><positions><position id="5150">'
        "<name>Game Artist</name><office>Remote</office></position></positions>"
    )
    rows = parse_personio_feed_xml(xml, source_name="Chimera", feed_url=FEED_URL)
    assert rows[0]["jobLink"] == f"https://{EXPECTED_HOST}/job/5150"


# --- the helper on its own -------------------------------------------------------------


def test_posting_url_refuses_without_a_host_or_an_id():
    assert personio_posting_url("", "1") == ""
    assert personio_posting_url(FEED_URL, "") == ""
    assert personio_posting_url("not-a-url", "1") == ""


def test_posting_url_lowercases_the_host_and_ignores_the_feed_path():
    assert personio_posting_url("https://AltagramGroup.Jobs.Personio.com/xml", "7") == (
        "https://altagramgroup.jobs.personio.com/job/7"
    )


def test_marketing_html_still_yields_nothing():
    """The dead-slug guard must not be weakened by the fallback."""
    assert parse_personio_feed_xml("<html><body>Personio</body></html>", feed_url=FEED_URL) == []
