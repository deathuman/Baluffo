from __future__ import annotations

from src.jobs.adapters.static_detail_heuristics import process_detail_link


def test_static_detail_fallback_uses_single_greenhouse_apply_target_as_job_link() -> None:
    detail = "https://wargaming.com/en/careers/vacancy_3159807_prague"
    apply_target = "https://boards.greenhouse.io/wargamingen/jobs/6933589?gh_jid=6933589"
    html = f"""
        <html>
          <head><title>Render Engineer (Unannounced project)</title></head>
          <body>
            <h1>Render Engineer (Unannounced project)</h1>
            <p>Location: Prague, Czechia</p>
            <p>Responsibilities include rendering systems, tools, and engine work.</p>
            <p>Requirements include experience with C++, graphics APIs, and game engines.</p>
            <a class="button apply" href="{apply_target}">Apply Now</a>
            <a href="https://boards.greenhouse.io/wargamingenoa/jobs/9999999">
              Submit Your Application
            </a>
          </body>
        </html>
    """

    result = process_detail_link(
        detail=detail,
        detail_title="Render Engineer (Unannounced project)",
        source_started=0,
        static_source_time_budget_s=10,
        fetch_html_cached=lambda *_args, **_kwargs: (html, False),
        timeout_s=5,
        detail_retries=0,
        company="Wargaming",
        source_name="static:listing_url:https://wargaming.com/en/careers/",
        source={"studio": "Wargaming"},
        ignored_link_titles=set(),
    )

    rows = result["rows"]
    assert len(rows) == 1
    assert rows[0]["jobLink"] == apply_target
    assert rows[0]["sourceJobId"].startswith(
        "static:static:listing_url:https://wargaming.com/en/careers/:"
    )


def test_static_detail_fallback_keeps_detail_url_when_greenhouse_apply_targets_are_ambiguous() -> (
    None
):
    detail = "https://studio.example/careers/vacancy_1"
    html = """
        <html>
          <head><title>Senior Artist</title></head>
          <body>
            <h1>Senior Artist</h1>
            <p>Location: Berlin, Germany</p>
            <p>Responsibilities include art production and team collaboration.</p>
            <p>Requirements include strong portfolio and game production experience.</p>
            <a class="apply" href="https://boards.greenhouse.io/studio/jobs/111">Apply Now</a>
            <a class="apply" href="https://boards.greenhouse.io/studio/jobs/222">Apply Now</a>
          </body>
        </html>
    """

    result = process_detail_link(
        detail=detail,
        detail_title="Senior Artist",
        source_started=0,
        static_source_time_budget_s=10,
        fetch_html_cached=lambda *_args, **_kwargs: (html, False),
        timeout_s=5,
        detail_retries=0,
        company="Studio",
        source_name="static:listing_url:https://studio.example/careers/",
        source={"studio": "Studio"},
        ignored_link_titles=set(),
    )

    assert result["rows"][0]["jobLink"] == detail
