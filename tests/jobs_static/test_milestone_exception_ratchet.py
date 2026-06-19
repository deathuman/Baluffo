import pytest

from src.jobs.adapters.plugins.static import milestone

from ._helpers import jf


def test_milestone_iframe_fetch_fallback_does_not_swallow_unexpected_runtime_bug() -> None:
    source_row = {
        "name": "Milestone (Sheet)",
        "studio": "Milestone",
        "company": "Milestone",
        "id": "static:listing_url:https://milestone.it/careers",
    }
    listing_html = """
        <script src="https://cezanneondemand.intervieweb.it/integration/announces_js.php?lang=en&utype=0&k=abc123&LAC=milestone&d=milestone.it&annType=published&view=list&defgroup=name&gnavenable=1&desc=1&typeView=large"></script>
        """

    def broken_iframe_fetch(url: str, _timeout: int) -> str:
        if url == "https://milestone.it/careers":
            return listing_html
        raise RuntimeError("unexpected milestone iframe fetch bug")

    with pytest.raises(RuntimeError, match="unexpected milestone iframe fetch bug"):
        milestone.run(
            fetch_text=broken_iframe_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            pages=["https://milestone.it/careers"],
            source_row=source_row,
            parse_jobpostings_from_html=jf.parse_jobpostings_from_html,
        )

    assert "_staticPluginMeta" not in source_row
