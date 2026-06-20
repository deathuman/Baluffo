from src.source_discovery import probe


def test_personio_malformed_xml_is_probe_failure() -> None:
    def malformed_fetch(_url: str, _timeout: int) -> str:
        return "<workzag-jobs><position>\x08</position></workzag-jobs>"

    ok, count, error = probe.probe_candidate(
        {"adapter": "personio", "feed_url": "https://demo.jobs.personio.de/xml"},
        timeout_s=5,
        fetcher=malformed_fetch,
    )

    assert ok is False
    assert count == 0
    assert "invalid personio XML" in error
