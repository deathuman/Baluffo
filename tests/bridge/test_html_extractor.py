from src.bridge.html_extractor import extract_text_job_signals


def test_extract_text_job_signals_strips_malformed_script_and_style_end_tags() -> None:
    noisy_html = """
        <script>
            apply now engineer apply now artist apply now designer apply now producer
        </script
            ignored>
        <style>
            apply now engineer apply now artist apply now designer apply now producer
        </style
            ignored>
        <main>Careers</main>
    """

    assert extract_text_job_signals(noisy_html, "https://example.com/careers") == []
