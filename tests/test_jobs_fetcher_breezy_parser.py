from src import jobs_fetcher as jf


def test_parse_breezy_jobs_html_handles_root_relative_nested_position_links() -> None:
    rows = jf.parse_breezy_jobs_html(
        """
        <section class="positions">
          <li class="position transition">
            <ul class="position-wrap">
              <li class="position-details">
                <a href="/p/83122cb1eac0-client-engineering-manager" title="Apply">
                  <h2>Client Engineering Manager</h2>
                  <ul class="meta">
                    <li class="location"><i></i><span>Seattle, US</span></li>
                    <li class="type"><i></i><span class="polygot">%LABEL_POSITION_TYPE_FULL_TIME%</span></li>
                  </ul>
                  <button>Apply</button>
                </a>
              </li>
            </ul>
          </li>
        </section>
        """,
        "https://flowplay-llc.breezy.hr/",
        "Flowplay",
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "Client Engineering Manager"
    assert rows[0]["company"] == "Flowplay"
    assert rows[0]["jobLink"] == (
        "https://flowplay-llc.breezy.hr/p/83122cb1eac0-client-engineering-manager"
    )
    assert "Apply" not in rows[0]["title"]
