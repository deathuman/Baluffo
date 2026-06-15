from ._helpers import elevato


def test_elevato_static_plugin_extracts_q_loc_listing_rows_without_policy_noise() -> None:
    html = """
    <main>
      <h1>Career</h1>
      <h2><a href="https://qloc.elevato.net/en/translator-proofreader,j,242">Translator / Proofreader</a></h2>
      <a href="https://q-loc.com/privacy-policy/personal-data-processing/">privacy policy</a>
      <a href="https://qloc.elevato.net/en/translator-proofreader,j,242">more >></a>
      <h2><a href="/en/technical-artist,j,240?source=10">Technical Artist</a></h2>
      <p>QLOC offers first-class services for the video game industry.</p>
      <a href="/en/technical-artist,j,240?source=10">more >></a>
      <h2><a href="/en/join-qloc,j,83">Join QLOC!</a></h2>
      <h2><a href="/en/job-offers,j">Show all job offers</a></h2>
    </main>
    """

    rows = elevato.run(
        fetch_text=lambda *_args: html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=["https://qloc.elevato.net/en/"],
        source_row={
            "id": "static:listing_url:https://qloc.elevato.net/en/",
            "name": "QLOC (Sheet)",
            "company": "QLOC",
            "studio": "QLOC",
        },
    )

    assert [(row["title"], row["jobLink"]) for row in rows] == [
        ("Translator / Proofreader", "https://qloc.elevato.net/en/translator-proofreader,j,242"),
        ("Technical Artist", "https://qloc.elevato.net/en/technical-artist,j,240"),
    ]
    assert {row["source"] for row in rows} == {"QLOC (Sheet)"}
    assert all("privacy" not in row["jobLink"] for row in rows)
    assert all(row["title"] != "Join QLOC!" for row in rows)


def test_elevato_static_plugin_treats_expired_detail_as_empty() -> None:
    html = """
    <h1>Oferty pracy</h1>
    <p>Przepraszamy, ale oferta pracy, której szukasz, jest już nieaktualna
    bądź została usunięta z naszego systemu.</p>
    """

    source_row = {
        "id": "static:listing_url:https://qloc.elevato.net/en/",
        "name": "QLOC (Sheet)",
        "company": "QLOC",
    }
    rows = elevato.run(
        fetch_text=lambda *_args: html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=["https://qloc.elevato.net/pl/technical-artist,j,229"],
        source_row=source_row,
    )

    assert rows == []
    assert (source_row.get("_staticPluginMeta") or {}).get(
        "extractorHint"
    ) == "elevato_expired_detail"
