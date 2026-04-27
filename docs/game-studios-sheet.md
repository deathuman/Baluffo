# Game Studios Sheet (directory) contract

The **game studios directory sheet** is a single Google Sheet used by source discovery to list studios and their careers page URLs. It is **not** the same as the job-listing Google Sheets used by the pipeline (e.g. `google_sheets`, `google_sheets_1er2oaxo`).

- **Config:** `GAME_STUDIOS_SHEET_ID` and `GAME_STUDIOS_SHEET_GID` in [src/source_discovery/config.py](../src/source_discovery/config.py).
- **Fetch:** CSV is fetched via export/gviz/pub URLs in [src/source_discovery/sheet_directory.py](../src/source_discovery/sheet_directory.py).
- **Parse:** [parse_game_studio_sheet_csv](../src/source_discovery/sheet_directory.py) expects the following.
- **Audit pilot:** Set `sheetDirectory.activeAuditEnabled=true` in source-discovery config to write/reuse `data/sheet-directory-discovery-audit.json` while preserving the same provider/static/failure output rows.

## Expected sheet structure

- **Header row:** Must contain (after normalisation):
  - A **studio/company** column: header contains `studio` or `company`.
  - A **link** column: header contains `link`, `url`, or `website`.
  - A **roles/openings** column: header contains `roles`, `hiring`, or `openings` (used for `openingsFlag`).
- **Data rows:** Each row must have non-empty studio and a valid `http://` or `https://` link.
- **Output:** Each parsed row is a dict with keys: `studio`, `careersUrl`, `openingsFlag` (e.g. `yes`, `no`, `speculative`, `unknown`).

If the header is not found or required columns are missing, the parser returns an empty list. As of the Game Studios Sheet Health plan, a parse failure (non-empty CSV but zero rows parsed) is reported in the discovery report as a failure with `adapter: "sheet_directory"` and `stage: "directory_parse"`.

## Changing the sheet layout

If you rename columns or change the header structure, update:

1. [parse_game_studio_sheet_csv](../src/source_discovery/sheet_directory.py) (header detection and column indices).
2. This doc.
3. Tests in [tests/source_discovery/test_directory_sources.py](../tests/source_discovery/test_directory_sources.py) that assert on parsed keys (e.g. `test_parse_game_studio_sheet_csv_returns_expected_keys`, `test_parse_game_studio_sheet_csv_handles_metadata_rows_and_openings_flag`).
