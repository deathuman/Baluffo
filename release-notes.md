## [0.2.131] - 2026-08-15

> Jobs-quality rollup: the Track A fixes from the 2026-08-12 entry-validation
> audit (static title noise, country normalization, country acceptance contract
> v3) plus runtime-artifact gitignore hygiene.

### Fixed

- Static parser noise-title classification extended (CSS/JS code payloads, nav/UI
  tokens, zero-width/control characters, country-code-as-title) and wired into the
  static listing append paths; `scripts/jobs_artifact_quality_gate.py` gains
  `parserNoiseTitleLeaks` so the previously shipped raw-title contamination
  classes are gate-visible. Regression tests in
  `test_static_parser_noise_titles.py`.
- Country normalization: `normalize_country` maps non-ISO US state codes to `US`
  and non-Latin garbage to `Unknown`; `sanitize_country_text` 2-letter passthrough
  is ASCII-gated. Regression tests in `test_country_normalization.py`.

### Changed

- `data/contracts/country_acceptance.json` v3: real ISO codes added to the
  acceptance contract (MY, TR, HK, LT, VN, QA, CY, UA, CI, EE, RO, BG, ID, PK, AZ,
  GE, MD, MK, PH, GT, PA, ...); `docs/DATA_CONTRACT.md` documents country
  normalization.
- Gitignore hygiene: `data/*.jsonl.gz` rows sidecar and `data/*.lock` feed
  reconciliation lock are runtime artifacts and stay untracked.

### Notes

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
