## Adapter plugin inventory (2026-03-16)

This note captures the initial inventory for the **adapter plugin framework** rollout.

### Biggest / highest-churn candidates

- **`src/jobs/adapters/static.py` (~802 LOC)**  
  Monolith that includes:
  - static studio page crawling (listing + detail heuristics)
  - detail-link heuristics / filtering
  - per-source concurrency tuning
  - HTML parsing + fallback title synthesis
  - scrapy-runner integration (`scrapy_static`)

### Existing “family-like” clusters (good early plugin families)

- **`src/jobs/adapters/provider_api.py` (~356 LOC)**  
  Already segmented by provider, each with its own registry key and parsing strategy:
  - `greenhouse` (boards JSON)
  - `teamtailor` (listing HTML + detail parsing)
  - `lever`, `smartrecruiters`, `workable` (similar registry-driven flow)
  - `ashby`, `personio` (provider-specific flows)

- **`src/jobs/adapters/social.py` (~269 LOC)**  
  Already segmented by provider:
  - reddit
  - x
  - mastodon

### Initial extraction slices (first wave)

To prove the architecture with minimal risk, start with the provider family that is already naturally sliced:

- **Family**: `provider_api`
  - **Plugins (3–5 slices)**: `greenhouse`, `teamtailor`, `lever`, `workable`, `smartrecruiters`

Then validate framework generality on a second family:

- **Family**: `social`
  - **Plugins**: `reddit`, `x` (optionally `mastodon`)

`static` remains the largest monolith; it becomes a follow-up family once the framework is validated in production paths.

