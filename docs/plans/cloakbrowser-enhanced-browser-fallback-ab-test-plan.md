# CloakBrowser Enhanced Browser Fallback A/B Test Plan

> - **Status:** Active plan, deferred experiment
> - **Use this when:** evaluating CloakBrowser as an optional enhanced browser fallback for blocked, challenged, or JavaScript-heavy career pages
> - **Canonical for:** proposed A/B test design, adoption guardrails, measurement criteria, and packaging/licensing constraints for CloakBrowser evaluation
> - **Not canonical for:** current browser fallback runtime behavior, implemented portable packaging, approved vendor licensing, or default scraping policy
> - **Then inspect:** [`../scraping-pipeline.md`](../scraping-pipeline.md), [`optional-playwright-browser-download-plan.md`](optional-playwright-browser-download-plan.md), [`../adapter-plugin-inventory.md`](../adapter-plugin-inventory.md), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-16

## Summary

A/B test [CloakBrowser](https://github.com/CloakHQ/CloakBrowser) as an off-by-default "enhanced browser fallback" for Baluffo sources that already qualify for browser recovery. The goal is to measure whether CloakBrowser recovers materially more official career pages than stock Playwright without increasing false positives, package size, legal risk, or maintenance burden.

CloakBrowser must not become a default dependency or bundled portable browser payload during the spike. Its wrapper is MIT-licensed, but its compiled browser binary has a separate [Binary License](https://github.com/CloakHQ/CloakBrowser/blob/main/BINARY-LICENSE.md) that forbids redistribution, bundling, repackaging, or embedding in a third-party product without separate OEM/SaaS licensing. Any experiment should use a developer-installed or user-initiated official download path only.

## Key Changes

- **Experiment scope:** Compare stock Playwright against CloakBrowser only in existing browser fallback lanes: admin source check, source-discovery browser recovery, static listing fallback, and the Scrapy-Playwright browser queue. Do not route provider APIs, normal HTTP fetches, dedup, no-openings evidence, saved jobs, or sync through CloakBrowser.
- **Input set:** Build a capped test corpus from `data/jobs-browser-fallback-queue.json`, discovery `browserRecoveryCandidates`, and latest fetch-report sources classified as `blocked_or_challenge`, `anti_bot_or_challenge`, `rate_limited`, `js_required`, or `needs_review` with `browserFallbackRecommended=true`.
- **Safety filter:** Include only public official career pages or first-party company careers domains. Exclude login-gated pages, account creation, government/health/financial authentication surfaces, third-party aggregators, social pages, and any source that would require CAPTCHA solving or bypassing access controls.
- **Fetcher design:** Keep the existing `(html, error)` browser helper contract for the spike. Add a separate experimental CloakBrowser fetch helper or script that returns rendered HTML and normalized errors without changing production route contracts.
- **Scrapy lane:** Test Scrapy-Playwright separately by pointing launch options at the CloakBrowser binary only if the official CloakBrowser install exposes a compatible executable path and required launch args. If compatibility is unclear, record that as a spike result instead of adapting production runner behavior.
- **Packaging policy:** Do not embed CloakBrowser, add it to release assets, or add it to main runtime requirements. If later productized, expose it as an optional enhanced fallback download with user-facing tradeoffs, separate from the stock Playwright payload.
- **Operational policy:** Keep existing robots, delay, concurrency, browser fallback cooldown, and queue caps. CloakBrowser is not a proxy-rotation, CAPTCHA-solving, or unsupported-site bypass feature.

## Measurement And Acceptance

- **Primary metric:** Additional sources with real kept jobs recovered by CloakBrowser over stock Playwright on the same input corpus.
- **Secondary metrics:** Reduction in `blocked_or_challenge`, `anti_bot_or_challenge`, `js_required`, and `needs_review`; per-source runtime; process memory; browser cache size; installer/download size; and retryable failure rate.
- **Quality checks:** Recovered rows must pass existing canonicalization, source identity, no-openings, and dedup gates. No hidden/script/template empty-state text should be promoted to legitimate no-openings evidence.
- **Adoption threshold:** Consider productization only if CloakBrowser recovers at least five additional real official-career sources or improves blocked/review recovery by at least 10% on the sampled corpus, with no material increase in false positives or noisy canonical drops.
- **Decision output:** Write `_out/cloakbrowser-ab/report.md` and `_out/cloakbrowser-ab/results.json` summarizing input sources, control vs experiment outcomes, recovered examples, failures, package/cache cost, licensing notes, and a clear adopt/defer/reject recommendation.

## Test Plan

- **Unit tests:** Cover experimental browser-provider selection, missing CloakBrowser dependency behavior, error normalization, no-secret diagnostics, and corpus filtering for allowed source categories.
- **A/B harness tests:** Mock stock Playwright and CloakBrowser fetchers to verify identical source input, paired result recording, timeout handling, and report generation.
- **Pipeline compatibility tests:** Prove that the spike does not alter default fetch behavior when CloakBrowser is absent or disabled.
- **Manual network smoke:** With CloakBrowser installed from official channels, run a small capped corpus and inspect recovered pages manually before trusting aggregate metrics.
- **Packaging guard:** Verify portable ZIP contents still follow the current browser payload invariant and contain no CloakBrowser binary or cache.

## Assumptions

- No new Python or Node dependency is added without explicit approval.
- CloakBrowser remains optional and off by default unless a later decision approves productization.
- Any future user-facing integration must explain the download size, official binary source, licensing constraints, privacy/operational tradeoffs, and that normal Baluffo features work without enhanced browser fallback.
- The existing Playwright browser fallback remains the baseline. CloakBrowser can supplement it only when evidence shows a clear recovery benefit on Baluffo's actual source corpus.
