# Measurements

Tools that run Baluffo or its pipeline and measure outputs, timing, yield, quality, and regressions.

## Structure

```
measurements/
  desktop/
    spawn_startup.py                       # Measure click -> window -> interactive from Popen

  pipeline/
    fetch_report_read_audit.py              # Audit every hot fetch-report consumer's read cost
    job_discovery_increment_measurement.py  # Measure pipeline discovery gains
    launch_baseline_read.py                 # Measure the launch's baseline output-count read
    static_residual_failures.py             # Classify residual failed source rows

  sources/
    social_sources_monitoring.py   # Monitor social source performance
    social_sources_optimization.py # Optimize social source config
```

## Usage

### Latest Run Triage

Use this first when you want a concise summary of the latest discovery/fetch run instead of opening the raw JSON artifacts:

```bash
python tools/measurements/pipeline/latest_run_report.py
```

Add `--json` when you want a machine-readable summary for automation.

### Desktop Spawn Startup

The packaged startup probe starts its clock at `desktop_launch_start`, so its
total leaves out the PyInstaller boot and the launcher's import preamble. This
tool starts the portable EXE itself and places every journal row against the
spawn timestamp. Launcher rows use `ts`; page rows use their own page clock
(`browserTsMs`), with the bridge-arrival value reported beside it so a queued
metric POST stays visible instead of being charged to the page.

```bash
python tools/measurements/desktop/spawn_startup.py --runs 2 --modes cold,warm
python tools/measurements/desktop/spawn_startup.py --page admin --probe --json
```

It needs a portable build (`npm run build:portable-exe`) or a Linux AppImage
(`npm run build:linux`), which `--exe` accepts and starts through
`--appimage-extract-and-run`. Runtime data, logs and the report land under
`--output-dir` (default `.tmp/spawn-startup`), so `data/` is never touched.
`spawn-startup.json` is the machine-readable report and `spawn-startup.md` is the
table the startup architecture doc quotes. A Linux launch has no observed window
mark, so its window figure is labelled `launcher-inferred` rather than reported
as a paint. The clock arithmetic lives in `src/ship/startup_spawn_clock.py` and is
shared with the packaged probe, so this report and the release preflight's
`startup-profile-summary.json` place a row on the same clock.

### Fetch Report Read Audit

Fetch-report consumers fail the same way: a "summary" path that quietly parses
the whole multi-megabyte artifact. This audit times every consumer class on a
real report and names where it runs, so only the explicit full view is allowed
to pay the full cost:

```bash
python tools/measurements/pipeline/fetch_report_read_audit.py
python tools/measurements/pipeline/fetch_report_read_audit.py --report PATH --runs 5
```

Arms: pipeline launch count (pre-change/current cold/current warm), desktop
startup success gate (pre-change/current), `/ops/fetch-report` summary and live
views, full normalize + dedup merge, and the retry-failed fetcher preset. The
startup gate runs against a temporary data dir; nothing under `data/` changes.

### Pipeline Launch Baseline Read

Measure how much of `PipelineService.start_task` is the read of the previous fetch
report's `summary.outputCount`. It drives the real launch with a stub worker and
times three count implementations (pre-memoization normalize, memoized cold, and
memoized warm), then adds the production share from
`jobs_pipeline_launch_finished` rows in the bridge journal:

```bash
# Largest fetch report found under _out (falls back to data/)
python tools/measurements/pipeline/launch_baseline_read.py

# Pin the report and increase iterations
python tools/measurements/pipeline/launch_baseline_read.py \
  --report _out/coverage/verify-v8/jobs-fetch-report.json --runs 5

# Only time the count reads, no launch arms
python tools/measurements/pipeline/launch_baseline_read.py --skip-launch
```

It writes nothing to `data/`: the launch's control status goes to a temporary
directory and the worker performs no stages.

### Static Residual Failure Classification

Use this after a validation fetch run to split failed static/provider/browser rows into
narrow follow-up buckets:

```bash
python tools/measurements/pipeline/static_residual_failures.py \
  --fetch-report _out/static-residual-validation/jobs-fetch-report.json \
  --active-registry data/source-registry-active.json \
  --json
```

### Pipeline Measurements

```bash
# Measure job discovery gains from social sources
python tools/measurements/pipeline/job_discovery_increment_measurement.py
```

### Source Measurements

```bash
# Monitor social sources performance (requires pipeline run first)
python tools/measurements/sources/social_sources_monitoring.py

# Optimize social sources configuration
python tools/measurements/sources/social_sources_optimization.py
```

## Background

- **Pipeline measurements**: Run the full pipeline with/without social sources and compare outputs
- **Source measurements**: Analyze performance of individual sources (Reddit, X, Mastodon)
