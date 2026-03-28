# Measurements

Tools that run Baluffo or its pipeline and measure outputs, timing, yield, quality, and regressions.

## Structure

```
measurements/
  pipeline/
    job_discovery_increment_measurement.py  # Measure pipeline discovery gains

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
