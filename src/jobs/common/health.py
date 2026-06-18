"""Source health scoring utilities."""

from datetime import UTC


def calculate_health_score(
    consecutive_failures: int,
    consecutive_zero_kept: int,
    median_latency_ms: int = 0,
    latency_penalty_threshold_ms: int = 300000,
) -> int:
    """Calculate health score (0-100) based on source history.

    Args:
        consecutive_failures: Number of consecutive failed runs
        consecutive_zero_kept: Number of consecutive zero-kept runs
        median_latency_ms: Median latency for the source
        latency_penalty_threshold_ms: Threshold above which latency penalizes score

    Returns:
        Health score between 0 (worst) and 100 (best)
    """
    base_score = 100

    failure_penalty = min(consecutive_failures * 15, 50)

    zero_kept_penalty = min(consecutive_zero_kept * 10, 40)

    latency_penalty = 0
    if median_latency_ms > latency_penalty_threshold_ms:
        excess = median_latency_ms - latency_penalty_threshold_ms
        latency_penalty = min(int(excess / 10000), 10)

    score = base_score - failure_penalty - zero_kept_penalty - latency_penalty

    return max(0, min(100, score))


def get_top_failing_sources(
    source_states: dict[str, dict],
    limit: int = 10,
) -> list[dict]:
    """Get top failing sources by consecutive failures.

    Args:
        source_states: Dict of source name -> source state
        limit: Maximum number of sources to return

    Returns:
        List of sources with highest failure counts
    """
    failures = [
        {
            "name": name,
            "consecutiveFailures": state.get("consecutiveFailures", 0),
            "lastFailureAt": state.get("lastFailureAt"),
            "lastError": state.get("lastError"),
            "lastFailureBucket": state.get("lastFailureBucket"),
        }
        for name, state in source_states.items()
        if state.get("consecutiveFailures", 0) > 0
    ]
    failures.sort(key=lambda x: x["consecutiveFailures"], reverse=True)
    return failures[:limit]


def get_top_zero_kept_sources(
    source_states: dict[str, dict],
    limit: int = 10,
) -> list[dict]:
    """Get top sources by consecutive zero-kept runs.

    Args:
        source_states: Dict of source name -> source state
        limit: Maximum number of sources to return

    Returns:
        List of sources with highest zero-kept counts
    """
    zero_kept = [
        {
            "name": name,
            "consecutiveZeroKept": state.get("consecutiveZeroKept", 0),
            "lastRunAt": state.get("lastRunAt"),
            "lastKeptCount": state.get("lastKeptCount", 0),
            "lastFailureBucket": state.get("lastFailureBucket"),
        }
        for name, state in source_states.items()
        if state.get("consecutiveZeroKept", 0) > 0
    ]
    zero_kept.sort(key=lambda x: x["consecutiveZeroKept"], reverse=True)
    return zero_kept[:limit]


def get_top_slow_sources(
    source_states: dict[str, dict],
    limit: int = 10,
) -> list[dict]:
    """Get top slowest sources by median latency.

    Args:
        source_states: Dict of source name -> source state
        limit: Maximum number of sources to return

    Returns:
        List of slowest sources
    """
    latencies = [
        {
            "name": name,
            "lastDurationMs": state.get("lastDurationMs", 0),
            "recentLatencies": state.get("recentLatencies", []),
        }
        for name, state in source_states.items()
        if state.get("lastDurationMs", 0) > 0
    ]
    latencies.sort(key=lambda x: x["lastDurationMs"], reverse=True)
    return latencies[:limit]


def get_quarantined_sources(
    source_states: dict[str, dict],
) -> list[dict]:
    """Get all currently quarantined sources.

    Args:
        source_states: Dict of source name -> source state

    Returns:
        List of quarantined sources with reasons
    """
    from datetime import datetime

    now = datetime.now(UTC)
    quarantined = []
    for name, state in source_states.items():
        quarantined_until = state.get("quarantinedUntilAt")
        if quarantined_until:
            try:
                until_dt = datetime.fromisoformat(quarantined_until.replace("Z", "+00:00"))
                if until_dt > now:
                    reason = "consecutive_failures"
                    if state.get("consecutiveZeroKept", 0) >= 3:
                        reason = "consecutive_zero_kept"
                    quarantined.append(
                        {
                            "name": name,
                            "quarantinedUntilAt": quarantined_until,
                            "reason": reason,
                            "consecutiveFailures": state.get("consecutiveFailures", 0),
                            "consecutiveZeroKept": state.get("consecutiveZeroKept", 0),
                        }
                    )
            except (AttributeError, TypeError, ValueError):
                pass
    return quarantined
