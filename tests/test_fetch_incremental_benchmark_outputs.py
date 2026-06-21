from __future__ import annotations

import gzip
import json

from src import fetch_incremental_sanity_benchmark as benchmark


def test_source_decision_matrix_markdown_is_written_next_to_summary(tmp_path) -> None:
    payload = {
        "sourceDecisionMatrix": [
            {
                "name": "static_source::maliyo",
                "action": "timeout_or_network_budget",
                "priority": 30,
                "keptCount": 5,
                "durationMs": 27072,
                "decisionType": "slow_productive_static",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {"flags": ["time_budget"]},
                "nextDecision": "Inspect timeout.",
            }
        ],
        "nextOptimizationTargets": [{"name": "static_source::maliyo"}],
    }

    (tmp_path / "benchmark-summary.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
    (tmp_path / "source-decision-matrix.md").write_text(
        benchmark._render_source_decision_matrix_markdown(
            [dict(row) for row in benchmark._as_list(payload.get("sourceDecisionMatrix"))]
        ),
        encoding="utf-8",
    )

    summary = json.loads((tmp_path / "benchmark-summary.json").read_text(encoding="utf-8"))
    markdown = (tmp_path / "source-decision-matrix.md").read_text(encoding="utf-8")
    assert summary["nextOptimizationTargets"] == [{"name": "static_source::maliyo"}]
    assert "static_source::maliyo" in markdown
    assert "`slow_productive_static`" in markdown


def test_load_output_jobs_reads_plain_or_gzip_backed_pipeline_json(tmp_path) -> None:
    with gzip.open(tmp_path / "jobs-unified.json.gz", mode="wt", encoding="utf-8") as handle:
        json.dump(
            [
                {"source": "static_source::super-lucky", "jobLink": "https://stillfront.com/a"},
                "not-a-row",
            ],
            handle,
        )

    assert benchmark._load_output_jobs(tmp_path) == [
        {"source": "static_source::super-lucky", "jobLink": "https://stillfront.com/a"}
    ]


def test_source_decision_log_template_renders_operator_fields_and_guardrails() -> None:
    markdown = benchmark._render_source_decision_log_template_markdown(
        [
            {
                "name": "static_source::super-lucky",
                "action": "source_policy_review",
                "keptCount": 33,
                "durationMs": 24614,
                "decisionType": "explicit_source_policy",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": True,
                "evidence": {
                    "flags": ["failure:site_changed", "high_merge_ratio"],
                    "reasons": ["site_changed"],
                    "failureBucket": "site_changed",
                    "registryPageEvidence": {"offListingHosts": ["stillfront.com"]},
                    "errorSamples": ["HTTP 404"],
                },
            },
            {
                "name": "static_source::koei",
                "action": "source_scope_and_timeout_review",
                "keptCount": 10,
                "durationMs": 24527,
                "decisionType": "explicit_source_scope",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": True,
                "evidence": {
                    "flags": ["failure:unknown", "time_budget"],
                    "reasons": ["time_budget"],
                    "registryPageEvidence": {"offListingHosts": ["careerviet.vn"]},
                },
            },
            {
                "name": "static_source::maliyo",
                "action": "timeout_or_network_budget",
                "keptCount": 5,
                "durationMs": 27072,
                "decisionType": "slow_productive_static",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {"flags": ["time_budget"]},
            },
            {
                "name": "static_source::netflix",
                "action": "source_policy_review",
                "keptCount": 0,
                "durationMs": 17503,
                "decisionType": "follow_up_review",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {"zeroKeptClassification": "needs_review"},
            },
        ]
    )

    assert "local review evidence only" in markdown
    assert "`explicit_source_policy`" in markdown
    assert "`explicit_source_scope`" in markdown
    assert "`slow_productive_static`" in markdown
    assert "`follow_up_review`" in markdown
    assert "`preserve_current_behavior`" in markdown
    assert "- Behavior change allowed: `false`" in markdown
    assert "- Requires explicit decision: `true`" in markdown
    assert ".".join(("stillfront", "com")) in markdown
    assert ".".join(("careerviet", "vn")) in markdown
    assert "Decision: preserve / investigate / change_later" in markdown
    assert "Chosen action:" in markdown
    assert "Reason:" in markdown
    assert "Risk accepted: yes/no" in markdown
    assert "Follow-up owner/date:" in markdown


def test_source_decision_log_template_empty_state_is_useful() -> None:
    markdown = benchmark._render_source_decision_log_template_markdown([])

    assert "# Source Decision Log Template" in markdown
    assert "No source decision rows were generated." in markdown
    assert "local review evidence only" in markdown


def test_static_outlier_artifact_family_includes_decision_log_template(tmp_path) -> None:
    payload = {
        "sourceDecisionMatrix": [
            {
                "name": "static_source::maliyo",
                "action": "timeout_or_network_budget",
                "priority": 30,
                "keptCount": 5,
                "durationMs": 27072,
                "decisionType": "slow_productive_static",
                "recommendedFirstPass": "preserve_current_behavior",
                "behaviorChangeAllowed": False,
                "requiresExplicitDecision": False,
                "evidence": {"flags": ["time_budget"]},
                "nextDecision": "Inspect timeout.",
            }
        ],
        "nextOptimizationTargets": [{"name": "static_source::maliyo"}],
    }
    rows = [dict(row) for row in benchmark._as_list(payload.get("sourceDecisionMatrix"))]

    (tmp_path / "benchmark-summary.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
    (tmp_path / "source-decision-matrix.md").write_text(
        benchmark._render_source_decision_matrix_markdown(rows),
        encoding="utf-8",
    )
    (tmp_path / "source-decision-log-template.md").write_text(
        benchmark._render_source_decision_log_template_markdown(rows),
        encoding="utf-8",
    )

    summary = json.loads((tmp_path / "benchmark-summary.json").read_text(encoding="utf-8"))
    matrix_markdown = (tmp_path / "source-decision-matrix.md").read_text(encoding="utf-8")
    log_markdown = (tmp_path / "source-decision-log-template.md").read_text(encoding="utf-8")
    assert summary["nextOptimizationTargets"] == [{"name": "static_source::maliyo"}]
    assert "`slow_productive_static`" in matrix_markdown
    assert "Decision: preserve / investigate / change_later" in log_markdown
