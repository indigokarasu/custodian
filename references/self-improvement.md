# Self-Improvement

`fix_effectiveness.jsonl`: per-fingerprint tracking of attempts, successes, failures, recurrence. See **Confidence Model** section above for the full algorithm including auto tier promotion/demotion.

Custodian OKRs (every journal): `success_rate`, `issues_detected`, `issues_auto_fixed`, `fix_success_rate`, `mean_time_to_fix_ms`, `open_residuals`, `escalations`, `high_recurrence_fingerprints`, `skills_initialized`, `background_tasks_registered`, `schedule_score`, `journal_completeness`, `confidence_model_coverage`, `escalation_precision`.

# Escalation Path

Tier 3: append `status: escalated` to `issues.jsonl`, tag journal `escalation_needed: true`, write InsightProposal (`anomaly_alert`) to `{agent_root}/commons/data/ocas-custodian/proposals/{proposal_id}.json`. Vesper reads from this directory. If Mentor present, note `mentor.plan.run custodian-repair --arg issue_id={id}` available.

**Confidence-gated escalation:** Before escalating, check `fix_effectiveness.jsonl`. If the fingerprint has `confidence_score >= 0.6` and `recommended_tier == 1`, reclassify as Tier 1 and auto-fix instead. Only escalate if the confidence model confirms the fix is unknown or known-bad.

Clean state: zero open issues + previous cycle clean = suppress Vesper signal. First run of day or issues now resolved = emit clean bill of health.
