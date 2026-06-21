# OKRs

Universal OKRs from spec-ocas-journal.md apply to all runs.

```yaml
skill_okrs:
  - name: fix_success_rate
    metric: fraction of Tier 1 fixes that resolve the underlying issue without recurrence within 7 days
    direction: maximize
    target: 0.85
    evaluation_window: 30_runs
  - name: skill_init_coverage
    metric: fraction of installed skills properly initialized and registered at any time
    direction: maximize
    target: 1.0
    evaluation_window: 7_runs
  - name: scan_detection_accuracy
    metric: fraction of real errors detected in light/deep scans within expected latency
    direction: maximize
    target: 0.90
    evaluation_window: 30_runs
  - name: escalation_precision
    metric: fraction of escalated issues that genuinely require user action (not auto-fixable)
    direction: maximize
    target: 0.90
    evaluation_window: 30_runs
  - name: confidence_model_coverage
    metric: fraction of known fingerprints with confidence_score >= 0.5
    direction: maximize
    target: 0.80
    evaluation_window: 30_runs
  - name: schedule_adherence
    metric: fraction of expected runs that produced evidence
    direction: maximize
    target: 0.98
    evaluation_window: 30_runs
  - name: data_integrity
    metric: fraction of reads that pass schema validation
    direction: maximize
    target: 1.00
    evaluation_window: 30_runs
```
