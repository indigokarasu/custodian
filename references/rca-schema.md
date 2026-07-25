# RCA Record Schema

RCA records are stored in `{agent_root}/commons/data/ocas-custodian/rca.jsonl`. Each line is one JSON object.

## Full Record Schema

```json
{
  "rca_id": "rca-{fingerprint}-{YYYYMMDD}",
  "fingerprint": "oc_cron_dead_script_ref",
  "sub_fingerprint": "oc_cron_dead_script_ref:update_rally_sh",
  "status": "open|closed|escalated|monitoring",
  "pattern": "A|B|C|D|E",
  "created": "2026-06-05T12:00:00Z",
  "updated": "2026-06-05T12:00:00Z",
  "closed": null,
  "created_by_scan": "deep-20260605-1200",
  "recurrence_count": 3,
  "occurrences": [
    {
      "timestamp": "2026-06-03T08:00:00Z",
      "job_id": "finch-scan-abc123",
      "error_detail": "FileNotFoundError: update_rally.sh not found at <hermes-home>/scripts/",
      "fix_applied": "oc_cron_dead_script_ref:auto_fix",
      "fix_outcome": "fix_applied",
      "schedule_adjusted_stickiness": 0.5
    },
    {
      "timestamp": "2026-06-04T08:00:00Z",
      "job_id": "finch-scan-abc123",
      "error_detail": "FileNotFoundError: update_rally.sh not found at <hermes-home>/scripts/",
      "fix_applied": "oc_cron_dead_script_ref:auto_fix",
      "fix_outcome": "fix_applied",
      "schedule_adjusted_stickiness": 0.25
    },
    {
      "timestamp": "2026-06-05T08:00:00Z",
      "job_id": "finch-scan-abc123",
      "error_detail": "FileNotFoundError: update_rally.sh not found at <hermes-home>/scripts/",
      "fix_applied": null,
      "fix_outcome": null,
      "schedule_adjusted_stickiness": null
    }
  ],
  "root_cause_hypothesis": "HERMES_HOME resolves to <hermes-home>/profiles/indigo in cron context. The script path <hermes-home>/scripts/ is outside HERMES_HOME and gets blocked by the cron security model. Path needs to be <hermes-home>/profiles/indigo/scripts/.",
  "variable_identified": "path_resolution",
  "variable_detail": "Script field uses <hermes-home>/scripts/ which is outside cron's HERMES_HOME=<hermes-home>/profiles/indigo. Cron security model rejects paths outside HERMES_HOME.",
  "proposed_root_cause_fix": {
    "type": "path_canonicalization",
    "description": "Update job's script field from <hermes-home>/scripts/update_rally.sh to <hermes-home>/profiles/indigo/scripts/update_rally.sh",
    "tier": 1,
    "blocking": false,
    "block_reason": null
  },
  "upstream_trigger": null,
  "correlation_with": null,
  "schedule_adjusted_stickiness": 0.375,
  "confidence_at_creation": 0.2,
  "resolution": null,
  "resolution_type": null
}
```

## Field Reference

| Field | Type | Description |
|-------|------|-------------|
| `rca_id` | string | Unique ID: `rca-{fingerprint}-{date_created}` |
| `fingerprint` | string | Base fingerprint from known_issues.json |
| `sub_fingerprint` | string\|null | Decomposed sub-fingerprint if Pattern C. Null for single-cause. |
| `status` | string | `open` (active investigation), `closed` (resolved, fix held >7d), `escalated` (handed off), `monitoring` (Pattern D transient) |
| `pattern` | string | Recurrence pattern A-E (see `references/root-cause-analysis.md`) |
| `created` | ISO8601 | When this RCA record was first created |
| `updated` | ISO8601 | Last modification timestamp |
| `closed` | ISO8601\|null | When this RCA record was closed. Null if open. |
| `created_by_scan` | string | Scan ID that created this record |
| `recurrence_count` | int | Total occurrences tracked in this record |
| `occurrences` | array | Full occurrence chain (see Occurrence Schema below) |
| `root_cause_hypothesis` | string | One-sentence hypothesis of the root cause |
| `variable_identified` | string | Category: `path_drift`, `state_mutation`, `dependency_shift`, `environmental`, `none` |
| `variable_detail` | string | Human-readable explanation of the variable |
| `proposed_root_cause_fix` | object\|null | The root-cause-level fix (not the symptom fix) |
| `upstream_trigger` | string\|null | Fingerprint of the upstream trigger (Pattern E) |
| `correlation_with` | float\|null | Correlation coefficient with upstream trigger (0.0-1.0) |
| `schedule_adjusted_stickiness` | float | `cycles_survived / (1 + recurrence_count)` where `cycles_survived = days_since_fix / avg_schedule_interval_days`. Normalizes by job frequency so a 6h-cycle job and a weekly job are evaluated on equal terms. |
| `confidence_at_creation` float | Confidence score of the fingerprint when this RCA was created |
| `resolution` | string\|null | Description of how this was resolved |
| `resolution_type` | string\|null | `root_cause_fixed`, `transient_resolved`, `upstream_fixed`, `user_intervention`, `undetermined` |

## Occurrence Sub-Schema

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | ISO8601 | When this occurrence happened |
| `job_id` | string | Cron job ID, or "gateway" for gateway-level errors |
| `error_detail` | string | Error message / stack trace excerpt (first 500 chars) |
| `fix_applied` | string\|null | Fix fingerprint that was applied, null if none |
| `fix_outcome` | string\|null | `fix_applied`, `fix_attempted_failed`, `skipped_pattern_b`, null if no fix attempted |
| `schedule_adjusted_stickiness` | float\|null | `cycles_survived / (1 + recurrence_count)` — normalized by job schedule. < 0.5 = fix didn't survive one cycle. > 2.0 = fix held across 2+ cycles per recurrence. |

## Lifecycle

1. **Creation** (Step 3b): Created when a fingerprint hits `recurrence_count >= 2`. Pattern classified. Initial `root_cause_hypothesis` formed.
2. **Update** (Step 3b + Step 14): Each subsequent scan that touches this fingerprint appends a new occurrence, updates `schedule_adjusted_stickiness`, and refines the hypothesis.
3. **Escalation** (Step 10 + Step 14): If Pattern B or fix-loop detected, RCA record is attached to the escalation journal entry. Tier 3 escalation includes the full occurrence chain.
4. **Closure** (Step 14): When `schedule_adjusted_stickiness > 2.0` for 7+ consecutive days after a root-cause fix, status → `closed`, `closed` timestamp set, `resolution` and `resolution_type` filled in.
5. **Reopening**: If a closed RCA's fingerprint recurs, reopen the RCA record instead of creating a new one. Increment `recurrence_count`, add new occurrence, reset `schedule_adjusted_stickiness`.

## File Location and Rotation

- **Path**: `{agent_root}/commons/data/ocas-custodian/rca.jsonl`
- **Rotation**: Closed records older than 30 days are compacted to a daily summary. Open records are never auto-deleted.
- **Concurrency safe**: Append-only JSONL. Multiple scans can write concurrently (each occurrence is a separate append; the RCA record update re-writes the full record line — use file-level append for new occurrences and atomic rewrite for status updates).

## Initial Backfill

On first deployment, scan `fix_effectiveness.jsonl` and `fixes.jsonl` for fingerprints with `recurrence_count >= 2` and create baseline RCA records. Set `pattern` to `unknown` (to be classified on next deep scan). Set `confidence_at_creation` to the current confidence score.
