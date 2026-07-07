# Observation Journal Schema (Light Scan / Deep Scan Clean Verdict)

When the scan is clean (all errors are transient or non-actionable), write this JSON shape. Always via `terminal(command="python3 << 'PYEOF' ...")`.

## JSON Shape

```json
{
  "run_id": "light-scan-{ISO8601_utc_timestamp}",
  "timestamp": "2026-06-25T04:05:20Z",
  "scan_type": "light",
  "jobs_total": 136,
  "jobs_error_count": 13,
  "fixes_applied": [],
  "fixes_deferred": [],
  "not_activity_reason": "clean_verdict_100pct_transient",
  "error_classification": {
    "<fingerprint>": {
      "count": 2,
      "description": "Human-readable pattern summary",
      "transient": true,
      "by_design": true,
      "jobs": ["job:name1", "job:name2"]
    }
  },
  "gateway_events": {
    "sigterm_restarts": 4,
    "times": ["05:13", "05:53", "13:24", "13:55"],
    "cause": "systemd description"
  },
  "platform_noise": {
    "<pattern>": "Classification"
  },
  "open_issues_count": 1,
  "open_issues": ["iss-XXXX (status, brief description)"]
}
```

## Field Definitions

| Field | Type | Notes |
|-------|------|-------|
| `run_id` | string | `light-scan-` or `deep-scan-` + ISO8601 UTC timestamp with seconds |
| `timestamp` | string | ISO 8601 UTC write time |
| `scan_type` | string | `"light"` or `"deep"` |
| `jobs_total` | int | Total entries in `jobs.json` array |
| `jobs_error_count` | int | Count where `last_status == "error"` |
| `fixes_applied` | array | Empty on clean verdict |
| `fixes_deferred` | array | Empty on clean verdict |
| `not_activity_reason` | string | `"clean_verdict_100pct_transient"`, `"clean_verdict_all_stale_or_transient"`, `"clean_verdict_all_errors_already_escalated"`, or `"monitoring_only"` |
| `error_classification` | object | Key = fingerprint slug; value = {count, description, transient, jobs[]?} |
| `gateway_events` | object | If relevant (restarts observed) |
| `platform_noise` | object | If relevant (Telegram/etc. warnings) |
| `open_issues_count` | int | Issues in issues.jsonl not resolved/closed |
| `open_issues` | array | List of issue IDs + brief status |

## Light Scan Clean Verdict Sequence

1. Parse `jobs.json` via terminal Python
2. Discover error jobs via grep/tail on gateway log
3. Classify each error into known fingerprints (all cf=None/0, transient patterns)
4. Confirm no active issues in issues.jsonl
5. Construct the journal JSON
6. Write to `{profile_root}/commons/journals/ocas-custodian/{YYYY-MM-DD}/{run_id}.json` via terminal Python heredoc
7. Return `[SILENT]`

## Pitfall: Double-Quoted JSON in Heredoc

When writing via `terminal(command='python3 << \'PYEOF\' ...')`, the `run_id` string uses f-string inside Python. The f-string braces and inner quotes must be escaped. **Test** the output with `cat {file} | python3 -m json.tool` after writing.

## Delta Journal Pattern (Repeated Clean Verdicts)

When consecutive light scans produce the **same classification** (same error jobs, same fingerprints, no new issues), add a `previous_scan_delta` block to build on the prior scan rather than restating from scratch. This turns "same thing again" into evidence of persistence.

**Structure:**
```json
"previous_scan_delta": {
  "previous_scan": "2026-06-29T01:04:57Z",
  "elapsed_minutes": 65,
  "new_issues": 0,
  "new_errors": 0,
  "note": "Fourth scan in ~3h, identical outcome. OAuth issue stable (no token rotation since revocation)."
}
```

**When to use:** Only when `error_classification` is identical to the previous scan (same fingerprints, same jobs, same counts). If ANY new error appears or a previously-errored job resolves, do NOT use delta — write a full classification.

**When NOT to use:** First scan after a gateway restart, after applying a fix, or when any state change occurred between scans. Delta is for stable-but-persistent conditions only.

**`not_activity_reason` for delta scans:** Use `"clean_verdict_all_errors_already_escalated"` when all error jobs are tracked in issues.jsonl with no new failures. Use `"clean_verdict_100pct_transient"` when all errors are transient patterns with no escalation needed.

**Confirmed 2026-06-29:** 4 consecutive scans within ~3h all found `email:check` + `monitor:list` failing from the same OAuth token revocation. Deltas at 57min, 124min, 65min — stable, persistent, user-gated.

## Examples

See `references/light-scan-2026-06-24-1001.md` for a complete real-world clean verdict journal.
See `references/light-scan-2026-06-29-0104.md` for a delta journal example (4th consecutive identical scan).
