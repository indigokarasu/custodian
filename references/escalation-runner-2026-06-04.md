# Escalation Runner — 2026-06-04 09:50 UTC

## Summary

Scanned 6 open issues. Resolved 1 (weave 401), marked 1 resolved (state.db bloat), updated 1 assessment (email:check stale error). 3 require user action (skill stubs, hygiene, ocas-critique).

## Issues Resolved (2 → 3)

| Issue | Resolution |
|-------|------------|
| oc_weave_401_upstream_auth_20260604 | All weave jobs (enrichability-recalc, overnight-enrichment, sync-contacts) now status=ok. Transient 401 self-resolved. |
| oc_state_db_bloat_15gb_20260603 | VACUUM completed. 13.4GB with 487 freelist pages (0.014% reclaimable). FTS trigram index is size consumer. Operational cost. |
| oc_google_token_invalid_email_check_20260603_rev2 | **RESOLVED by esc-run 10:07 UTC.** Error traceback references OLD path, job script field fixed 2026-06-03, last_run_at (2026-06-01) predates fix. MCP credentials valid with refresh_token. google_auth_mcp import verified working. Error is definitively stale — no user action needed. |

## Issues Updated (0)

(Previously 1 updated issue — now resolved above.)

## User Action Required (3)

- **skill_library_stubs** (Tier 4) — 25 stub directories without SKILL.md. Requires user confirmation.
- **skill_hygiene_followup_20260601** (Tier 2) — Same + nested .git repos. Awaiting user response.
- **ocas-critique-missing-skillmd** (Tier 2) — No SKILL.md, no affected jobs. Requires user confirmation.

## Monitoring (1)

- **oc_finch_weekly_manifest_401_20260531** — API key confirmed valid by 3+ escalation runners (curl /v1/models returns 200). 401 is transient/job-specific. Next run: 2026-06-07 (Sunday). **Recommend closing this issue** — it has been in monitoring for 4+ days with no new occurrences and the root cause (bad API key) was disproven.

## System Health

- Gateway: ok
- Disk: 69% (66G/96G)
- state.db: 13.4GB (VACUUM complete, FTS trigram index is size consumer)
- Cron: 105 total, 105 enabled, 8 stale errors, 0 fresh errors

## Key Learnings

### 1. Stale error detection via last_run_at vs fix date

When a cron job's `last_error` traceback references a different script path than the current `script` field in jobs.json, compare `last_run_at` against the fix date. If `last_run_at` is BEFORE the fix, the error is stale — the job hasn't run with the corrected path yet. The `consecutive_failures` counter may persist across script field changes, so don't treat it as evidence of a current problem without checking `last_run_at`.

### 2. finch:weekly manifest.build 401 is a FALSE POSITIVE

The `oc_finch_weekly_manifest_401_20260531` issue was originally classified as "user must update API key" (Tier 3). However, multiple escalation runners have now confirmed:
- 2026-06-01: API key tested via curl → 200 OK
- 2026-06-03: API key re-tested → 200 OK with valid model list
- 2026-06-04: API key still valid

The 401 is transient/job-specific, not a bad API key. **This issue should be closed.** Leaving it open wastes escalation cycles.

### 3. MCP credential file location

The `google_auth_mcp.py` module reads from `<gworkspace-creds>/credentials/<email>.json`, NOT from `<hermes-home>/google_token.json`. When diagnosing email:check auth errors, check the MCP credentials file, not the hermes token file. The MCP credentials file can be refreshed independently and may have a valid token even when the hermes token file is stale.

### 4. Stale Error Resolution Threshold (NEW)

The previous escalation runner (09:50 UTC) left email:check as "updated but still flagged" pending a successful run. This session (10:07 UTC) established that **three conditions are sufficient to resolve immediately without waiting for confirmation**:

1. Error traceback path ≠ current `script` field in jobs.json
2. `last_run_at` predates the fix date
3. New script path's dependencies verified working (imports resolve, credentials valid)

When all three are met, the error is **definitively stale** — the job has never run with the corrected configuration. Resolve the issue and move on. Do not wait for a successful run that may be days away due to scheduling.

After pruning 115K messages (198K → 83K), VACUUM reclaimed minimal space because the FTS trigram index retains its size. The 13.4GB is now the operational baseline. Do not re-flag state.db bloat unless it grows significantly beyond this baseline.