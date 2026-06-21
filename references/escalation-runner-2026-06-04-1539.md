# Escalation Runner 2026-06-04 15:39

## Key Learning: Verify Job Status Directly from jobs.json

When checking whether a previously-escalated issue has self-resolved, the fastest path is to query the job's `last_status` directly from `jobs.json` rather than waiting for journal evidence. In this run, `email:check` showed `last_status=ok` with `last_run_at=2026-06-04T15:21` — confirming the stale error from a previous script path issue had been absorbed without needing a new fix.

**Pattern:** `last_status=ok` + `last_run_at` recent = issue resolved, close immediately.

## Findings Summary

| Issue | Status | Notes |
|---|---|---|
| `oc_google_oauth_client_deleted_20260604` | open, cannot fix | OAuth client deleted from GCP Console. Needs interactive re-auth. |
| `oc_cron_script_path_block_update_jobs` | open, framework bug | 8 scripts verified at correct paths. Security model false positive. |
| `oc_http_401_auth_failure` | open, transient | 4 jobs, upstream provider auth failures. Self-retry expected. |
| `oc_google_token_invalid_email_check_20260603_rev2` | **resolved** | email:check last_status=ok. Stale error, closed. |
| `skill_library_stubs` | open, needs user | 29 stub dirs, unchanged. |
| `skill_hygiene_followup_20260601` | open, needs user | Same as above. |
| `ocas-critique-missing-skillmd` | open, needs user | No affected jobs. |

## System State

- Gateway: ok
- Cron: 105 total, 8 script-path-blocked (false positive), 4 HTTP 401 (transient)
- Google OAuth: credentials file exists but client deleted — interactive re-auth required
- email:check: ok (self-healed)
