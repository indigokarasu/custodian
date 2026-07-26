# Escalation Runner — 2026-06-01 21:13 UTC (Run 4)

## Fix applied

**look:update re-enabled** — Job `bbc802581354` was paused (`enabled: false`, `state: paused`) due to HTTP 429 rate limit on 2026-05-31. Error was transient. Re-enabled by setting `enabled: true`, `state: idle`, removing `paused_at`/`paused_reason` in `<hermes-home>/cron/jobs.json`.

## Issues verified (all unchanged, require user action)

1. **oc_email_check_auth_failure_20260531** — Google OAuth token revoked (`invalid_grant`). Token files exist but refresh is rejected. Interactive re-auth required.
2. **oc_finch_weekly_manifest_401_20260531** — manifest.build API key rejected. User must update key.
3. **oc_auxiliary_nous_payment_20260530** — Nous payment/credit issue. User must check subscription.
4. **skill_library_stubs** — 21 stub directories without SKILL.md. User confirmation required.
5. **ocas-critique-missing-skillmd** — Directory exists with data but no SKILL.md. User decision needed.

## Key learning: jobs.json path

**jobs.json is at `<hermes-home>/cron/jobs.json`** — NOT `<hermes-home>/jobs.json`. The escalation runner workflow must always use this path. The file structure is `{"jobs": [...], "updated_at": "..."}`.

## Key learning: paused jobs from transient 429

Jobs auto-paused by the system due to HTTP 429 rate limits are transient. The escalation runner should check for paused jobs with transient error patterns and re-enable them as a Tier 1 auto-fix. Pattern to detect:
- `enabled: false`
- `state: paused`
- `last_status: error`
- `last_error` contains "HTTP 429" or "Rate limited"

## Key learning: google_auth_mcp.py function verification

`get_gmail_service` DOES exist in `<hermes-home>/scripts/google_auth_mcp.py` (line 109). Previous reference documentation claiming it only exports `get_service` was incorrect. The import works fine; the actual email:check issue is the revoked OAuth token.
