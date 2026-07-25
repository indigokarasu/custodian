# Escalation Runner — 2026-06-01 23:40 UTC

## Summary

Second escalation run of the evening (first was 22:06). Scanned 5 open issues. 0 autonomous fixes applied. 3 require user action.

## Root Cause Analysis: email_check.py `invalid_grant`

Both credential stores have revoked refresh tokens. Key finding: email_check.py uses MCP credentials (client `112292610034...`), NOT google_token.json (client `550801240087...`). Copied google_token.json access token into MCP credentials as experimental fix — still returns `invalid_grant`.

## Issues Reviewed

| Issue | Status | Notes |
|-------|--------|-------|
| oc_email_check_script_failure_20260601 | OPEN | Both OAuth clients revoked. User re-auth required. |
| oc_finch_weekly_manifest_401_20260601 | OPEN | manifest.build API key invalid. |
| skill_library_stubs | OPEN | 21 stub dirs. User confirmation required. |
| ocas-critique-missing-skillmd | OPEN | No SKILL.md. User must re-install or remove. |
| oc_disk_brief_full_20260601_1103 | MONITORING | Disk recovered. VACUUM needed. |