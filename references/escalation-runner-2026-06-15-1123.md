# Escalation Runner 2026-06-15 11:23 PT

## Session Summary

Escalation runner cron job ran at 11:23 PT. The 11:08 UTC runner had already resolved 3 issues. This runner found and resolved 6 more stale issues across 4 issues.jsonl files, and demoted 2 provider-level issues.

## Issues Resolved

| Issue ID | Root Cause | Resolution |
|----------|------------|------------|
| `oc_google_auth_<account-identity>` | Empty token file | Token valid, auto-refreshing. email:check job healthy. |
| `oc_google_oauth_client_deleted_20260604` | OAuth client deleted | Client functional, token refreshed today. |
| `oc_mcp_server_files_missing_20260614` | 4 MCP servers missing files | All 4 disabled in config.yaml (resolved by 11:08 runner). |
| `oc_gateway_collision_default_profile_20260613` | Default gateway crash loop | Service no longer exists. |
| `iss-20260528-005` | Bones Kalshi API breakage | Transient, job now ok. |
| `iss-20260531-001` | Email auth token corrupt | Duplicate of oc_google_auth_<account-identity>. |

## Issues Demoted

| Issue ID | Root Cause | Reason |
|----------|------------|--------|
| `iss-20260426-001` | HTTP 429 rate limit | Low frequency, provider-level, self-resolving. |
| `iss-20260528-002` | HTTP 401 auth failure | Only from manifest.build custom provider, not main OpenRouter. |

## Key Learning: Multi-Path Issue Discovery

Found issues in 4 different `issues.jsonl` files:
- `<hermes-home>/profiles/indigo/commons/journals/ocas-custodian/issues.jsonl`
- `<hermes-home>/profiles/indigo/commons/data/custodian/issues.jsonl`
- `<hermes-home>/profiles/indigo/commons/data/ocas-custodian/issues.jsonl`
- `<hermes-home>/profiles/indigo/commons/ocas-custodian/issues.jsonl`

Same root cause appeared with different IDs across files. Must deduplicate by description.

## System State

- Gateway: running (PID 369402)
- Cron jobs: 112/113 ok
- Disk: 80% (at threshold)
- state.db: 4.8GB (healthy)
- Google OAuth: token valid until 2026-06-15T19:23Z
