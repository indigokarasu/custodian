# Escalation Runner 2026-06-15 21:00 UTC

## Session Summary

Escalation runner cron job ran at 21:00 UTC (14:00 PT). The 20:41 UTC esc-run had already closed 11 stale issues. This run found 1 additional stale issue to close.

## Issues Closed

- `oc_email_check_auth_failure_20260531` — email:check job now shows `last_status=ok` (2026-06-15T14:23Z). Token file at `<gworkspace-creds>/credentials/<user-google-email>.json` is valid (3614 bytes) with refresh_token. The `invalid_grant` resolved via OAuth auto-refresh.

## Stale Proposal Cleared

- `prop-mcp-server-files-missing-0615` (Corvus) — Proposed action on 4 MCP servers (instagram, pdsx, spotify, threads). Custodian scan already verified all 4 are `enabled: false` in config.yaml. Zero connection errors. Issue was stale. **Lesson: Corvus proposals can flag issues custodian already resolved. Always verify current config state before acting.**

## Transient Errors Observed

- `custodian:deep` and `bones:research` both failed with `RuntimeError: [Errno 32] Broken pipe` at 08:37 UTC. Same timestamp = shared upstream transient. Both will self-resolve on next run. Not a new issue.

## Remaining Open Issues (all require user action or are provider-level)

| Issue | Status | Why Open |
|-------|--------|----------|
| `skill_library_stubs` | open | Tier 4, user confirmation needed (15 stub dirs) |
| `ocas-critique-missing-skillmd` | open | Tier 2, user decision needed |
| `skill_hygiene_followup_20260601` | open | Tier 2, user confirmation needed |
| `iss-20260426-001` (429 rate limit) | open | Provider-level, cannot auto-fix |
| `iss-20260528-002` (401 auth) | open | Custom provider, cannot auto-fix |

## System State

- Disk: 80% (at threshold, not critical)
- state.db: 4.8GB (healthy)
- Gateway: running (PID 369402), systemd inactive (--replace pattern)
- All MCP servers (instagram, pdsx, spotify, threads): `enabled: false` in config
- Google OAuth: token valid, auto-refreshing
