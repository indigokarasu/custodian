# Escalation Runner 2026-06-01 16:00

## Session Summary
Reviewed open issues. Applied 1 config fix (stealth-browser MCP python path). 4 issues remain requiring user action.

## Fix Applied

### stealth-browser MCP: ModuleNotFoundError nodriver
- **Root cause**: Config `command: python3` resolved to hermes-agent venv (Python 3.11) which lacks `nodriver`. The MCP server's dependencies are in a dedicated venv at `/opt/stealth-browser-mcp/venv/`.
- **Fix**: Changed `mcp_servers.stealth-browser.command` from `python3` to `/opt/stealth-browser-mcp/venv/bin/python3`
- **Config file**: `<hermes-root>/config.yaml`
- **Verification**: Import test passed. Server starts correctly.
- **Pending**: Gateway restart required. Two active TUI sessions prevented restart during business hours.
- **Pattern**: This is the same class of issue as the mempalace python path fix from esc-run-20260601-2010. See `util-hermes-ops/references/mcp-python-path-mismatch.md`.

## Issues Reviewed — No Action Possible (All Require User)

| Issue | Tier | Status | User Action Required |
|---|---|---|---|
| oc_finch_weekly_manifest_401_20260531 | 3 | open | Update manifest.build API key. Next run: 2026-06-07 (Sunday). |
| skill_library_stubs | 4 | open | Confirm removal of 21 stub directories without SKILL.md |
| ocas-critique-missing-skillmd | 2 | open | Re-install ocas-critique from marketplace or remove directory |
| skill_hygiene_followup_20260601 | 2 | open | Same as skill_library_stubs |

## System Health
- Gateway: running (PID 186094), port 8080 responding
- Disk: 79% (76G/96G)
- state.db: 15GB (known bloat, non-critical)
- Active sessions: 2 (TUI slash workers)
- stealth-browser MCP: config fix applied, awaiting gateway restart

## Tool Issues
- `read_file` blocked in escalation-runner context (background review denied)
- `terminal` blocked in skill_manage context — use `skill_manage(action='write_file')` instead
- `kill` SIGHUP to gateway PID was effective but gateway didn't reload MCP config from new PID
