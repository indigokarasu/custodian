# Escalation Runner — 2026-06-04 18:07 UTC

## Summary

Scanned 6 open issues. Resolved 2 (google-workspace MCP binary, email:check stale error). 4 require user action (skill stubs, hygiene, ocas-critique, finch:weekly monitoring).

## Issues Resolved (2)

| Issue | Resolution |
|-------|------------|
| oc_mcp_google_workspace_missing_binary_20260604 | workspace-mcp-fixed wrapper existed but pointed to missing /usr/local/bin/workspace-mcp. Reinstalled workspace-mcp package (v1.21.1) via pip, created entry point at /usr/local/bin/workspace-mcp. Tested: both binaries respond to --help and JSON-RPC initialize. MCP server 'google_workspace' v3.2.4 operational. |
| oc_google_token_invalid_email_check_20260603_rev2 | Stale error — traceback references old script path, job field already fixed 2026-06-03, last_run_at=2026-06-01 predates fix. MCP credentials updated 2026-06-04T09:36. Error definitively stale. |

## User Action Required (4)

- **skill_library_stubs** (Tier 4) — 17-25 stub directories without SKILL.md. Requires user confirmation.
- **skill_hygiene_followup_20260601** (Tier 2) — Same + nested .git repos. Awaiting user response.
- **ocas-critique-missing-skillmd** (Tier 2) — No SKILL.md, no affected jobs. Requires user confirmation.
- **oc_finch_weekly_manifest_401_20260531** (Tier 3, monitoring) — API key valid, transient 401. Next run: 2026-06-07.

## System Health

- Gateway: ok
- Disk: 79% (75G/96G)
- state.db: 13.4GB (VACUUM complete, FTS trigram index is size consumer)
- Cron: 105 total, 8 stale errors, 0 fresh errors
- MCP google-workspace: **FIXED** — binary recreated, server responds to JSON-RPC

## Key Learnings

### 1. workspace-mcp entry point missing after pip install

The `workspace-mcp` Python package (v1.21.1) installs `main.py` at the top level of `/usr/local/lib/python3.13/dist-packages/` but the console script entry point (`/usr/local/bin/workspace-mcp`) may not be created if the package was previously installed and partially removed. The `workspace-mcp-fixed` wrapper script chains to `workspace-mcp`, so both must exist.

**Fix pattern:**
```bash
# Check if workspace-mcp binary exists
ls -la /usr/local/bin/workspace-mcp

# If missing, reinstall
pip install --force-reinstall workspace-mcp

# If still missing after reinstall, create manually:
cat > /usr/local/bin/workspace-mcp << 'SCRIPT'
#!/bin/bash
exec python3.13 -m main "$@"
SCRIPT
chmod +x /usr/local/bin/workspace-mcp

# Verify MCP server responds
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' | timeout 5 /usr/local/bin/workspace-mcp-fixed --single-user --transport stdio 2>/dev/null
```

### 2. issues.jsonl `id` vs `issue_id` field name inconsistency (reconfirmed)

When filtering issues.jsonl entries, always check BOTH `issue_id` and `id` fields:
```python
iid = e.get('issue_id', e.get('id', ''))
```

Matching only `issue_id` will silently skip entries that use `id` (e.g., `oc_google_token_invalid_email_check_20260603_rev2`). This session confirmed the pattern again — the first Python script matched only `issue_id` and reported "Wrote 64 lines" without actually updating the target entry.
