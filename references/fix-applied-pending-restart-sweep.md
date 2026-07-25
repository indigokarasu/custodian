# Stale fix_applied_pending_restart Sweep

**Added 2026-06-17**: During a deep scan, `stealth_browser_mcp_nodriver_missing_20260601` was found stuck in `fix_applied_pending_restart` status for 16 days. The nodriver import actually worked the whole time — verified via `/opt/stealth-browser-mcp/venv/bin/python3 -c "import nodriver"`. The issue was never auto-closed because no step in the deep scan handles verification of `fix_applied_pending_restart` issues.

## The Gap

Step 10 (Escalation pass) only processes Tier 3/4 issues. `fix_applied_pending_restart` is neither Tier 3 nor Tier 4 — it's a "fix applied but needs restart" state that falls through the cracks. The escalation runner also didn't catch it.

## Procedure (Step 9b — between Web Search and Escalation passes)

For every issue across all `issues.jsonl` paths with `status=fix_applied_pending_restart`:

1. Check `resolved_at` or issue age — if >7 days old, proceed
2. Re-run the original verification:
   - **ModuleNotFoundError**: `<venv_python> -c "import <module>"`
   - **Dead script ref**: `ls <script_path>`
   - **Dead skill ref**: `ls <skill_dir>/SKILL.md`
   - **Config section**: check config.yaml for the key being non-null
3. If check passes: close as `resolved`, set `resolution: fix verified held after N days`
4. If check fails: demote to Tier 3, escalate with evidence

## MCP Server Verification Pattern

When verifying MCP server ModuleNotFoundError fixes, **always use the MCP server's own venv Python**, not the system Python or the agent's venv:

```bash
# stealth-browser MCP
/opt/stealth-browser-mcp/venv/bin/python3 -c "import nodriver; print('OK')"

# Other MCP servers: check config.yaml for the `command` field,
# extract the venv path from the Python binary path
```

Do NOT verify by checking file existence alone — the `.py` file can exist without the underlying package being importable.