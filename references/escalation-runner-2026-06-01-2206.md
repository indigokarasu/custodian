# Escalation Runner — 2026-06-01 22:06 UTC

## Summary

Scanned 35 issues (10 open/observation). Resolved 6 transient issues. 1 requires user action (finch:weekly manifest.build 401). 4 remain open requiring user confirmation (skill stubs, hygiene).

## Issues Resolved (6)

| Issue | Resolution |
|-------|------------|
| oc_http_429_rate_limit_scan_20260530 | No recent 429s. Transient. |
| oc_http_429_dispatch_briefing_20260530_new | No recent 429s. Transient. |
| oc_http_429_spot_watch_sweep_20260530_new | No recent 429s. Transient. |
| oc_auxiliary_nous_payment_20260530 | Zero Nous payment errors in recent logs. Resolved. |
| oc_mcp_google_workspace_keepalive_20260601 | Transient keepalive failures. On-demand MCP. |
| oc_stealth_browser_persistent_failure_20260601 | Transient TaskGroup failures. On-demand MCP. |

## User Action Required (1)

- **ocas-finch:weekly** — HTTP 401 from manifest.build. API key in config.yaml fallback_model.api_key is invalid. User must update at https://app.manifest.build.

## Awaiting User Confirmation (4)

- skill_library_stubs — 21 stub directories without SKILL.md
- skill_hygiene_followup_20260601 — Same + 35 nested .git repos
- ocas-critique-missing-skillmd — Data intact, no SKILL.md

## Tool Pitfall Hit

### Path.mkdir(parents_ok=True) TypeError

**Wrong:**
```python
journal_dir.mkdir(parents_ok=True, exist_ok=True)
```

**Correct:**
```python
journal_dir.mkdir(parents=True, exist_ok=True)
```

pathlib.Path.mkdir() takes parents=True (not parents_ok). This raised TypeError and crashed the journal-writing step. Fixed mid-run and documented in critical-pitfalls.md as pitfall 9e.

## Notes

- All Google OAuth token issues confirmed resolved.
- Disk at 79% — no longer critical.
- MCP server failures are all on-demand pattern, not persistent outages.
- finch:daily showing 429 (transient) — separate from weekly's 401 auth failure.
