# Escalation Runner Concurrent Execution Gap

## Problem

The escalation runner schedule (`*/30 9-17 * * 1-5`) can produce multiple runs per day. When Runner A resolves an issue, Runner B (which may have started earlier or be running concurrently) can still flag the same issue as "open" or "framework bug" in its journal output. This creates contradictory journal entries and wastes cycles re-investigating already-resolved issues.

## Observed Instance (2026-06-04)

- `esc-run-20260604-1618-final` resolved `oc_cron_script_path_block` by updating 8 job script paths in jobs.json
- `esc-run-20260604-1539` (timestamped later but started earlier) still flagged it as "framework bug"
- `esc-run-20260604-2010` also flagged it as unresolved

The 1539 and 2010 runners did not check whether a sibling runner had already resolved the issue.

## Diagnostic Pattern

When an escalation runner finds an issue flagged as "open" or "cannot fix":

1. **Before** classifying the issue, list all esc-run journals for the current day:
   ```bash
   ls -la journals/ocas-custodian/YYYY-MM-DD/esc-run-*.json
   ```
2. **Sort by timestamp** (not filename — filenames use creation time, not completion time)
3. For the specific `issue_id`, find the **most recent** journal entry that references it
4. If the most recent entry shows `status: resolved` or `action_taken: resolved_*`, skip the issue — it's already handled
5. Only create a new journal entry if your assessment differs from the latest one AND you have new evidence

## Prevention

- Always read the latest esc-run journal for an issue before adding a new assessment
- If resolving an issue, update `issues.jsonl` immediately (set `status: resolved`, `resolved_at`, `resolution_note`) — don't rely on journal entries alone
- Journal entries should reference the `issues.jsonl` entry they're updating

## Related

- `references/critical-pitfalls.md` — general pitfalls affecting all operations
- `references/escalation-runner-2026-06-04-1807.md` — workspace-mcp binary fix (example of successful resolution)
