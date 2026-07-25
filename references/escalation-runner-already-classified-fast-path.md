# Escalation Runner Already-Classified Fast Path

When the escalation runner fires, the most common steady-state is: the same user-gated issue(s) persist from the prior run, and no new actionable issues have appeared. Re-verifying every open issue against `issues.jsonl` and the raw job state every 30 minutes is wasteful and produces identical journals.

## Fast-Path Trigger

After Step 1 (check latest esc-run journal), apply this shortcut:

1. Read the most recent esc-run journal (e.g., `esc-run-20260629T090530Z.json`)
2. If ALL of these conditions are true:
   - The journal classified every open issue as `open_user_gated` (not `open_actionable`)
   - The journal was written < 2 hours ago
   - No new fingerprints appear in `issues.jsonl` that weren't in the prior journal
   - The active job errors (from `jobs.json`) are the same set of `last_error` messages referenced in the journal
   → **Skip Steps 2-6 entirely.** Go directly to writing the delta journal and returning `[SILENT]`.

3. Write a follow-up esc-run journal with:
   - `type`: `escalation_runner`
   - `not_activity_reason`: `clean_verdict_all_issues_already_classified_user_gated`
   - `issues_processed`: reference the prior journal's classifications (don't restate — pointer is sufficient)
   - `fixes_applied`: 0
   - `escalation_needed`: false

## When NOT to use the fast path

- A new `last_error` appears on a job that wasn't in the prior journal
- A new entry appears in any `issues.jsonl` path
- The prior journal is > 2 hours old (>4 missed cron cycles)
- The prior journal classified any issue as `open_actionable`
- A fix was applied between the prior journal and now (config change, package install)
- Gateway restarted since prior journal

## Confirmed instances

| Date | Run | Issue | Prior classification |
|------|-----|-------|---------------------|
| 2026-06-29T16:41Z | esc-run-20260629T164131Z | `oc_google_oauth_token_revoked` (email:check, monitor:list) | esc-run-20260629T090530Z classified as `open_user_gated` |
| 2026-06-25 | Multiple scans | `oc_config_empty_section` (Pattern B) | First scan escalated, subsequent scans stayed silent (analogous pattern) |

## Why this matters without the fast path

Each full esc-run issues a `terminal()` call per `issues.jsonl` path (5+ paths), plus a `jobs.json` parse, plus a `find` for all `issues.jsonl` files. That's ~10-15 terminal calls that all return the same data. In cron context, each call is ~1-3 seconds. The full scan is 30-45 seconds of wasted work producing a journal identical to the prior one.

The fast path is a single `ls`/`cat` of one journal file + one `jobs.json` parse = ~5 seconds, same `[SILENT]` output.

## Journal write pattern reminder

**Always use Python `json.dump()` for writing journals in cron.** The heredoc pattern fails silently when it contains `$(date)` or other shell variables. The reliable pattern:

```python
import json, os
from datetime import datetime, timezone

now = datetime.now(timezone.utc)
run_id = f"esc-run-{now.strftime('%Y%m%dT%H%M%SZ')}"
date_dir = now.strftime('%Y-%m-%d')

journal = {
    "run_id": run_id,
    "timestamp": now.strftime('%Y-%m-%dT%H:%M:%SZ'),
    "type": "escalation_runner",
    ...
}

path = f'<hermes-home>/profiles/indigo/commons/journals/ocas-custodian/{date_dir}/{run_id}.json'
os.makedirs(os.path.dirname(path), exist_ok=True)
with open(path, 'w') as f:
    json.dump(journal, f, indent=2)
```

Do NOT use `cat > file << 'EOF'` for JSON with dynamic content — the single-quoted heredoc prevents variable expansion, producing files with literal `$(date)` and `$RUN_ID`.