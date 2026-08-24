# Light Scan Session — 2026-05-19

## New Patterns Discovered

### `rally:daily` — Undeclared Cron Job

A cron job `rally:daily` exists in jobs.json with `last_status: null`, `last_run: null`, `next_run_at: 2026-05-20T06:00:00-07:00`. It is **NOT declared** in ocas-rally SKILL.md (neither in `## Background tasks` table nor in frontmatter `metadata.hermes.cron` array).

The ocas-rally frontmatter only declares: `rally:research`, `rally:healthcheck-pre-open`, `rally:healthcheck-pre-close`, `rally:update`.

**Action**: Tier 2 observation. This may be a user-created job. Do NOT remove — it has a future `next_run_at` indicating it was intentionally created. Surface in report only.

### `kalshi_portfolio.py` — Prompt-Based Job Path Mismatch

The `bones:market-monitor` cron job (ID: `bbb15fb19401`) is a prompt-based job (`script: null`) whose prompt instructs the agent to run `python3 $HERMES_INSTALL/scripts/kalshi_portfolio.py`. That file does not exist at that path.

**Actual location**: `<hermes-home>/skills/ocas-bones/scripts/kalshi_portfolio.py`

**Error in logs**:
```
python3: can't open file '$HERMES_INSTALL/scripts/kalshi_portfolio.py': [Errno 2] No such file or directory
```

**Classification**: Tier 2 — prompt-based job with dead script reference. Cannot auto-fix without knowing the correct path. The job's `last_status` was `ok` at time of scan (the error was from a previous run), suggesting the agent may have found a workaround.

**Diagnostic pattern**: When checking prompt-based jobs (`script: null`), scan the `prompt` field for `python3 /path/to/script.py` patterns and verify the referenced scripts exist. This is in addition to checking the `script` field.

### `bones:lirr-watch` — Disabled, Undeclared Job

`bones:lirr-watch` is disabled (`enabled: false`) and is NOT declared in ocas-bones SKILL.md. `last_status: ok`, `last_run: 2026-05-19T16:06:54`. This was likely user-disabled intentionally. No action needed.

## Scan Metrics

- Total cron jobs: 82
- Jobs with `last_status=error`: 9 (mostly transient)
- Open issues: 0
- Uninitialized skills: 0
- Failed fixes to retry: 0
- Gateway: healthy (--replace takeover pattern)

## execute_code Pitfall Reminder

The `execute_code` sandbox hit a `NameError: name 'data' is not defined` when a variable was referenced before assignment. This is the **execute_code import isolation** pitfall — each call runs in a fresh Python process. Always include all variable definitions at the top of every block.

In this case, the bug was a copy-paste error where `data` was used before being assigned. Always verify variable assignment order in execute_code blocks.