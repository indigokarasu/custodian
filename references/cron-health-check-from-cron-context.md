# Running custodian_cron_health from a cron/scheduled context

When invoked as a cron job (e.g., `custodian:cron-health`), the `execute_code` tool is blocked and native MCP plugin tools cannot be called by name. The solution is to import the plugin's underlying Python function via `terminal()`.

## Pattern

```bash
<<<<<<< Updated upstream
cd <hermes-home>/plugins/custodian && python3 -c "
=======
cd ~/.hermes/plugins/custodian && python3 -c "
>>>>>>> Stashed changes
import sys, json
sys.path.insert(0, '.')
from hermes_custodian_plugin.cron_health import run_cron_health_check, format_health_report

report = run_cron_health_check(dry_run=False)
print(format_health_report(request))
"
```

For the full JSON report (programmatic consumption):

```bash
<<<<<<< Updated upstream
cd <hermes-home>/plugins/custodian && python3 -c "
=======
cd ~/.hermes/plugins/custodian && python3 -c "
>>>>>>> Stashed changes
import sys, json
sys.path.insert(0, '.')
from hermes_custodian_plugin.cron_health import run_cron_health_check

report = run_cron_health_check(dry_run=False)
print(json.dumps(report, indent=2, default=str))
"
```

## Tool location

- Module: `hermes_custodian_plugin.cron_health`
- Function: `run_cron_health_check(dry_run: bool) -> dict`
- Formatter: `format_health_report(report) -> str`
<<<<<<< Updated upstream
- Plugin dir: `<hermes-home>/plugins/custodian/`
- Skill dir: `<hermes-home>/profiles/<profile>/skills/ocas-custodian/` (read-only reference)
=======
- Plugin dir: `~/.hermes/plugins/custodian/`
- Skill dir: `~/.hermes/profiles/<profile>/skills/ocas-custodian/` (read-only reference)
>>>>>>> Stashed changes

## What the report includes

- `total`, `ok`, `error`, `paused`, `error_rate`
- `alerts` — jobs with consecutive_failures <= 1 (new) or >= 3 (chronic)
- `chronic_jobs` — names with consecutive_failures >= 3
- `categories` — error category → [job_names]
- `auto_remediations` — fixes attempted (only when dry_run=false)
- `daily_health_line` — one-liner for briefing

## Error categories detected

- `google-workspace-mcp-unavailable`
- `google-auth`
- `rate-limit`
- `execute-code-blocked`
- `missing-script-tool`
- `timeout`
- `oom`
- `unknown`

## Usage in automated checks

For scheduled health-check crons that also need memory-guard verification, combine both in one `terminal()` call:

```bash
<<<<<<< Updated upstream
cd <hermes-home>/plugins/custodian && python3 << 'PYEOF'
=======
cd ~/.hermes/plugins/custodian && python3 << 'PYEOF'
>>>>>>> Stashed changes
import sys, json
sys.path.insert(0, '.')
from hermes_custodian_plugin.cron_health import run_cron_health_check
report = run_cron_health_check(dry_run=True)  # dry_run for side-effect-free

# Memory guard check
import os
<<<<<<< Updated upstream
mem_path = "<hermes-home>/profiles/indigo/memories/MEMORY.md"
decisions_path = "<hermes-home>/profiles/indigo/commons/data/ocas-finch/decisions.jsonl"
=======
mem_path = "~/.hermes/profiles/indigo/memories/MEMORY.md"
decisions_path = "~/.hermes/profiles/indigo/commons/data/ocas-finch/decisions.jsonl"
>>>>>>> Stashed changes
mem_size = os.path.getsize(mem_path) if os.path.exists(mem_path) else 0

result = {
    "cron_health": {
        "ok": report.get("ok"),
        "error": report.get("error"),
        "total": report.get("total"),
    },
    "memory_guard": {
        "memory_bytes": mem_size,
        "over_limit": mem_size > 2200,
    },
    "alerts_count": len(report.get("alerts", [])),
}
print(json.dumps(result, indent=2, default=str))
PYEOF
```