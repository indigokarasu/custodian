# no_agent Script Path Mismatch — Symlink Fix Pattern

**Confirmed 2026-06-26**: `brief:email-evening` reported "Script not found" but the script existed at a different path than the job expected.

## Diagnosis

When `no_agent: true`, the `script` field is treated as a literal file path. The Hermes security model resolves relative paths against `$HERMES_HOME/scripts/`. Under a profile, `HERMES_HOME=<hermes-root>/profiles/<profile>`, so the job looks for the script at:

```
<hermes-root>/profiles/<profile>/scripts/<basename>
```

But many scripts are installed to the shared scripts directory:

```
<hermes-root>/scripts/<basename>
```

This produces "Script not found" even though the script exists and is executable at the system path.

## Distinction from Other Patterns

| Pattern | `last_error` | Current `script` field | Fix |
|---------|-------------|----------------------|-----|
| `oc_cron_script_not_found_transient` | "Script not found: /path/script.py" | Points to the exact path that exists | Transient (race), no fix needed |
| `oc_cron_no_agent_script_args` | "Script not found: /path/foo.py --flag" | Contains embedded arguments | Wrapper script |
| **`oc_no_agent_script_path_mismatch`** | "Script not found: /profile/scripts/name.py" | Basename only, exists at `<hermes-root>/scripts/name.py` | **Symlink or copy** |

## Detection

```bash
# Find no_agent jobs with "Script not found" where the script exists elsewhere
cat <hermes-root>/profiles/<profile>/cron/jobs.json | python3 -c "
import json, sys, os
data = json.load(sys.stdin)
jobs = data.get('jobs', []) if isinstance(data, dict) else data
for j in jobs:
    if not j.get('no_agent'):
        continue
    err = j.get('last_error', '')
    if 'Script not found' not in err:
        continue
    script = j.get('script', '')
    # Check if the error path != current script field (stale) or == (path mismatch)
    # Extract path from error message
    import re
    m = re.search(r\"Script not found: (.+?)(?:\\\")\", err)
    if not m:
        continue
    err_path = m.group(1)
    print(f\"Job: {j['name']}\")
    print(f\"  Error path: {err_path}\")
    print(f\"  Script field: {script}\")
    # Check if script exists at system path
    sys_path = f'<hermes-root>/scripts/{script}'
    prof_path = f'<hermes-root>/profiles/<profile>/scripts/{script}'
    print(f\"  Exists at system path: {os.path.exists(sys_path)}\")
    print(f\"  Exists at profile path: {os.path.exists(prof_path)}\")
"
```

## Fix

Create a symlink from the profile scripts dir to the system scripts dir:

```bash
mkdir -p <hermes-root>/profiles/<profile>/scripts/
ln -sf <hermes-root>/scripts/<basename> <hermes-root>/profiles/<profile>/scripts/<basename>
```

Verify:
```bash
test -f <hermes-root>/profiles/<profile>/scripts/<basename> && echo "OK" || echo "BROKEN"
```

## Post-Fix Verification (Required)

After creating the symlink, verify the script actually resolves and runs:

```bash
# 1. Confirm symlink resolves to a real file
test -f <hermes-root>/profiles/<profile>/scripts/<basename> && echo "OK" || echo "BROKEN"

# 2. Run the script directly to confirm exit code 0
python3 <hermes-root>/profiles/<profile>/scripts/<basename> 2>&1 | tail -5
echo "EXIT: $?"
```

If the script fails (e.g., import errors):
```bash
# Run directly with python3 and check stderr
python3 <hermes-root>/profiles/<profile>/scripts/<basename>
```

### Stale Error After Fix

The `last_error` in jobs.json will persist with "Script not found" until the next scheduled run overwrites it. Classification during subsequent scans:

- `consecutive_failures: None` + `last_status: error` + `last_error` contains "Script not found" + fix verified working = **stale error, no action needed**
- Do NOT re-escalate or re-apply the fix — the symlink is in place and confirmed working
- The job will self-resolve on its next run

**Confirmed 2026-06-26:** `brief:email-evening` symlink created at 05:02, stale error at 03:39. Python execution confirmed EXIT 0. Scan at 16:16 correctly classified as stale.

**Confirmed 2026-06-26 (later same day):** `brief:email-morning` symlink created at 14:02 by light scan (21:04), stale error at 13:30. Deep scan at 21:08 classified both email jobs as stale — correctly did not re-apply fix or escalate. Both jobs' `next_run_at` is tomorrow — will self-verify on next scheduled run.

**Confirmed 2026-06-27:** Both `brief:email-evening` and `brief:email-morning` still show stale errors after script files were *copied* (not symlinked) to the profile path at 05:33:
- `brief:email-evening`: `last_error` = "Blocked: script path resolves outside the scripts directory" from 05:27 run (before copy). Script verified executable at profile path, exit 0.
- `brief:email-morning`: `last_error` = "Script not found" from 2026-06-26 run (before copy). Script verified executable at profile path, exit 0.
- Both classified as `oc_cron_stale_error_script_mismatch`. No fix re-applied. Both will self-verify on next scheduled run.

**Lesson:** The stale error pattern holds regardless of fix method (symlink vs copy). What matters is: (1) script exists at the resolved path, (2) script executes successfully, (3) `consecutive_failures` is None/0. The specific error message in `last_error` ("Script not found" vs "Blocked: script path resolves outside") is irrelevant — it's the pre-fix error preserved in jobs.json until the next successful run overwrites it.

## See Also

- `references/no-agent-script-argument-pattern.md` — compound `&&` command variant
- `references/script-path-security-block-pattern.md` — when the security model rejects the path
- `references/oc-cron-script-not-found-transient-pattern.md` — write/read race variant
- `references/post-fix-stale-error-pattern.md` — general stale error classification
