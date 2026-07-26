# monitor:list Exit-1 Masking Variant

Sub-pattern of the Step-6 no_agent exit-1 de-aggregation (confirmed 2026-07-14).

## The trap
`monitor_list.py` (no_agent, job 39b7edc44b35) exits 1 every run with **empty stdout AND empty stderr** — cron `last_error` is just `Script exited with code 1`. Its only covering issues (`oc_google_tasks_api_403_forbidden`, `oc_cron_no_agent_exit_1_ambiguous_20260712T040120`) were both already `resolved`, so a naive match reports it "covered" and skips it — a silent monitoring gap.

Root cause of the silence: the wrapper runs the real worker as a subprocess and converts ANY non-zero to `sys.exit(1)` with no output forwarded:
```python
result = subprocess.run([sys.executable, SCRIPT, "--mode", "check"], capture_output=True, text=True, timeout=15)
if result.returncode != 0:
    sys.exit(1)   # masks the wrapped traceback
```

## De-aggregation recipe
Run the WRAPPED script directly, not the wrapper:
```
timeout 60 python3 <hermes-home>/profiles/indigo/skills/ocas-tasks/scripts/tasks_monitor.py --mode check
```
Live failure surfaced:
```
File ".../tasks_monitor.py", line 88, in get_access_token
    return creds["access_token"]
KeyError: 'access_token'
```
Failure at 16:55Z — AFTER both prior monitor:list issues resolved (07:07Z / 04:02Z) → distinct live signature, no open issue covers it.

## Step 8b/8e closure
When an error job is referenced ONLY by resolved issues and its live re-run shows a NEW signature:
- It is a persistence GAP (Step 8b: flagged-but-never-persisted; Step 8e: distinct root cause vs resolved issues).
- Persist a NEW issue with the live signature (`oc_google_tasks_access_token_missing`), `status: user_gated`, `escalation_needed: true`. `email:check` (same credential file) was OK → narrow to the Tasks-scoped token (1 job), not a broad OAuth revocation.
- Write append-only; re-verify the issue_id does not already exist at write time.

## Race-safe issues.jsonl parser (cron)
Concurrent sibling custodian:light cron runs rewrite `issues.jsonl` at top of hour. Ad-hoc `python3 << 'PYEOF'` parsers that hand-walk braces with backslash-escape handling RETURN 0 OBJECTS (the `ins`/escape logic breaks on `\"`). Use `json.JSONDecoder().raw_decode` instead of a manual char-walk:
```python
import json
dec = json.JSONDecoder()
i, n, out = 0, len(raw), []
while i < n:
    while i < n and raw[i] in ' \r\n\t': i += 1
    if i >= n: break
    obj, end = dec.raw_decode(raw, i); out.append(obj); i = end
```
Robust to escaped quotes; safe for issues.jsonl in cron.