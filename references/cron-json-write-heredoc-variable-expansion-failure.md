# Cron JSON Write: Heredoc Variable Expansion Failure

Confirmed 2026-06-24: Escalation runner journal write produced a corrupted file.

## Symptom

Using `cat > /path/to/file << 'EOF'` (single-quoted heredoc) to write JSON containing dynamic content:

```bash
cat > /tmp/esc_run_$(date -u +%Y%m%dT%H%M%SZ).json << 'EOF'
{
  "run_id": "esc-run-$(date -u +%Y%m%dT%H%M%SZ)",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  ...
}
EOF
```

Result: File contains LITERAL `$(date -u +%Y%m%dT%H%M%SZ)` — shell does not expand variables inside single-quoted heredocs.

## Root Cause

- `<< 'EOF'` (single-quoted): NO shell expansion (variables, command substitution, etc.)
- `<< EOF` (unquoted): Shell expansion BUT escapes like `\n`, `\t` are interpreted, breaking JSON
- Neither works for JSON with dynamic content

## Fix

**Always use Python for JSON writes in cron:**

```python
python3 -c "
import json, datetime
now = datetime.datetime.now(datetime.timezone.utc)
run_id = f'esc-run-{now.strftime(\"%Y%m%dT%H%M%SZ\")}'
ts = now.strftime('%Y-%m-%dT%H:%M:%SZ')
journal = {
    'run_id': run_id,
    'timestamp': ts,
    'type': 'escalation_runner',
    ...
}
import os
dir_path = '<hermes-root>/commons/journals/ocas-custodian/' + now.strftime('%Y-%m-%d')
os.makedirs(dir_path, exist_ok=True)
file_path = os.path.join(dir_path, f'{run_id}.json')
with open(file_path, 'w') as f:
    json.dump(journal, f, indent=2)
print(f'Written: {file_path}')
"
```

Or write a script to `/tmp/` via `write_file` then invoke `python3 /tmp/script.py`.

## When Heredoc IS Safe

- Static content with no variables, no timestamps, no dynamic values
- Shell scripts (not JSON) where you want literal content
- Example: writing a fixed Python script to `/tmp/` that doesn't contain timestamps

## Recommended Pattern for Cron Journal Writes (2026-06-25)

For large JSON (observation journals with error_classification objects), `python3 -c "..."` becomes unwieldy with nested quotes. Use `terminal(command='python3 << \'PYEOF\' ...')` instead:

```
terminal(command='''python3 << 'PYEOF'
import json, os
from datetime import datetime, timezone

now = datetime.now(timezone.utc)
run_id = f'light-scan-{now.strftime("%Y%m%dT%H%M%SZ")}'
journal_dir = f'<hermes-home>/commons/journals/ocas-custodian/{now.strftime("%Y-%m-%d")}'
os.makedirs(journal_dir, exist_ok=True)

journal = {
    "run_id": run_id,
    "timestamp": now.isoformat(),
    "scan_type": "light",
    # ... full journal content ...
}

fpath = os.path.join(journal_dir, f'{run_id}.json')
with open(fpath, 'w') as f:
    json.dump(journal, f, indent=2)
print(f'Written: {fpath}')
PYEOF''')
```

This avoids all quote-escaping issues and keeps the JSON readable.

## Recovery

If a corrupted file is detected:
1. `rm` the broken file
2. Rewrite using Python
3. Verify with `cat <file> | python3 -m json.tool`
