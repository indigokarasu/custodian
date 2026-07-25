# Self-Resolved Module Verification Pattern

## When to Use

A prior scan classified an error as "self-resolved" (a `ModuleNotFoundError` for `google`, `googleapiclient`, `google.oauth2`, etc. that supposedly resolved without intervention). Before accepting this classification as ground truth, independently verify.

## Why This Exists (2026-07-01)

During a light scan (2026-07-01), `dispatch:triage-morning` showed:
- `last_error`: `ModuleNotFoundError: No module named 'google'`
- Prior deep scan classification: "self-resolved, script runs with fallback to cached data"

The self-resolved claim could have been accepted without verification. Instead:
1. Checked the actual python PATH cron jobs use (`<hermes-venv>/bin/python3`)
2. Ran `python3 -c "import google.oauth2.credentials; print('OK')"` — returned OK
<<<<<<< Updated upstream
3. Found the script at `<hermes-home>/profiles/indigo/scripts/triage_morning.sh` (NOT at the previously-assumed path)
=======
3. Found the script at `~/.hermes/profiles/indigo/scripts/triage_morning.sh` (NOT at the previously-assumed path)
>>>>>>> Stashed changes
4. Confirmed the module IS importable → classification was correct

Without step 2, the scan would have:
- Relied on a stale journal entry from a different scan's methodology
- Potentially misclassified or escalated a non-issue

## Verification Procedure

```python
# Step 1: Find the actual python that cron jobs use
# Cron no_agent scripts run 'python3' from the shell PATH.
# Do NOT assume a profile venv path.
import subprocess
result = subprocess.run(['which', 'python3'], capture_output=True, text=True, timeout=5)
actual_python = result.stdout.strip()
print(f"Cron python: {actual_python}")

# Step 2: Check the actual import
result = subprocess.run(
    [actual_python, '-c', f'import {module_name}; print("OK")'],
    capture_output=True, text=True, timeout=10
)
print(f"stdout: {result.stdout}")
print(f"stderr: {result.stderr[:200]}")
print(f"exit: {result.returncode}")
# exit 0 = module available, exit 1 = still missing
```

## In Cron Context

```python
# Cannot use subprocess — use terminal() directly
import subprocess, sys
result = subprocess.run(['python3', '-c', 'import google.oauth2.credentials; print("OK")'], capture_output=True, text=True, timeout=10)
```

Or via terminal:
```
python3 -c "import google.oauth2.credentials; print('OK')"
```

## Pitfalls

<<<<<<< Updated upstream
- **Wrong venv assumption**: The profile venv at `<hermes-home>/profiles/<profile>/venv/bin/python3` may not exist. Cron uses PATH resolution, which typically resolves to `<hermes-venv>/bin/python3`.
=======
- **Wrong venv assumption**: The profile venv at `~/.hermes/profiles/<profile>/venv/bin/python3` may not exist. Cron uses PATH resolution, which typically resolves to `<hermes-venv>/bin/python3`.
>>>>>>> Stashed changes
- **Same job family ≠ same error**: Two "Script exited with code 1" errors from similar-sounding jobs (e.g., `email:check` and `dispatch:triage-morning`) can have completely different root causes (OAuth revocation vs. missing module). Read the COMPLETE `last_error` stderr traceback before classifying.
- **Self-resolved by what mechanism?**: A ModuleNotFoundError self-resolves when the module is installed (by a package install cron, manual pip install, or gateway restart import-window closing). If no such event occurred, the error may still be active — verify before accepting.