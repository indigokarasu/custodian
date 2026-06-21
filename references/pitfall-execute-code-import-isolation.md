# Pitfall: execute_code Import Isolation

Each `execute_code` call runs in a **fresh Python process**. Imports from previous calls do NOT persist.

## Symptoms

- `NameError: name 're' is not defined` in a second `execute_code` block
- `NameError: name 'timedelta' is not defined` when `from datetime import timedelta` was only imported in a previous call
- `NameError: name 'json' is not defined` in any block after the first

## Root Cause

The Hermes `execute_code` tool spawns a new Python interpreter for each call. There is no shared state between calls — no imports, no variables, no function definitions survive.

## Fix

Every `execute_code` block must be **self-contained**:

```python
# ALWAYS include all needed imports at the top of EVERY block
import json, os, re, time, sqlite3
from datetime import datetime, timedelta, timezone
# ... rest of code
```

## Concrete Example (2026-05-20)

During a light scan, the first `execute_code` block imported `json, os, re` and parsed `jobs.json` successfully. The second block also needed `re` for regex cleaning but didn't include it — `NameError: name 're' not defined`. This is especially easy to miss during light scans where you're making 3-4 quick sequential calls.

## Priority

This is the **#1 cause of `NameError` crashes** during deep scans. Always double-check imports when a scan involves multiple sequential `execute_code` calls.
