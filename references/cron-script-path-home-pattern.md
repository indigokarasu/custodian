# Cron Script `Path.home()` Pattern — Detection and Fix

## The Problem

Python scripts that use `Path.home() / ".hermes"` as a default path break in cron/scheduled contexts because `HOME` may not be set or may not be `/root`. The `Path.home()` call resolves to the home directory of the user running the process, which in cron can be unpredictable.

**Broken pattern:**
```python
from pathlib import Path
AGENT_ROOT = Path(os.environ.get("AGENT_ROOT", Path.home() / ".hermes"))
```

**Fixed pattern:**
```python
from pathlib import Path
<<<<<<< Updated upstream
AGENT_ROOT = Path(os.environ.get("AGENT_ROOT", "<hermes-home>"))
=======
AGENT_ROOT = Path(os.environ.get("AGENT_ROOT", "~/.hermes"))
>>>>>>> Stashed changes
```

## Detection

Scan all scripts in the profile scripts directory:
```bash
<<<<<<< Updated upstream
grep -rn "Path.home()" <hermes-home>/profiles/indigo/scripts/ | grep -v ".pyc"
=======
grep -rn "Path.home()" ~/.hermes/profiles/indigo/scripts/ | grep -v ".pyc"
>>>>>>> Stashed changes
```

Also check the default scripts directory:
```bash
<<<<<<< Updated upstream
grep -rn "Path.home()" <hermes-home>/scripts/ | grep -v ".pyc"
=======
grep -rn "Path.home()" ~/.hermes/scripts/ | grep -v ".pyc"
>>>>>>> Stashed changes
```

## Known Instances (fixed 2026-06-04)

| Script | Line | Pattern | Status |
|--------|------|---------|--------|
| `email_check.py` | 12 | `Path.home() / ".hermes"` | Fixed 2026-06-03 |
| `dream_journal_pipeline.py` | 21 | `Path.home() / ".hermes"` | Fixed 2026-06-04 |
| `overnight_weave_enrichment.py` | 35 | `Path.home() / ".hermes"` | Fixed 2026-06-04 |
| `elephas_bridge_ingest.py` | 17 | `Path.home() / ".hermes"` | Fixed 2026-06-04 |
| `elephas_ingest.py` | 15 | `Path.home() / ".hermes"` | Fixed 2026-06-04 |
| `gateway_health_check.py` | 75 | `Path.home() / ".hermes"` | Fixed 2026-06-04 |

## Fix Procedure

For each file found:
1. Read the file via `terminal(command="cat /path/to/file.py")`
2. Identify the exact `Path.home() / ".hermes"` pattern
<<<<<<< Updated upstream
3. Replace with hardcoded `<hermes-home>`
=======
3. Replace with hardcoded `~/.hermes`
>>>>>>> Stashed changes
4. Verify with a follow-up grep

**Note:** The `email_check.py` fix was applied on 2026-06-03 by a previous escalation run. The remaining 5 were found and fixed on 2026-06-04.

## Related Pitfalls

<<<<<<< Updated upstream
- `references/cron-script-path-security-model.md` — **IMPORTANT**: The Hermes security model validates script paths against `$HERMES_HOME/scripts/`. When `HERMES_HOME=<hermes-home>/profiles/indigo` (set by systemd), scripts must be at `<hermes-home>/profiles/indigo/scripts/`. Do NOT point to `<hermes-home>/scripts/` — it will be blocked.
=======
- `references/cron-script-path-security-model.md` — **IMPORTANT**: The Hermes security model validates script paths against `$HERMES_HOME/scripts/`. When `HERMES_HOME=~/.hermes/profiles/indigo` (set by systemd), scripts must be at `~/.hermes/profiles/indigo/scripts/`. Do NOT point to `~/.hermes/scripts/` — it will be blocked.
>>>>>>> Stashed changes

- `references/cron-script-environment-pitfalls.md` — Full catalog of cron environment issues
- `references/critical-pitfalls.md` pitfall #9e — `parents_ok` vs `parents=True` in `Path.mkdir()`