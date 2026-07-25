# Subdirectory Hints Home Directory Resolution Pattern

**Fingerprint**: `oc_subdirectory_hints_home_dir`
**Tier**: 2 (surface only, escalate if recurring)
**First seen**: 2026-06-17T03:13:02-07:00

## Pattern

```
RuntimeError: Could not determine home directory
  File ".../subdirectory_hints.py", line ~130, in _add_path_candidate
    Path(raw_path).expanduser()
```

## Root Cause

`Path.expanduser()` relies on `$HOME` being set in the environment. In cron execution contexts, `$HOME` may not be exported, causing `expanduser()` to raise `RuntimeError`.

## Classification

- **Non-fatal**: Agent handles the error gracefully.
- **Framework bug**: hermes-agent's `subdirectory_hints.py` should guard against unset `$HOME`.
- **Distinct from**: Path security blocks (which are about script path validation, not home dir resolution).

## Fix Applied (2026-06-17)

<<<<<<< Updated upstream
**One-line patch** to `<hermes-agent>/agent/subdirectory_hints.py` line 147:
=======
**One-line patch** to `<fs-root>/hermes-agent/agent/subdirectory_hints.py` line 147:
>>>>>>> Stashed changes

```diff
-        except (OSError, ValueError):
+        except (OSError, ValueError, RuntimeError):
             pass
```

The `expanduser()` call on line 130 is already inside a `try` block, but the except clause only caught `OSError` and `ValueError`. When `$HOME` is unset (common in cron environments), `expanduser()` raises `RuntimeError` which was unhandled. Adding `RuntimeError` to the except tuple is the minimal fix.

**Verified**: After patch, 0 new occurrences in errors.log (was 4 before fix).

## Installed Copy vs Editable Source (2026-06-17 Lesson)

<<<<<<< Updated upstream
When hermes-agent is installed in editable mode (`pip install -e`), Python imports resolve to the **source checkout** at `<hermes-agent>/agent/`, NOT the installed copy at `/usr/local/lib/hermes-agent/agent/`. However, **both copies exist** and both may be loaded depending on the import path:

- **Editable source** (loaded by the agent at runtime): `<hermes-agent>/agent/subdirectory_hints.py`
=======
When hermes-agent is installed in editable mode (`pip install -e`), Python imports resolve to the **source checkout** at `<fs-root>/hermes-agent/agent/`, NOT the installed copy at `/usr/local/lib/hermes-agent/agent/`. However, **both copies exist** and both may be loaded depending on the import path:

- **Editable source** (loaded by the agent at runtime): `<fs-root>/hermes-agent/agent/subdirectory_hints.py`
>>>>>>> Stashed changes
- **Installed copy** (may be loaded by other processes): `/usr/local/lib/hermes-agent/agent/subdirectory_hints.py`

**Both must be patched.** The editable source had the fix (RuntimeError in except) but the installed copy did not. Errors continued until both were patched and the stale `.pyc` at `/usr/local/lib/hermes-agent/agent/__pycache__/` was cleared.

**Detection**: Check which file is actually loaded:
```bash
python3 -c "import importlib.util; spec = importlib.util.find_spec('agent.subdirectory_hints'); print(spec.origin)"
```

**Fix both locations**:
```bash
# Editable source (if not already fixed)
<<<<<<< Updated upstream
grep -n "except.*OSError.*ValueError" <hermes-agent>/agent/subdirectory_hints.py
=======
grep -n "except.*OSError.*ValueError" <fs-root>/hermes-agent/agent/subdirectory_hints.py
>>>>>>> Stashed changes

# Installed copy (often missed)
grep -n "except.*OSError.*ValueError" /usr/local/lib/hermes-agent/agent/subdirectory_hints.py

# Patch all except clauses in installed copy
sed -i 's/except (OSError, ValueError):/except (OSError, ValueError, RuntimeError):/g' /usr/local/lib/hermes-agent/agent/subdirectory_hints.py

# Clear stale .pyc
rm -f /usr/local/lib/hermes-agent/agent/__pycache__/subdirectory_hints*.pyc
```

## Fix Direction (for future reference)

1. **Framework patch** (preferred, confirmed working): Add `RuntimeError` to the except clause in `_add_path_candidate`.
2. **Cron env workaround**: Ensure `$HOME` is exported in cron execution environment.

## Escalation Criteria

- `recurrence_count >= 2`: Escalate to Tier 3 with framework patch proposal.
- `recurrence_count == 1`: Surface in report, monitor for recurrence.

## Affected Jobs

Any cron job that triggers agent sessions where the agent's tool path resolution goes through `subdirectory_hints.py`. First observed in `custodian:cron-health` job.