# Chronicle Plugin Directories Empty Pattern

## Fingerprint: `oc_chronicle_plugins_empty`

## Description

The Chronicle plugin directories exist but contain no `.py` source files — only `__pycache__/` with stale `.pyc` artifacts:

```
plugins/memory/chronicle/          → only __pycache__/
plugins/context_engine/chronicle/  → only __pycache__/
```

These directories are **not tracked by git** in the hermes-agent HEAD (verified via `git show HEAD:plugins/memory/chronicle/` → "exists on disk, but not in HEAD"). They were likely installed separately (e.g., via a skill package or manual copy) and were removed/lost during a gateway update or cleanup operation.

## Detection

```bash
for d in /usr/local/lib/hermes-agent/plugins/memory/chronicle/ /usr/local/lib/hermes-agent/plugins/context_engine/chronicle/; do
  count=$(find "$d" -maxdepth 1 -name "*.py" -not -path "*__pycache__" 2>/dev/null | wc -l)
  [ "$count" -eq 0 ] && echo "EMPTY CHRONICLE PLUGIN DIR: $d"
done
```

## Impact

- **ChronicleContextEngine** `on_session_start()` and `initialize()` methods are unavailable or fail silently
- **ChronicleMemoryProvider** cannot function without its source files
- May manifest as `TypeError: got multiple values for keyword argument 'hermes_home'` (if partial code remains) OR silent degradation with no Chronicle functionality
- Non-fatal: Chronicle degrades gracefully; sessions proceed without Chronicle enrichment

## Distinction from `oc_chronicle_kwargs_get_duplicate`

That pattern is a **code bug** (`kwargs.get` vs `kwargs.pop`) in files that exist on disk. This pattern is **missing files** — the directory skeleton remains but the source is gone. Both affect Chronicle initialization but have completely different root causes and fix directions.

## Classification

**Tier 2** — Surface in report, escalate if first occurrence. Not auto-fixable because:
1. Files are not in git history, so `git checkout` cannot restore them
2. The original installation method is unknown
3. Blindly recreating `.py` files risks version mismatch with the installed hermes-agent version

## Fix Direction

1. Determine the intended Chronicle plugin version (check hermes-agent version: `hermes --version`)
2. Check if Chronicle was installed via a skill package, pip package, or manual copy
3. Re-install the Chronicle plugin package appropriate for the current hermes-agent version
4. Verify restored files match the expected API surface

## First Observed

2026-06-05 (light scan): Both directories empty. `.pyc` files in `__pycache__` confirm files existed previously. Issue tracked as `chronicle_plugin_dirs_empty_20260605` in `issues.jsonl`.