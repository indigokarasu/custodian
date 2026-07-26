# Config Null Key Verification Pattern

## Problem

PyYAML's `yaml.safe_load()` can return stale/null values for config keys that are actually populated on disk. This happens when a terminal session caches file content and reads a different state than what's actually written.

**Confirmed 2026-06-24:** Deep scan found `fallback_model: None`, `mcp: None`, `max_concurrent_sessions: None` via PyYAML, which would have triggered the `oc_config_empty_section` fix-loop logic (4th occurrence = Tier 3 escalation). Raw file verification (`grep`, `sed -n`) showed these keys were NOT null — `mcp:` was a dict with api_key/base_url/model/provider/timeout, and `fallback_model`/`max_concurrent_sessions` didn't exist as top-level keys at all.

## Root Cause

Terminal tool sessions can cache file reads. `terminal(command='python3 -c "..."')` may read a different file state than `terminal(command='grep ...')` moments later.

## Verification Procedure

**Before concluding config has null/empty sections, ALWAYS verify against raw file:**

```bash
# Method 1: Direct grep for null-value keys at column 0
python3 -c "
with open('<hermes-home>/profiles/<profile>/config.yaml') as f:
    for i, line in enumerate(f, 1):
        if line[0] not in (' ', '\t', '#', '\n', '-'):
            stripped = line.rstrip()
            if stripped.endswith(': null') or stripped.endswith(': ~'):
                print(f'Line {i}: {stripped}')
"

# Method 2: If PyYAML says None, confirm with sed
sed -n '/^fallback_model:/p' <hermes-home>/profiles/<profile>/config.yaml
sed -n '/^max_concurrent_sessions:/p' <hermes-home>/profiles/<profile>/config.yaml
```

**If PyYAML and raw file disagree, raw file is authoritative.** Do NOT apply config fixes based solely on PyYAML output in cron/scheduled contexts.

## Also applies to

- `jobs.json` — `json.load()` via terminal() can similarly cache. Re-read with `grep` or `head` if results seem stale.
- Any config read immediately after a gateway restart — the restart may have changed config state between the PyYAML read and the actual file on disk.

## Pitfall

If `oc_config_empty_section` fix-loop detection relies on PyYAML alone, a session that reads stale null values will:
1. Incorrectly conclude the fix didn't hold (or wasn't applied)
2. Re-apply the fix (writing `null` back into a key that wasn't null)
3. OR escalate to Tier 3 based on phantom null keys

Either outcome is a self-inflicted error. Always verify raw.