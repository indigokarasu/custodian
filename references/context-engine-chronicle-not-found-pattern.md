# Context Engine 'Chronicle' Not Found Pattern

**Fingerprint:** `oc_context_engine_chronicle_not_found`
**Tier:** 2 (Surface Only — Requires Investigation)

## Description

"Context engine 'chronicle' not found — falling back to built-in compressor" warnings in `errors.log`. The count can reach 200+ per day.

## Root Cause

The Chronicle context engine plugin is not being loaded. This can happen when:
1. The plugin directory `plugins/context_engine/chronicle/` is empty (no `.py` files, only `__pycache__/`)
2. The plugin is installed but not discovered by the plugin loader
3. The plugin code has a bug preventing initialization

**Note:** The kwargs bug (`oc_chronicle_kwargs_get_20260604`) was resolved, but the engine still isn't loading. This suggests the issue is NOT the code bug but rather the plugin not being present or discoverable.

## Diagnostic

1. Check plugin directory: `ls -la <hermes-home>/plugins/context_engine/chronicle/`
2. If empty (only `__pycache__/`): plugin files were removed or never installed
3. If `.py` files exist: check plugin discovery config in `config.yaml`
4. Check if the plugin is listed in `hermes plugins list`

## Related Patterns

| Fingerprint | Description | Distinction |
|---|---|---|
| `oc_chronicle_plugins_empty` | Plugin directory has no `.py` files | Structural — files missing |
| `oc_context_engine_chronicle_not_found` | Engine not loaded (may or may not be empty dir) | Runtime — discovery/init failure |
| `oc_chronicle_kwargs_get_duplicate` | kwargs.get() should be kwargs.pop() | Code bug (resolved 2026-06-04) |

## Action

Surface in report. No auto-fix — requires determining if the Chronicle context engine should be installed/enabled. If the built-in compressor is acceptable, this can be left as-is.

## Verified Instances

| Date | Count | Plugin Dir Status | Action |
|------|-------|-----------------|--------|
| 2026-06-13 | 203 | Investigation needed | Escalated — user decision required |