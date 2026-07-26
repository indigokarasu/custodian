# `oc_hook_post_tool_call_task_id` Pattern

## Status: RESOLVED (2026-06-09)

This was a Tier 2 issue — non-fatal but noisy (2,367+ occurrences of `post_tool_call` alone).

## Root Cause (CORRECTED)

The hooks already had `**kwargs` but `ctx` was a **required positional argument**. The Hermes framework calls hooks without a positional `ctx` argument (it passes everything as kwargs or doesn't pass `ctx` at all). This caused:

```
TypeError: _hook_post_tool_call() missing 1 required positional argument: 'ctx'
TypeError: _hook_on_session_start() missing 1 required positional argument: 'ctx'
TypeError: _hook_on_session_end() missing 1 required positional argument: 'ctx'
```

The fix is to make `ctx` optional with a default value of `None`:

```python
# Before — crashes: ctx is required positional
def _hook_post_tool_call(ctx, tool_name: str, args: dict, result: Any, **kwargs) -> None:

# After — resilient: ctx is optional
def _hook_post_tool_call(ctx=None, tool_name: str = "", args: dict | None = None, result: Any = None, **kwargs) -> None:
def _hook_on_session_start(ctx=None, **kwargs) -> None:
def _hook_on_session_end(ctx=None, **kwargs) -> None:
def _hook_on_session_reset(ctx=None, **kwargs) -> None:
```

## CRITICAL: Editable Install Path vs Plugin Directory

**The active plugin code may NOT be at the path you expect.** Hermes uses editable installs (`pip install -e`) which map to a different path via a finder module.

To find the actual loaded path:
```python
import importlib
spec = importlib.util.find_spec('hermes_custodian_plugin')
print(spec.origin)  # THIS is the file you need to edit
```

In this session, the locations were:
- **Editable (ACTIVE):** `<hermes-home>/profiles/indigo/home/.hermes/plugins/custodian/hermes_custodian_plugin/__init__.py`
- **Plugin directory (INACTIVE):** `<hermes-home>/plugins/custodian/hermes_custodian_plugin/__init__.py` (belongs to default profile, cross-profile write blocked)
- **Profile plugin dir (INACTIVE):** `<hermes-home>/profiles/indigo/plugins/custodian/` (old version, without `ctx`)

The editable finder is at:
```
/usr/local/lib/hermes-agent/venv/lib/python3.11/site-packages/__editable___hermes_custodian_plugin_2_0_0_finder.py
```

Its `MAPPING` dict shows the actual path. **Always edit the mapped path, not the plugin directory.**

## Fix Applied (2026-06-09)

- **File:** `<hermes-home>/profiles/indigo/home/.hermes/plugins/custodian/hermes_custodian_plugin/__init__.py`
- Changed all 4 hook signatures: `ctx` → `ctx=None`
- Also changed `args: dict = None` → `args: dict | None = None` (Pyright type fix)

## Verification

After gateway restart, check that the errors cease:
```bash
grep -c "post_tool_call.*missing.*ctx" <hermes-home>/profiles/indigo/logs/errors.log
```
Expected: 0 new occurrences after restart.

## Pattern: Plugin Hook Signature Resilience (Updated)

1. **All hook callbacks must accept `**kwargs`** (already documented)
2. **All parameters before `**kwargs` must have default values** — the framework may call hooks with any subset of positional args
3. **Check the editable install path** — the plugin directory path is not always the active code

## Original Detection

- **Tier:** 2 — Non-fatal, did not block execution
- **First observed:** 2026-06-08 in light scan
- **Impact:** 2,367 occurrences in a single day — noisy but non-fatal
- **Fix verified:** Pending gateway restart