# Owl-alpha Model 404 Diagnostic

## Fingerprint: `oc_http_404_model_deprecated`

**Error string:** `RuntimeError: HTTP 404: No endpoints found for openrouter/owl-alpha.`

**Impact:** Systemic — affects ALL null-model, null-provider cron jobs that trigger agent sessions. Not job-specific.

## Diagnostic Chain

When 10+ null-model, null-provider cron jobs simultaneously show `"No endpoints found for openrouter/owl-alpha"`, the root cause is almost always the **`auxiliary.compression` section** in config.yaml — NOT the main `model.default` setting.

### Step 1: Check config.yaml for owl-alpha references

```bash
grep -n 'owl-alpha\|auxiliary.*compression' <hermes-home>/config.yaml
```

The main `model.default` is typically a different model (e.g., `deepseek/deepseek-v4-flash`) and works fine. The `auxiliary.compression` section overrides the compression model independently:

```yaml
auxiliary:
  compression:
    provider: openrouter
    model: openrouter/owl-alpha   # <-- DEPRECATED
```

### Step 2: Verify the model is actually gone

```bash
curl -s https://openrouter.ai/api/v1/models | jq -r '.data[] | .id' | grep owl-alpha
```

If this returns nothing, the model has been fully removed from OpenRouter.

### Step 3: Count affected jobs

```python
import json
with open("<hermes-home>/cron/jobs.json") as f:
    data = json.load(f)
jobs = data.get("jobs", [])
affected = [j for j in jobs if "openrouter/owl-alpha" in (j.get("last_error") or "")]
print(f"{len(affected)} jobs affected")
```

### Step 4: Verify main default model is NOT the source

The `model.default` in config.yaml typically references a working model (e.g., `deepseek/deepseek-v4-flash`). Verify:

```bash
grep '^model:' -A 3 <hermes-home>/config.yaml | head -6
# Should show model.default with a different model
```

If `model.default` is also `openrouter/owl-alpha`, the fix is the same but the impact is broader.

## Root Cause Mechanism

The `auxiliary.compression` model is used by the hermes-agent Python runtime to compress conversation context during agent sessions. When a cron job with `model: null, provider: null` runs an agent session, it triggers context compression at some point during execution. If the compression model returns 404, the entire session fails with the compression error — NOT a main-model error.

This is why the error appears on 30+ diverse jobs (taste:*, scout:*, vesper:*, etc.) even though none of them explicitly configure `openrouter/owl-alpha`.

## Fix

Update `auxiliary.compression.model` in config.yaml to a working free model:

```yaml
auxiliary:
  compression:
    provider: openrouter
    model: openrouter/qwen/qwen3-vl-8b-instruct:free   # Working alternative
```

Or set `api_key` explicitly for the compression model if the credential pool is the real issue.

**Tier:** 3 (configuration change, requires verification)
**Confidence:** 0.85

## Related Patterns

- `oc_http_404_model_deprecated` in `known_issues.json` — parent fingerprint
- `stale-model-error-diagnostic-pattern.md` — distinguishing stale from active model errors
- `null-provider-fallback-routing-2026-06-18.md` — when null providers route through unexpected fallbacks