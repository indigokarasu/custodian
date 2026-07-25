# Stale Model Error Diagnostic Pattern

When many jobs (`provider: null`, `model: null`) simultaneously show `HTTP 404: No endpoints found for <provider>/<model>`, the default model was likely removed by the provider. This document covers the diagnostic flow to distinguish stale errors (config already fixed, jobs will self-resolve) from active errors (config still broken).

## Confirmed Instances

- **2026-07-01**: 32 jobs showed `HTTP 404: No endpoints found for openrouter/owl-alpha`. OpenRouter removed the model. Config had already been updated to `deepseek/deepseek-v4-flash` at 22:57 PDT. All 32 errors were stale — confirmed by OpenRouter model list showing 338 models without owl-alpha, and config.yaml mtime after all job `last_run_at` timestamps.

## Diagnostic Flow

### Step 1: Check if the model exists on the provider

Query the provider's model list API to confirm the model was actually removed:

```python
import json, urllib.request
req = urllib.request.Request('https://openrouter.ai/api/v1/models', 
    headers={'User-Agent': 'Mozilla/5.0'})
resp = urllib.request.urlopen(req, timeout=10)
data = json.loads(resp.read())
models = data if isinstance(data, list) else data.get('data', [])
owl = [m for m in models if 'owl' in str(m.get('id', '')).lower()]
# If empty: model no longer exists on provider
```

Other providers: adjust the URL to the provider's /v1/models endpoint.

### Step 2: Check the profile config.yaml for the default model

<<<<<<< Updated upstream
The config `model:` section at `<hermes-home>/profiles/<profile>/config.yaml` defines the default model:
=======
The config `model:` section at `~/.hermes/profiles/<profile>/config.yaml` defines the default model:
>>>>>>> Stashed changes

```yaml
model:
  base_url: https://openrouter.ai/api/v1
  default: <model_name>
  provider: openrouter
  api_mode: chat_completions
```

<<<<<<< Updated upstream
**Important:** The `model:` section is at the PROFILE config level, not the main `<hermes-home>/config.yaml`. The main config stores different settings. Always check the profile config.
=======
**Important:** The `model:` section is at the PROFILE config level, not the main `~/.hermes/config.yaml`. The main config stores different settings. Always check the profile config.
>>>>>>> Stashed changes

### Step 3: Check config modification time vs jobs' last_run_at

```bash
<<<<<<< Updated upstream
stat <hermes-home>/profiles/<profile>/config.yaml | grep Modify
=======
stat ~/.hermes/profiles/<profile>/config.yaml | grep Modify
>>>>>>> Stashed changes
```

Then cross-reference: for each error job in jobs.json, compare `last_run_at` to the config's mtime. If ALL error jobs have `last_run_at < config_mtime`, the errors are **stale** — the config was fixed after the jobs failed. They will self-resolve on next scheduled run.

### Step 4: Classify

- **Config mtime AFTER all error job last_run_at** → **Stale** (will self-resolve). Check that the new model actually exists on the provider (Step 1).
- **Config mtime BEFORE any error job last_run_at** → **Active** (config still broken). Jobs ran with the bad model AFTER the config was supposedly fixed. Either the fix didn't take effect or a different model selection mechanism is involved.
- **No config change found** → **Active**. The model is genuinely gone and needs to be replaced.

## Pitfalls

- **Consecutive_failures may be None (null)** on stale model errors — the scheduler doesn't set cf when the agent never started (model not found is treated differently than script failures). Null cf doesn't mean "no errors."
- **The `model:` section in profile config is NOT the same as `fallback_model` or `fallback_providers`** — The `model:` section sets the default model used when jobs have `model: null`. Fallback mechanisms only kick in when the default fails. Check the `model:` section FIRST.
- **Multiple scanning runs may have missed the errors** — If a scan iterates only N errors it found in a prior scan, it won't catch newly stale errors. Always parse ALL jobs.json entries.
- **Gateway log may show no errors for stale model failures** — Model resolution errors happen during agent startup, before the gateway logs most events. Clean gateway log does NOT mean no model errors.