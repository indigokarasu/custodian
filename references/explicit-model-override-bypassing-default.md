# Explicit Model Overrides Bypassing Config.yaml Default

## Summary

Jobs with explicit `model:` and `provider:` fields in `jobs.json` **bypass the config.yaml default model**. When the default model is fixed (e.g., owl-alpha → qwen), jobs with hardcoded deprecated models continue failing independently.

## Error Signature

```json
{
  "model": "deepseek/deepseek-v4-flash",
  "provider": "openrouter",
  "last_error": "HTTP 404: No endpoints found for openrouter/owl-alpha"
}
```

Note: The error message references `owl-alpha` (the old default) but the job actually requests `deepseek/deepseek-v4-flash` — the error is from the provider's model resolution.

## Classification

- **Fingerprint:** `oc_http_404_model_deprecated` (subtype: `explicit_override`)
- **Tier:** 2 (per-job config fix, not systemic)
- **Transient:** NO — model is deprecated/removed from provider
- **Scope:** Only jobs with explicit `model:` + `provider:` in jobs.json

## Detection

**During deep scan Step 3 (Fingerprint + Classify):**

```python
# For each error job with HTTP 404 model error:
if job.get('model') and job.get('provider'):
    # This job has explicit override — does NOT inherit config.yaml default
    # Must fix per-job, not via config.yaml
    classify_as_explicit_override(job)
else:
    # Null model/provider — inherits config.yaml default
    classify_as_systemic_default(job)
```

## Affected Jobs (2026-07-06)

7 jobs with explicit `model: deepseek/deepseek-v4-flash, provider: openrouter`:
1. `rally:research`
2. `vesper:morning`
3. `Executive Job Search — Mon/Wed/Fri`
4. `Job Search Feedback Monitor`
5. `genie:update`
6. `soul:sync`
7. `EHCS Monthly Refill Form`

## Root Cause

Model `deepseek/deepseek-v4-flash` was deprecated/removed from OpenRouter. These jobs were created with explicit model pins and never updated when the default changed.

## Remediation

**Option 1: Update per-job model** (Tier 1 fix per job)
```bash
hermes cron edit --name "rally:research" --model "openrouter/qwen/qwen3-vl-8b-instruct:free"
```

**Option 2: Remove explicit model** (let it inherit default)
```bash
hermes cron edit --name "rally:research" --model "" --provider ""
```
Note: `hermes cron edit` may not support clearing fields; may need direct `jobs.json` edit.

## Clean Verdict Gate Update

**The deep scan clean verdict shortcut must check for explicit model overrides.**

Current logic (Step 3): "if ALL error jobs classify as transient → clean verdict"

**Required addition:**
```python
# After classifying all errors:
explicit_override_errors = [j for j in error_jobs 
    if j.get('model') and j.get('provider') 
    and '404' in j.get('last_error', '')]

if explicit_override_errors:
    # NOT clean — these are Tier 2 per-job fixes needed
    scan_verdict = "action_required"
    for job in explicit_override_errors:
        schedule_tier2_fix(job, "update explicit model to working free-tier model")
else:
    # Proceed with transient-only clean verdict
    ...
```

## Related Patterns

- `stale-model-error-diagnostic-pattern.md` — systemic default model deprecation (config.yaml level)
- `openrouter-402-credits-exhausted-pattern.md` — account-level failure affecting all jobs
- `deep-scan-clean-verdict-2026-06-23.md` — clean verdict shortcut procedure

## Confidence Model

- `confidence_score`: 0.85 (clear fix: update model string)
- `recommended_tier`: 2 (per-job, not auto-fixable globally)
- Fix-loop risk: LOW — one-time model string update

## Key Lesson

**Config.yaml default model is only for jobs with `model: null` / `provider: null`.** Explicit pins create "model islands" that rot independently. Audit `jobs.json` for explicit model/provider pairs during every deep scan.