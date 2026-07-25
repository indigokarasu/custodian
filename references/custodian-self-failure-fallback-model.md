# Custodian Self-Failure via fallback_model (2026-07-06)

## Pattern

When `custodian:light` (the primary system health detection mechanism) has `provider: null` and falls through to a `fallback_model` with exhausted OpenRouter credits, **custodian itself fails**. This creates a blind spot: the monitoring system cannot detect issues because it's broken by the same root cause it's supposed to detect.

## Error Signature

```
custodian:light last_error: RuntimeError: Error code: 402 - {'error': {'message': 'This request requires more credits, or fewer max_tokens. You requested up to 65536 tokens, but can only afford 5066...'}}
```

## Root Cause

<<<<<<< Updated upstream
Profile config (`<hermes-home>/profiles/indigo/config.yaml`) contained:
=======
Profile config (`~/.hermes/profiles/indigo/config.yaml`) contained:
>>>>>>> Stashed changes
```yaml
fallback_model:
  api_key: 'sk-or-v1-...'  # expired/exhausted
  base_url: https://openrouter.ai/api/v1
  model: 'nvidia/nemotron-3-ultra-550b-a55b:free'
  provider: 'custom'
```

When `custodian:light` runs with `provider: null`, it uses the default model chain → hits `fallback_model` → 402 credits exhausted.

## Impact

- **Journal gap:** 4 days (July 2-5) with no custodian journals
- **Blind escalation:** Issues accumulated undetected (71 jobs failing with 402)
- **Recovery contract violation:** `oc_journal_gap` triggered

## Detection During Scans

**Light scan Step 2:** If `custodian:light` appears in error jobs with 402, flag `oc_custodian_self_failure` (Tier 1 — immediate).

**Deep scan gate:** Before clean verdict, verify `custodian:light` and `custodian:deep` both ran successfully in last 24h. If custodian itself is failing, scan is NOT clean regardless of other job status.

## Prevention

1. **Audit `fallback_model` in profile config** during deep scan Step 2 (disk/config audit)
2. **Remove `fallback_model` entirely** if it uses a custom provider — let null-provider jobs use the default model chain
3. **Configure explicit free-tier model** as default in `model:` section instead of relying on fallback
4. **Add health check** for custodian jobs in `custodian:cron-health` (already exists, but verify it catches this)

## Remediation

```bash
# Remove broken fallback_model from profile config
<<<<<<< Updated upstream
# Edit <hermes-home>/profiles/indigo/config.yaml
=======
# Edit ~/.hermes/profiles/indigo/config.yaml
>>>>>>> Stashed changes
# Remove the fallback_model: section entirely
# Gateway restart required for config change to take effect
```

## Related Patterns

- `openrouter-402-credits-exhausted-pattern.md` — root cause
- `null-provider-fallback-routing-2026-06-18.md` — general fallback routing
- `fallback-model-manifest-build-401-pattern.md` — similar pattern with Manifest.build

## Key Lesson

**The monitor must not share failure modes with the monitored.** Custodian should use a dedicated, guaranteed-available model/provider (e.g., local Ollama, or a separate API account with independent billing). Shared `fallback_model` creates a single point of failure for the entire detection system.