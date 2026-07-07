# OpenRouter HTTP 402 Credits Exhausted Pattern

## Summary

OpenRouter "free" tier models (e.g., `nvidia/nemotron-3-ultra-550b-a55b:free`, `openrouter/owl-alpha`, `deepseek/deepseek-v4-flash`) **still consume account credits**. The `:free` suffix indicates no per-token charge, but requests draw from the account's credit balance. When credits hit $0, all models return HTTP 402.

## Error Signature

```json
{
  "error": {
    "message": "This request requires more credits, or fewer max_tokens. You requested up to 65536 tokens, but can only afford ~5000.",
    "code": 402,
    "metadata": {
      "provider_name": null,
      "previous_errors": [...]
    }
  }
}
```

## Classification

- **Fingerprint:** `oc_openrouter_402_credits_exhausted`
- **Tier:** 3 (user-gated — requires adding credits or switching providers)
- **Transient:** NO — will not self-resolve without user action
- **Scope:** ALL jobs using the affected OpenRouter account, regardless of model

## Root Cause

OpenRouter account credits depleted. The "free" label is a pricing tier (zero per-token cost), not a credit exemption. Free models still require available account balance.

## Detection

## Affected Jobs (2026-07-06)

27 jobs failing simultaneously:
- `custodian:deep`, `haiku:morning-scan`, `haiku:content-review`
- `bower:scan`, `bones:market-monitor`, `dispatch:triage-morning`
- `daily-user-context`, `ocas-autobio-grade`, `taste:ingest`
- `vesper:update`, `taste:historical-email`, `taste:historical-calendar`
- `scout:update`, `taste:sync-spotify`, `mentor:update`
- `praxis:update`, `forge:update`, `sift:update`
- `sands:update`, `look:update`, `fellow:update`
- `weave:update`, `taste:update`, `rally:healthcheck-pre-open`
- `custodian:update`, `vesper:morning`, `haiku:haiku-post`
- `bones:update`, `vesper:deliver-morning`, `bones:paper-trade`
- `styx:update`, `genie:update`, `dream-journal:morning`
- `soul:sync`, `monitor:wikipedia-talk`, `daily-false-trigger-fix`

Plus 7 jobs with explicit `deepseek/deepseek-v4-flash` overrides (see `explicit-model-override-bypassing-default.md`)

## Remediation Options

1. **Add credits** at https://openrouter.ai/settings/credits — immediate fix
2. **Switch default model** in config.yaml to a verified free-tier model that doesn't hit credit limits (e.g., `openrouter/qwen/qwen3-vl-8b-instruct:free` already used for compression)
3. **Configure per-job models** to use free-tier models explicitly
4. **Add fallback_model** with working free provider (currently commented out in config.yaml)

## Detection During Scans

- **Light scan:** Check all error jobs for `last_error` containing "402" and "credits"
- **Deep scan:** Cross-reference with `known_issues.json` fingerprint `oc_openrouter_402_credits_exhausted`
- **Clean verdict gate:** If ANY job shows 402, scan is NOT clean — even if other errors are transient

## Related Patterns

- `openrouter-502-provider-unavailable.md` — 502 is transient provider issue, 402 is account-level
- `fallback-model-manifest-build-401-pattern.md` — 401 from custom provider in fallback_model
- `null-provider-fallback-routing-2026-06-18.md` — how null-provider jobs route through config.yaml model section

## Confidence Model

- `confidence_score`: 0.95 (high — deterministic account state)
- `recommended_tier`: 3 (cannot auto-fix, requires user action)
- `schedule_adjusted_stickiness`: N/A (not a fix-loop pattern)

## Key Lesson

**"Free" ≠ "No Credits Required".** The `:free` suffix only waives per-token fees. Account-level credit balance is still consumed. A $0 balance blocks ALL models on that account, including free-tier ones.