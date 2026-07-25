# Vision Model Invalid ChatCompletion Pattern

## Summary
Vision model `nvidia/nemotron-nano-12b-v2-vl:free` (provider: openrouter) returns `ChatCompletion` objects with null `choices`, causing `AttributeError: missing choices[0].message` in `vision_analyze` and `browser_vision` tools.

## Distinction from `oc_vision_model_incompatible`
The existing `oc_vision_model_incompatible` fingerprint covers the case where `auxiliary.vision.provider` is set to `auto` and needs to be explicitly set. This is a **different** sub-pattern: the provider is already correctly set to `openrouter`, but the specific free model returns malformed responses.

## First Seen
2026-06-22 (first occurrences traced to 2026-06-20). 12+ occurrences over 2 days.

## Affected Tools
- `vision_analyze` — fails after ~120s timeout
- `browser_vision` — fails after ~120s timeout

## Affected Sessions
User-initiated sessions (telegram DM) that trigger vision analysis. Not cron jobs.

## Config State
```yaml
auxiliary:
  vision:
    provider: openrouter  # already explicit, not "auto"
    model: nvidia/nemotron-nano-12b-v2-vl:free
```

## Recommended Fix
Change `auxiliary.vision.model` to a working vision model. The `nvidia/nemeron-nano-12b-v2-vl:free` free tier appears to have compatibility issues with the OpenRouter API adapter.

## Tier Classification
Tier 2 — surface only. Cannot auto-fix (requires model selection decision).