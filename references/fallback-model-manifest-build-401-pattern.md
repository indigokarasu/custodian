# fallback_model manifest.build 401/429 Pattern

## Summary
`config.yaml`'s `fallback_model` top-level key pointed to Manifest.build custom provider (`base_url: https://app.manifest.build/v1`) with API key `mnfst_8977...`. The provider returned HTTP 401 (auth failed) and HTTP 429 (rate limited), affecting ALL null-provider cron jobs that fell through to the fallback chain.

## Affected Jobs (2026-06-20 to 2026-06-22)
- `bower:weekly-deep` — HTTP 401
- `taste:scan` — HTTP 401
- `bones:update` — HTTP 401
- `rainbow-grocery-receipts` — HTTP 401
- `dream-journal:morning` — HTTP 401
- `Gateway health monitor` — HTTP 429 (via manifest.build after openrouter rate limit)
- `praxis:journal_ingest` — HTTP 429
- `monitor:wikipedia-talk` — HTTP 429

## Root Cause
The `fallback_model` provider's API key (`mnfst_8977mcT5YGITdW1x_UrEGyI17xl-Ze401-5TU2WfUyQ`) was either expired, revoked, or the Manifest.build service was rate-limiting the account. When any job with `provider: null` + `model: null` hit a provider error on the default upstream (openrouter), the fallback routing could intercept the auxiliary/fallback LLM call and route it to the broken manifest.build provider.

## Fix Applied (2026-06-22)
Set `fallback_model: null` in `<hermes-home>/config.yaml`. This removes the broken provider from the fallback chain entirely. Jobs will now only use the default provider (openrouter) and its configured fallback_providers list (which is empty).

## Verification
After fix: check that affected jobs run successfully on their next scheduled run. The 401 errors should stop immediately. The 429 errors from manifest.build should also stop (openrouter 429s are transient and self-resolving).

## Prevention
- Monitor `fallback_model` API key validity when the key is set
- Consider removing `fallback_model` entirely if not actively used
- The `fallback_providers: []` list should also be monitored — empty list is fine, but if entries are added with invalid keys, the same cascade will occur
