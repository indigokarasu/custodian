# OpenRouter HTTP 502 `provider_unavailable` Pattern

## Distinction from Other Provider Errors

This is a **distinct failure mode** from the well-known HTTP 429 patterns:

| Pattern | HTTP Code | Error Type | Root Cause | Auto-fix |
|---|---|---|---|---|
| `oc_http_429_rate_limit` | 429 | rate-limit | Plan-level weekly usage limit | Wait for reset |
| `oc_http_429_concurrent` | 429 | concurrent | Too many simultaneous requests | Stagger cron schedules |
| `oc_provider_error_transient` (502) | 502 | provider_unavailable | OpenRouter's upstream provider is down | **None** — transient |

## Diagnostic Steps

When you see "RuntimeError: Provider returned error" in cron logs:

1. **Check the request dump files** — the generic log message hides the real error:
   ```bash
   ls -lt <hermes-root>/sessions/request_dump_*.json | head -5
   ```
   Read the most recent dump and look at `error.body.metadata.error_type`:
   - `"provider_unavailable"` → HTTP 502, transient upstream outage
   - `"rate_limit"` → HTTP 429, plan/concurrency limit

2. **Verify API key is valid** (rules out 401/403):
   ```bash
   curl -s "https://openrouter.ai/api/v1/models" -H "Authorization: Bearer $KEY" > /tmp/models.json
   # HTTP 200 = key valid
   ```

3. **Verify model exists** (rules out 404 deprecation):
   ```python
   import json
   with open('/tmp/models.json') as f:
       models = [m['id'] for m in json.load(f)['data']]
   assert 'openrouter/owl-alpha' in models
   ```

4. **Check for HTTP 429 errors** (rules out rate limit cascade):
   ```bash
   grep "HTTP 429" <hermes-root>/logs/errors.log | grep "$(date +%Y-%m-%d)" | wc -l
   # 0 = not a rate limit issue
   ```

5. **Check credential pool health**:
   ```python
   import json
   with open('<hermes-root>/auth.json') as f:
       auth = json.load(f)
   pool = auth.get('credential_pool', {}).get('openrouter', [])
   for entry in pool:
       print(f"last_status={entry.get('last_status')} | id={entry.get('id')}")
   # last_status=ok + no 429s = not a credential issue
   ```

## Key Indicators of 502 `provider_unavailable`

- All errors are from the **same provider** (openrouter)
- All errors use the **same model** (openrouter/owl-alpha)
- **No HTTP 429 errors** in logs on the same day
- **API key is valid** (models endpoint returns 200)
- **Model exists** in the models list
- **Credential pool** shows `last_status: ok`
- Errors cluster in a **time window** (e.g., 28 minutes) then stop
- Request dump shows `error.body.metadata.error_type == "provider_unavailable"`

## Why No Auto-Fix Is Available

This is a **provider-side outage** — the actual model host that OpenRouter routes to is temporarily down. The agent cannot fix this. Jobs will retry on their next scheduled run and succeed when the upstream recovers.

## Structural Gap: No Fallback Model

The current config has:
```yaml
model:
  fallback_model:
    context_length: 256000
  provider: openrouter
fallback_providers: []
```

`fallback_model` has no `default` or `provider` set, and `fallback_providers` is empty. This means when the primary model fails with a 502, there is **no automatic failover** to a secondary model or provider. Jobs simply fail until the next retry.

**Recommendation**: If 502 outages become frequent, configure a fallback model:
```yaml
model:
  fallback_model:
    default: openrouter/anthropic/claude-sonnet-4  # or any reliable model
    provider: openrouter
    context_length: 256000
```

## Session Origin

First identified: 2026-05-20 16:07-16:35 UTC. 6 cron jobs failed in 28 minutes. Investigation via request dump files revealed `error_type: provider_unavailable`. API key confirmed valid, model confirmed existing, no 429 errors detected.

## Update History

- 2026-05-20: Initial documentation after first observed occurrence.
