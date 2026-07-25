# Transient Provider Errors on Cron Jobs

## Pattern: `RuntimeError: Provider returned error`

When a cron job fails with `RuntimeError: Provider returned error`, this is a transient OpenRouter API error (usually HTTP 429 rate limiting or provider-side failure).

### Diagnostic Steps

1. **Verify OpenRouter reachability:**
   ```bash
   curl -s -o /dev/null -w "%{http_code}" https://openrouter.ai/api/v1/models
   ```
   HTTP 200 = provider is up, error is transient.

2. **Check for correlated failures:**
   ```bash
   grep "Provider returned error" <hermes-home>/logs/errors.log | tail -10
   ```
   If multiple jobs failed in the same time window, it's a provider-wide blip, not a job config issue.

3. **Check fix_effectiveness.jsonl** for the `oc_http_429_rate_limit` fingerprint to see if this pattern was previously tracked and resolved.

### Decision Rules

| Condition | Action |
|---|---|
| OpenRouter reachable (HTTP 200) + isolated failure | Mark resolved: "transient provider error". Do NOT escalate, do NOT modify job config. |
| OpenRouter unreachable (timeout/5xx) | Wait for provider recovery. Mark as "provider outage, monitoring". |
| Same job fails 3+ consecutive runs | Investigate job-specific issues (model config, prompt length, token limits). |
| Multiple jobs failing across multiple hours | Check for sustained rate limiting or account issues. |

### What NOT To Do

- Do NOT re-register or reconfigure the job
- Do NOT escalate as a new issue each scan — check if it's the same fingerprint
- Do NOT modify the job's model/provider settings
- Do NOT generate `oc_cron_job_inactivity_timeout` proposals for this error pattern

## Pattern: `upstream_error` 401 — Transient Upstream Auth Failure

When a cron job fails with:
```
RuntimeError: Error code: 401 - {'error': {'message': 'Authentication failed with upstream provider', 'type': 'upstream_error', 'status': 401}}
```

This is **NOT** an invalid API key. It is a transient OpenRouter upstream provider error where the provider's own auth handshake with its upstream model provider fails momentarily.

### How to confirm

1. **Verify API key is valid** (direct test):
   ```bash
   curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $KEY" https://openrouter.ai/api/v1/models
   ```
   HTTP 200 = key is valid, error is transient upstream.

2. **Check `consecutive_failures`**:
   - `None` or `0` = first occurrence, not yet retried → transient
   - `2+` = may be a persistent issue, investigate further

3. **Check if other jobs using the same provider are working**:
   - If most jobs are `status=ok` but a few weave/specific jobs are 401 → transient
   - If ALL jobs are failing with 401 → possible key rotation or account issue

### Decision Rules

| Condition | Action |
|---|---|
| API key valid (HTTP 200) + `consecutive_failures=None` | Mark as transient. No action needed. Will self-recover on next run. |
| API key valid + `consecutive_failures=2+` | Surface in report. Check if specific job model/config differs from working jobs. |
| API key invalid (HTTP 401 on direct test) | Escalate — key needs rotation. Check `config.yaml` for stale key. |

### Real example (2026-06-04)

- 3 weave jobs failed: `weave:enrichability-recalc`, `weave:overnight-enrichment`, `weave:sync-contacts`
- All had `consecutive_failures=None` (first occurrence)
- Direct API test: HTTP 200, 345 models available → key valid
- Other jobs using same provider (openrouter/owl-alpha) were `status=ok`
- Root cause: transient upstream auth blip
- Resolution: no action needed, jobs will self-recover on next scheduled run

### What NOT To Do

- Do NOT attempt OAuth re-auth (this is not a Google OAuth issue)
- Do NOT rotate the API key
- Do NOT disable the affected jobs
- Do NOT escalate as a new issue each scan — check if it's the same fingerprint

### Historical Context

**See also:** `references/known_issues.json` → `oc_http_401_upstream_error_transient` for the fingerprint entry and classification rules.

- `oc_http_429_rate_limit` fingerprint tracked 34 occurrences on 2026-05-16, all resolved without config changes
- `custodian:deep` and `praxis:review` both hit this on 2026-05-19; next runs succeeded
- This is a known pattern: OpenRouter free-tier models have intermittent rate limits during peak hours
- 2026-05-19 20:06 UTC: `custodian:deep` hit transient provider error, retried, next run at 20:23 UTC succeeded. `praxis:review` also failed at 12:38 UTC with same pattern. Both self-resolved — confirmed transient OpenRouter blip.

---

## Pattern: `upstream_error` 401 — Custom Provider Auth Failure (NOT Transient)

**CRITICAL DISTINCTION:** Not all 401 "Authentication failed with upstream provider" errors are transient OpenRouter blips. Some cron jobs use a **custom provider** (e.g., `https://app.manifest.build/v1/`) that has its own separate API key and auth lifecycle.

### How to distinguish transient OpenRouter 401 vs. custom provider 401

Check the **gateway logs** for the `base_url` and `provider` fields in the error:

```bash
journalctl --user -u hermes-gateway --since "YYYY-MM-DDTHH:MM:SS" --no-pager 2>/dev/null | grep "401\|manifest.build"
```

| Log signature | Meaning |
|---|---|
| `provider=openrouter base_url=https://openrouter.ai/api/v1` | Transient OpenRouter upstream error — see transient pattern above |
| `provider=custom base_url=https://app.manifest.build/v1/` | Custom provider auth failure — **NOT transient**, requires credential update |
| `provider=nous base_url=https://inference-api.nousresearch.com/v1` | Nous provider auth failure — check `hermes auth` |

### Affected jobs (manifest.build provider)

As of 2026-06-04, these jobs use the manifest.build custom provider and fail with 401:
- `sands:update` (calendar management)
- `weave:enrichability-recalc` (social graph enrichment)
- `weave:overnight-enrichment` (social graph enrichment)
- `ocas-finch:weekly` (self-improvement orchestrator)

### Diagnostic steps for custom provider 401s

1. **Confirm the provider** — check logs for `base_url=` field as shown above
2. **Test the API key directly** (if the custom provider URL is known):
   ```bash
   curl -s -o /dev/null -w "%{http_code}" https://app.manifest.build/v1/models
   ```
3. **Check if the API key is configured** — look for the key in the job's config or `.env`:
   ```bash
   grep -r "manifest" <hermes-home>/profiles/indigo/.env 2>/dev/null
   ```
4. **Check if other jobs using the same custom provider are also failing** — if all manifest.build jobs fail, it's a provider-wide key issue, not job-specific

### Decision rules

| Condition | Action |
|---|---|
| Custom provider 401 + all manifest.build jobs failing | Escalate — API key expired/revoked. User must update key or reconfigure jobs to use a different provider. |
| Custom provider 401 + only some manifest.build jobs failing | Investigate job-specific config — may be a per-job model or prompt issue |
| Custom provider 401 + direct API test returns 401 | Key is invalid — user action required |
| Custom provider 401 + direct API test returns 200 | May be transient — monitor for recurrence |

### What NOT To Do for custom provider 401s

- Do NOT assume it's transient and wait — custom provider 401s do NOT self-recover
- Do NOT mark as "transient provider error" without checking the `base_url` in logs
- Do NOT attempt OAuth re-auth (this is not a Google OAuth issue)
- Do NOT rotate the OpenRouter key (wrong provider)

### Real example (2026-06-04 investigation)

- 4 jobs failed with 401: `sands:update`, `weave:enrichability-recalc`, `weave:overnight-enrichment`, `ocas-finch:weekly`
- All traced to `provider=custom base_url=https://app.manifest.build/v1/`
- OpenRouter jobs (90+ others) were all healthy — confirmed this was NOT an OpenRouter issue
- The manifest.build API key is expired or revoked
- Resolution: User must update the manifest.build API key or reconfigure affected jobs to use OpenRouter/Nous
