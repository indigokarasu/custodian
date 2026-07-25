# Provider Auth Token Expiry Fingerprint — `oc_provider_auth_token_expired`

## Exact error text (OpenRouter / Nous agent-mode + no_agent jobs)

```
RuntimeError: Error code: 401 - {'error': {'message': 'Provided authentication token is expired. Please try signing in again.', 'type': None, 'code': 'token_expired', 'param': None}, 'status': 401}
```

The discriminators in the `last_error` string are: `token_expired` and/or `authentication token is expired`.

## MUST NOT be confused with these other 401 patterns

| Fingerprint | Discriminator string | Class | Handling |
|---|---|---|---|
| `oc_provider_auth_token_expired` | `token_expired` / `authentication token is expired` | **User-gated (Tier 3)** — session token lapsed; needs re-auth | Re-auth provider token; leave jobs running to self-recover |
| `oc_nous_api_key_invalid` | `portal.nousresearch.com` ("Your API key is invalid, blocked or out of funds") | User-gated (Tier 3) — key invalid/blocked/out of funds | Rotate Nous key / add credits |
| `transient-401-self-resolution` / `oc_cron_provider_error_transient` | first-occurrence generic `Provider returned error` or 401 from default upstream, `cf=None` | **Transient** — self-resolves next run | No action; monitor only |

**The trap:** a naive scan that buckets all `401` as "transient 401 self-resolution" will misclassify `token_expired` and report a false clean verdict while 48+ jobs keep failing. `token_expired` is persistent and **recurs** — it does not self-heal.

## Recurrence evidence (this environment)

- 2026-07-09: `oc_default_provider_token_expired_20260709` opened, resolved 2026-07-10 (token re-authed).
- 2026-07-12: same root cause returned → `oc_provider_auth_token_expired_20260712T040120` opened (48 jobs). Token expired again ~2 days later.

This is the dominant recurring failure mode in this deployment. Expect periodic re-auth until the credential lifecycle is fixed at the source (token TTL / refresh flow).

## Handling rules (per 2026-07-11 correction: do NOT pause provider-auth outages)

1. Classify as `oc_provider_auth_token_expired`, user-gated, Tier 3 — NOT transient.
2. **Do not pause** the affected recurring jobs. Leave them enabled so they retry and self-clear on re-auth.
3. Do not report as "fixed" — re-auth is a user action (<operator> signs in / refreshes the provider token).
4. Ensure a matching open issue exists in `issues.jsonl` (`fingerprint: oc_provider_auth_token_expired`). If missing, write it (status `user_gated`, `escalation_needed: true`).
5. In a light scan, if an existing `oc_provider_auth_token_expired` issue's jobs have ALL recovered (`last_status: ok`, `last_error` empty), resolve it (forward-stale reconciliation).

## RECOVERY VERIFICATION — time-based, NOT model/endpoint probe (2026-07-13 correction)

A prior escalation loop resolved this issue citing "provider recovered — `hermes chat -q` returned
PONG on the free default model `tencent/hy3:free`" and "OpenRouter `/models` returned HTTP 200."
**That is NOT valid recovery evidence** and caused a false-resolution of 19+ still-failing jobs:

- Probing the **free default model** only proves the *free* model works. It does NOT validate the
  **session token** these jobs use. The token can still be expired even though the free model answers.
- Probing OpenRouter `/models` (HTTP 200) only proves the *endpoint* is up, not that the key/credits
  the failing jobs present are valid.
- The loop inferred "recovered" from *other* jobs (using the free model) that reported `status=ok`,
  then declared the token-expired jobs would "self-clear on next run." But their `last_run_at`
  predated the claimed recovery and they never re-ran successfully — the token is NOT recovered.

**The ONLY valid recovery evidence is TIME-BASED.** For each affected job, confirm it actually
re-ran with `last_status: ok` AND `last_error` cleared **after** the claimed recovery timestamp.
Decisive check:

```bash
python3 scripts/verify_recovery_by_runtime.py --issue oc_provider_auth_token_expired_20260712T040120
# OR: python3 scripts/verify_recovery_by_runtime.py --jobs <id1>,<id2> --recovery 2026-07-13T16:10:00Z
```

Exit 0 = all re-ran OK since recovery (resolution valid). Exit 1 = any still failing → the issue
must stay OPEN or be RE-OPENED (do not trust the resolution). Never close this issue unless
`verify_recovery_by_runtime.py` exits 0.

## Companion probe

`scripts/bucket_error_jobs.py` classifies every enabled error job into known fingerprints and surfaces UNKNOWN ones, so `token_expired` is never collapsed into a generic "other" bucket. Run it first in any light/deep scan.
