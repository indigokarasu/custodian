# Cron Provider Error Transient Pattern

## When to use

During light or deep scan error classification — when a cron job with `provider: null` + `model: null` (or even explicit provider) shows `last_error: "RuntimeError: Provider returned error"` with `consecutive_failures=None` (first occurrence).

## Distinction from other patterns

| Pattern | Key field | Meaning |
|---------|-----------|---------|
| `oc_cron_provider_error_transient` | `last_error: "RuntimeError: Provider returned error"` | LLM execution context hit a provider API error before/while running the job |
| `oc_fallback_model_manifest_build_401` | `last_error: "HTTP 401"` + `provider=custom` + `base_url=manifest.build` | Specific broken fallback provider |
| `oc_null_provider_fallback_routing` | `last_error: "HTTP 403"` + unexpected provider name | Job routed to broken fallback provider |
| `oc_gateway_restart_import_window` | `ModuleNotFoundError` or `certifi SSL` | Transient import failure after restart |

## Classification

**Tier: Transient (no fix needed)**

The error `"RuntimeError: Provider returned error"` with `consecutive_failures=None` is a first-occurrence provider API error. The LLM execution context hit a transient upstream issue (rate limit, timeout, 502) that prevented it from completing the job prompt.

## Diagnostic steps

1. Verify `consecutive_failures=None` (literal null) — first occurrence
2. Verify the script itself runs correctly: `bash <script_path> --dry-run` or manual execution
3. Check `no_agent` field:
   - If `no_agent: false` — the LLM attempted to execute the script and hit a provider error
   - If `no_agent: true` — the error is from the agent wrapper, not the script
4. Check `provider`/`model` fields:
   - If both `null` — job uses default provider; error is from default provider
   - If explicit — error is from that specific provider
5. Check `next_run_at` — if scheduled to run again soon, just wait

## Resolution

**No action needed.** The job will self-resolve on its next scheduled run. The provider error is transient — upstream rate limits, 502s, and timeouts self-resolve within minutes.

**Escalation threshold:** Only escalate if `consecutive_failures >= 3` OR the error persists across 2+ scheduled runs.

## Example: Backup Hermes Sessions to GitHub (2026-06-27)

- `no_agent: false`, `provider: null`, `model: null`
- `last_error: "RuntimeError: Provider returned error"`
- `consecutive_failures: None`
- `last_run_at: 2026-06-27T12:09:23`
- `next_run_at: 2026-06-27T18:00:00`
- Script dry-run: **pass** (all sources backed up correctly)
- Classification: **Transient provider error** — LLM execution context hit upstream issue
- Fix: **None** — re-runs at 18:00

## Pitfall: Don't confuse with stale error

This is NOT a stale error — the job actually ran and errored today. But because `consecutive_failures=None` and the script itself works, it's transient, not a persistent failure requiring fix.

## Pitfall: Disabled jobs retaining stale errors (2026-06-27)

When a cron job is disabled by another job (e.g., `finch:work` setting `enabled: false`), its `last_error` from the pre-disable run persists in `jobs.json`. The job shows `status=error` + `enabled=false` + `consecutive_failures=None`. This is **not** an active error — the job is intentionally disabled and the error predates the disable action.

**Detection during scan:**
1. Check `enabled` field — if `false`, the job is intentionally disabled
2. Cross-reference the disable timestamp: check the enabling job's `last_run_at` (e.g., `finch:work` ran at 13:32, error on disabled job was from 13:40 → stale)
3. Verify the error predates the disable action: `last_error` timestamp < enabling job's `last_run_at`

**Classification:** `oc_cron_stale_error_disabled_job` — Tier 2, surface only. No fix needed. The stale error clears on next enable + successful run.

**Example (2026-06-27):**
- `brief:email-morning`: `enabled=false`, `last_error="Script not found"` from 2026-06-26
- `brief:email-evening`: `enabled=false`, `last_error="Blocked: script path..."` from 2026-06-27 05:27
- `finch:work` ran at 13:32, disabling both jobs
- Both errors predate the disable action → stale, not active