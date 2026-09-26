# Custodian Core Fingerprints (Operational Detection Set)

The fingerprints Custodian actively matches during a scan, with their Tier
assignments and transient-vs-auto-fixable classification. This is the
**operational** set — distinct from the Tier-2 surface-only catalog in
`non-fatal-error-patterns.md` (which covers patterns that are detected but
never auto-fixed).

**When to read:** during light-scan **Step 3 (fingerprint matching)** and
**Step 6 (recurrence check)**, and whenever a new error job must be classified.
For the `monitor:list` access-token case, also load
`references/monitor-list-access-token-recurrence-durable-fix-2026-07-15.md`.

| Fingerprint | Pattern | Tier |
|-------------|---------|------|
| `oc_gateway_restart_import_window` | ModuleNotFoundError / certifi SSL after restart | Transient |
| `oc_cron_no_agent_script_args` | no_agent script field has embedded arguments → wrapper fix | Tier 1 |
| `oc_no_agent_script_path_mismatch` | Script at system path, not profile path → symlink fix | Tier 1 |
| `oc_cron_script_not_found_transient` | Write/read race, script exists and runs | Transient |
| `oc_cron_stale_error_script_mismatch` | last_error ≠ current script field | Tier 2 |
| `oc_cron_provider_error_transient` | Generic "Provider returned error", cf=None | Transient |
| `oc_http_503_upstream_capacity` | OpenRouter HTTP 503 — upstream capacity limits. "The requested model is temporarily unavailable due to upstream capacity limits." Distinct from 502 (provider_unavailable, not capacity). Self-resolves when capacity recovers. Often affects multiple jobs simultaneously (provider-side throttling). Confirmed 2026-07-27: 5 concurrent 503 errors across vesper:morning, vesper:evening, ocas-finch:daily, haiku:content-post, 10khr-grind. | Transient |
| `oc_cron_llm_unnecessary` | LLM job whose prompt is just a script-wrapper, self-update, or needless skill-load — no LLM reasoning needed | Tier 2 |
| `oc_fallback_model_manifest_build_401` | fallback_model has expired custom provider key | Tier 3 |
| `oc_skill_reference_path_mismatch` | Skill reads refs from wrong path | Tier 2 |
| `oc_script_timeout_chronicle_embed` | `chronicle:daily-embed` exceeds the 600s cron hard limit (SOFT_TIMEOUT_SECS=540 insufficient); daily embedding volume too large for the free nvidia endpoint | Tier 2 |
| `oc_proxy_upstream_unknown_logical_model` | `400 {'message': 'Unknown model: <logical_model>'}` from a LOCAL router (hello_operator at 127.0.0.1:8800). **Not the router's own guard** — that emits `404 model_not_found` with different wording; verify the distinction before blaming config. The router rewrites `out['model']=spec.id` in `_forward_body`, so the bare logical name reaching an upstream means a forward path (buffered/stream/escalation/affinity re-forward) skipped the rewrite. Jobs lose ONE turn to a non-retryable 400 then succeed later, so `enabled+erroring` stays 0 while the defect recurs — do NOT let a clean job count suppress it. Confirm config is fine via `/models` + a direct non-stream AND stream probe before escalating. Confirmed 2026-09-25: 70 events / 18 sessions / 10 cron jobs, 0 job pins involved. | Tier 4 (code defect; needs live-traffic service change) |
| `oc_cron_occurrence_unclaimed_sustained` | `occurrence ... was taken off the schedule but never claimed (scheduler stopped before dispatch)` from `cron.jobs`. **Always grep for the `restoring it as the due instant` clause before escalating** — `_restore_unclaimed_slot` (`cron/jobs.py`, #107485) makes this LOSSLESS, so it is churn/latency, not lost work. Escalate on VOLUME + DURATION, not occurrence: dozens-to-hundreds across many jobs means dispatch-window starvation. Prime suspect is a scheduler lifecycle problem — check for TWO concurrent `cron.scheduler` processes under one parent (successor racing predecessor) and correlate with restarts of services those workers call. The count rising DURING a scan proves it is still active. Never Tier 1: reaping a scheduler is service-affecting and needs operator approval. Confirmed 2026-09-25: 199 events / 50 jobs over 2 days, 199/199 restored, 0 jobs failed. | Tier 3 (operator decision) |
| `oc_google_tasks_access_token_race` | `monitor:list` masked `Script exited with code 1` → real `KeyError: 'access_token'` from `tasks_monitor.py`. **TWO DISTINCT CASES (discriminate on the creds file!):** (a) `access_token` PRESENT (non-empty) + re-runs succeed → transient credential-refresh RACE, NOT a defect; `user_gated` issue is a false escalation, resolve, do not persist. (b) `access_token` ABSENT (only `token` + valid `refresh_token` + future `expiry`) → PERSISTENT CODE DEFECT in `tasks_monitor.get_access_token()`, NOT a race; the upstream credential store periodically strips `access_token` and the old code trusts the future `expiry` and crashes. Recoverable NON-interactively via `refresh_token()` (valid `refresh_token`, no <operator> re-auth) — apply the DURABLE code fix (fall back to `creds['token']` + refresh on absent token; see `references/monitor-list-access-token-recurrence-durable-fix-2026-07-15.md`) and VERIFY. Recurs after a one-off refresh if the durable fix is NOT applied — a prior "resolved" that re-fails live is a Step 8d FALSE RESOLUTION, not a new issue. | Transient (case a) / Code-defect fixable by this loop, non-interactive (case b) |

## Related pattern references

- `references/non-fatal-error-patterns.md` — Tier-2 surface-only catalog (detected, never auto-fixed).
- `references/kanban-dispatcher-stuck-diagnostic.md` — "kanban dispatcher stuck" root-cause hierarchy.
- `references/browser-cdp-502-loop-pattern.md` — CDP 502 loop classification.
- `references/provider-401-diagnosis.md` — HTTP 401 diagnosis.
- `references/oc-hook-post-tool-call-task-id-pattern.md` — post-tool-call task-id hook pattern.
- `references/transient-401-self-resolution-pattern.md` — first-occurrence 401 that self-resolves on re-run.