# Provider Recovery — Forward-Stale Confirmation

## When to use
A light/deep scan surfaces a large cluster of `401` / `402 (credits)` / `token is expired` errors, and one or more `issues.jsonl` entries already carry `status: user_gated` + `escalation_needed: true` for that provider. Before treating it as a live outage, confirm whether the provider has actually recovered. This is the **positive** counterpart to the forward-stale gotchas: those tell you *what* to resolve; this tells you *how to prove it is safe to resolve*.

## The trap (why naive checks mislead)
- **`hermes chat -q ping`** launches an interactive agent session, not a direct API auth test. It prints a session-resume banner and exits 0 — proves nothing about token validity.
- **Generic `GET /v1/models` with the stored API key** returns 403 on non-standard `base_url` values (e.g. `https://chatgpt.com/backend-api/codex` — a Codex-style endpoint). A 403 there means "path not served here", NOT "token dead". Ambiguous → do not conclude from it.
- The jobs' own `last_error` is **stale**: it persists with the timestamp of the last run that actually failed (often the day before recovery). `consecutive_failures=None` means the scheduler never even counted it as a consecutive failure.

## Positive-confirmation procedure (what actually decides)
1. **Count today's OK runs.** Parse `jobs.json`; for jobs with `last_run_at` starting today's date (`YYYY-MM-DDT…`), count `last_status=ok`. If a large share of ALL jobs (e.g. 78/140 enabled) ran OK today, the scheduler + default provider are working.
2. **Cross-check the suspect issue's `affected_job_ids`.** Pull the issue's `affected_job_ids` (or `affected_jobs`); look each up in live `jobs.json`. If those exact jobs now show `last_status=ok` with `last_run_at` today, the failure is no longer present — the issue is forward-stale.
3. **Resolve only after both hold.** Set `status: resolved`, `escalation_needed: false`, `resolved_at: <now>`, `resolved_by: custodian.scan.light`, and a `resolution_note` stating the positive evidence (N jobs incl. listed affected jobs ran OK today via the default provider; remaining live errors predate recovery and self-clear).

## Worked example (2026-07-13 light scan)
- 40 enabled-error jobs; 23 `token_expired`, 4 `openrouter_402`, 2 `nous_401`, 2 `owl_404`, 1 nvidia, 6 `script_exit1`, 1 timeout.
- **78 jobs ran OK today** via default provider `nous` → `tencent/hy3:free`, including `vesper:update`, `mentor:update`, `sands:update` — all on the `oc_nous_api_key_invalid` affected list.
- The 3 provider-auth issues (`oc_provider_auth_token_expired_20260712T040120`, `oc_nous_api_key_invalid_20260712T040120`, `oc_openrouter_402_credits_exhausted_20260712T040120`) had affected jobs now `last_status=ok` with today timestamps. → All three resolved as forward-stale.
- The 12 errors that actually occurred TODAY were a DIFFERENT set (weave Path.home bug, chronicle facts_fts, bones kalshi SDK ValidationError, spotify/git/gh credential gaps, no_agent exit-1 no-ops, one 600s timeout) — none provider-auth.

## Reusable probe
`scripts/confirm_provider_recovery.py` — prints today's OK-run count, fingerprints the live enabled-error set, and for each provider-auth issue checks how many of its `affected_job_ids` are now live-OK. Run it after the standard `verify_escalation_state.py` + `find_missed_user_gated_jobs.py` probes when a provider-auth cluster looks suspicious.
