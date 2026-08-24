# Escalation Loop Resolved Patterns (2026-07-28)

Session-derived fixes and resolution procedures catalogued from the escalation execution loop.

## Patterns

### `oc_praxis_literal_tilde_path` — Tier 4 auto-fixable code defect

**Fingerprint:** `oc_praxis_literal_tilde_path`  
**Affected pattern:** Python scripts in OCAS skills use literal `~/.hermes/...` or `$AGENT_ROOT/...` string paths instead of `os.path.expanduser()`. The `~` tilde is never expanded in Python `open()` calls, causing `FileNotFoundError` at runtime even though the expanded path exists.

**Detection:** During light-scan Step 6 (script path blocks), check any `FileNotFoundError` whose path starts with `~` or `$AGENT_ROOT` and the script is in a skill's `scripts/` directory. The path will contain a literal tilde or `/root` prefix that won't resolve in the cron execution environment.

**Fix:** Replace literal tilde paths with `os.path.expanduser()` calls. Ensure `import os` is present. Run the script to verify. Force-flip the cron registry with `hermes cron run <job_id>`.

**Verified fix:** `ocas-praxis/scripts/praxis_review.py` lines 17-18 — patched and verified (script runs successfully, writes decisions.jsonl to correct profile path). Confirmed 2026-07-28.

### `oc_interpreter_shutdown_transient` — Tier 2 transient, auto-resolve after gateway restart

**Fingerprint:** `oc_interpreter_shutdown_transient` ("cannot schedule new futures after interpreter shutdown")  
**Root cause:** Gateway restart drains the asyncio event loop; concurrent.futures calls hit a shutdown executor. This is NOT a code defect — it is a transient state after a gateway SIGTERM/restart.

**Fix:** Re-run the affected job(s) via `hermes cron run <id>`. If the job succeeds, the fault was transient and self-resolved. Mark `status: resolved`, `escalation_needed: false`. Do NOT file a Tier 1 fix ticket or pause the job.

### `oc_provider_503_dispatcher_gap` — Stale 503 gap, self-resolving on provider recovery

**Fingerprint:** `oc_provider_503_upstream_capacity` on dispatcher job only (not the broader jobs)  
**Root cause:** Nous provider 503 (upstream capacity) affects multiple jobs. The dispatcher gap is a side effect — the dispatcher skips its run when it encounters the 503, then re-runs cleanly when the provider recovers.

**Fix:** Verify the dispatcher job is now `status=ok` via `hermes cron run <id>`. If it succeeds, the gap is stale — mark resolved. Do NOT escalate or pause.

### `oc_bones_missing_kalshi_creds_file` — Tier 3 user-gated credential provisioning

**Fingerprint:** `oc_bones_missing_kalshi_creds_file` — `FileNotFoundError: kalshi_creds.json` in both `bones:position-tracker` and `stones:market-monitor`.  
**Root cause:** The `kalshi_creds.json` credential file has not been created in the Bones data directory after Kalshi OAuth setup.

**Fix direction:** User-gated. Requires user to create `kalshi_creds.json` with valid Kalshi API credentials in `$HERMES_HOME/../indigo/commons/data/ocas-bones/`. Both Bones jobs share the same `CREDS_PATH` variable (`{BONES_DIR}/kalshi_creds.json`), so a single credential file fixes both. The scripts should ideally handle missing credentials gracefully (exit 0 with a warning rather than crashing), but the primary fix is credential provisioning.
