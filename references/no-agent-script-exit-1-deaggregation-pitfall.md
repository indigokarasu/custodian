# No-Agent "Script exited with code 1" — De-Aggregation Pitfall

## The trap
When several `no_agent: true` cron jobs fail, their `last_error` is often the bare
string `Script exited with code 1` — the scheduler reports the **wrapper's** exit
code, not the script's stderr. If a scan classifies by substring-matching
`last_error`, ALL these jobs collapse into ONE bucket, and the scanner may (a) report
the count as a single root cause, (b) **miss jobs whose real failure differs**, or
(c) wrongly assume a prior scan's classification covers them.

**Confirmed 2026-07-07:** a 13:11Z light scan bucketed three distinct jobs as one or
omitted one:
- `monitor:list` — real failure: Google Tasks API 403 (subprocess cascade inside `tasks_monitor.py`).
- `monitor:journals` — **FALSE POSITIVE**: `monitor_journals.py` exits 1 *by design* when
  no new journals exist since last check (lines 39/47, no stderr). Classify
  `oc_cron_no_agent_exit_1_noop` (Tier 2 surface-only).
- `SearXNG Health Watchdog` — infra: container was down; traceback in `restart()` /
  `search_works()`. Transient (container later came back Up).

The scan reported `no_agent_monitor_list_403_new: 1` and
`no_agent_searxng_watchdog_recovered: 1` and **omitted `monitor:journals` entirely**.
The next scan had to rediscover it.

## Rule
When ≥2 error jobs share the bare `Script exited with code 1` wrapper (or any
identical low-information wrapper message):
1. Do **NOT** sum them into one root cause.
2. Enumerate each job individually. For every such job, read its `script` field and
   determine the REAL failure:
   - Read the full `last_error` (including any `stderr:` traceback — see the
     "Classification bias" gotcha).
   - If `last_error` has no stderr, **RUN the script directly**
     (`python3 <hermes-root>/profiles/<profile>/scripts/<script>`) and capture its
     exit + output. Inspect the script source for `sys.exit(1)` paths: a
     no-op-by-design exit (no stderr) ≠ a real failure (traceback).
   - For subprocess-wrapping monitors, run the wrapped subprocess directly to surface
     the masked error (see `subprocess-cascade-reverification-pitfall.md` /
     `subprocess-cascade-oauth-masking.md`).
3. Classify each job on its **OWN merit**. A shared wrapper message says nothing about
   shared root cause.

## Why it bites
The wrapper message is identical across auth failures, no-op-by-design exits, missing
deps, and infra outages. Collapsing them hides real failures and inflates "recovered"
claims when one job in the group happens to pass.

## Related
- `subprocess-cascade-reverification-pitfall.md`, `subprocess-cascade-oauth-masking.md` — masked subprocess errors
- `light-scan-stale-no-agent-error-triage.md` — stale vs active on no_agent jobs
- "Classification bias: don't assume root cause from a tracked issue on a different job" gotcha
- `scripts/classify_error_jobs.py` — deterministic probe that lists every "Script exited with code 1" job with its `script` name so each can be inspected

## Enrollment-coverage blind spot (skill-code-bug jobs slip past both probes)

The escalation-loop procedure says: run `verify_escalation_state.py` (internal consistency of jobs already in `jobs_paused`), THEN `find_missed_user_gated_jobs.py` (jobs that failed in the inter-scan window and were never enrolled). **Both probes miss a job whose failure is a SKILL-CODE bug rather than a billing/auth error.**

- `find_missed_user_gated_jobs.py` only matches billing/auth fingerprints: Nous 401 (`portal.nousresearch.com`), OpenRouter 402 (credits), owl-alpha 404, Google 403/401. A job failing with **`FileNotFoundError` from `Path.home()` double-path resolution**, **`sqlite3.OperationalError: no such table: facts_fts`**, or any traceback inside a skill script is NOT matched — it sails through as "no discrepancy."
- `verify_escalation_state.py` only validates jobs ALREADY listed in `jobs_paused`; it cannot discover a job that should be there but isn't.

**Result:** a job can be enabled+burning with an open issue already tracking its exact bug, yet never enrolled. Confirmed 2026-07-13: `weave:sync-google` (32b72994b28d, `Path.home()` `FileNotFoundError`) and `Chronicle Embedding Enrichment` (39d06c70d0a6, `facts_fts` schema error) were both enabled+erroring while open issues `oc_weave_path_home_resolution_bug` and `oc_chronicle_facts_fts_missing` existed with `jobs_paused` lists that omitted them. The light scan caught and enrolled them only because it manually cross-checked the stderr traceback against the issue fingerprints.

### Required cross-check (after de-aggregation, before closing the scan)
For every enabled+error job whose `last_error` is a SKILL-CODE bug (stderr traceback naming a skill script, `Path.home()`/`expanduser` resolution, missing table/column, missing module inside a skill):
1. Grep `issues.jsonl` for the matching fingerprint (the bug's known fingerprint, e.g. `oc_weave_path_home_resolution_bug`, `oc_chronicle_facts_fts_missing`).
2. If an **open** issue with that fingerprint exists AND the job's id is **absent** from its `jobs_paused` → missed enrollment.
3. **Auto-pause the job** as mitigation: set `enabled: false`, `state: paused`, `paused_at`, and a `paused_reason` describing the deterministic skill-code bug + a re-enable-on-recovery check. Append its id to the issue's `jobs_paused` and (if missing) to `affected_components`.
4. Note in the journal that the job was enrolled retroactively.

This is a legitimate light-scan reconciliation action (like resolving forward-stale escalations): a burning job absent from its own issue's `jobs_paused` is itself an operational defect. A provider/auth/credits job that fails the same way is handled by `find_missed_user_gated_jobs.py` instead — do NOT double-pause.

