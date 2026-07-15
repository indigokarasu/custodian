# no_agent Monitor exit-1 with Futile Restart = Real Upstream-Degraded Fault (2026-07-08)

## Pattern
A `no_agent: true` health/watchdog script exits 1 with
`Script exited with code 1` + stdout like:

    [ts] UNHEALTHY: no results
    [ts] Restarting <svc>...
    [ts] Restart FAILED

This is NOT `oc_cron_no_agent_exit_1_noop` (which is a genuine no-work
no-op). It is a REAL infrastructure fault. The restart is *futile* because
the root cause is upstream of the container, not the container itself.

## Confirmed case (2026-07-08) — SearXNG
- `searxng_watchdog.py` probes `http://localhost:8888/search?q=...&format=json`
  and requires `len(results) > 0`.
- Container was RUNNING (HTTP 200) — docker restart did nothing useful.
- `search_works()` still returned 0 results because upstream engines were
  SUSPENDED: `aol` (HTTP error), `brave` (too many requests),
  `duckduckgo` (CAPTCHA), `karmasearch` (access denied).
- Every 3-min run: detect unhealthy -> restart (futile) -> still 0
  results -> exit 1. `cf=None` (scheduler does not count no_agent exit-1
  as consecutive failures) so it fails SILENTLY.

## Diagnosis procedure (do this before classifying)
1. Probe the actual upstream dependency LIVE (HTTP curl / docker inspect),
   do NOT trust the script's own stdout verdict alone.
   - **Check the docker port mapping first** (`docker ps` -> read the `PORTS`
     column). The monitored service may not listen on the port you assume.
     SearXNG publishes `127.0.0.1:8888->8080/tcp`, so curling `:8080` returns
     `000` (connection refused) while `:8888` serves 200. Curling the wrong
     port manufactures a false "container down" signal.
   - **Run an actual query, not just a home-page check.** A 200 on `/` does
     NOT prove the service works — the watchdog fails on *query* results
     (`len(results) == 0`), so probe the real endpoint (e.g.
     `GET /search?q=test&format=json`) and count the returned results.
2. Check the job's recent run history BEFORE classifying:
   - `ls -t <hermes-root>/profiles/<profile>/cron/output/<job_id>/` — if the
     monitor was **silent/healthy (empty output) minutes before** the failed
     run, the current failure is a **transient blip, not a persistent crash
     loop**. A persistent upstream-degraded fault shows failures across
     consecutive runs.
3. Distinguish three cases (not two):
   - container DOWN -> restart would help -> if restart also fails, real
     infra fault (Tier 2/3).
   - container UP but upstream DEPENDENCY degraded (0 results / suspended
     engines / API 403) **across multiple runs** -> restart is futile -> real
     fault, NOT a Custodian auto-fix. Escalate; leave the monitor RUNNING so
     it auto-clears when the upstream recovers (rate-limit cooldown).
   - **COLD-START RACE (transient, NOT a fault):** monitor reports
     `UNHEALTHY: no results` + `Restart FAILED`, but the container is actually
     UP (the monitor's own restart succeeded — `docker ps` shows it `Up`) and
     a **post-warmup live query returns results**. This happens when the
     monitor triggers a restart and then health-checks immediately, before
     the service finished warming up. It self-clears on the next scheduled
     run. Leave the monitor RUNNING; do NOT escalate. Distinguish from genuine
     suspension by: (a) prior runs were healthy (run history), and (b) a
     post-warmup query returns >0 results. Genuine suspension returns 0
     results even after warmup and recurs across runs.
   - **Decision summary:** post-warmup live query returns results + recent
     runs were healthy -> cold-start race, transient, self-clears (leave
     running, no escalation). Post-warmup query returns 0 results across
     multiple runs -> genuine upstream-degraded, escalate + leave running.
4. Do NOT pause a monitor that is correctly reporting a transient upstream
   degradation — pausing hides recovery. Let it self-clear; the
   escalation runner can pause if it persists past the cooldown window.

## Classification
- fingerprint suggestion: `oc_<svc>_upstream_degraded_no_results`
- auto_fixable: False (requires user action or upstream cooldown)
- escalation_needed: true (for Mentor)

## Resolution flip-flop hazard (2026-07-08)

Do NOT resolve an open upstream-degraded issue just because the monitor is
momentarily OK. These faults RECUR within the upstream cooldown window.
Resolving when the monitor is green creates churn:

- A later light scan sees `last_status: ok` and "resolves" the issue
  (forward-staleness) in its journal.
- The fault recurs (engines re-rate-limited); the monitor goes red again.
- An esc-exec must then "re-open" the issue — extra work and a misleading
  resolved→reopen history.

**Correct steady-state for an open upstream-degraded fault:** keep
`status: open` + `escalation_needed: true`, leave the monitor RUNNING. The
running monitor IS the detection mechanism; it self-clears on upstream
recovery. Only resolve when the root cause is GONE (engine re-enabled, model
restored, etc.) — for user-gated billing/key issues that means owner added
credits / rotated the key.

**Distinguish the two forward-staleness cases:**
- USER-GATED billing/key issue whose job RECOVERED (`last_status: ok`,
  `last_error` empty, e.g. token re-authorized) -> resolve (set
  `status: resolved`, clear `escalation_needed`).
- OPEN infra fault that auto-clears by leaving the monitor running -> do NOT
  resolve; keep open+escalated.

Confirmed 2026-07-08: `oc_searxng_engines_suspended_20260708` flip-flopped —
light scans resolved at 16:08/18:08, esc-exec re-opened at 20:35. Final
correct state: open, monitor running, momentarily OK on upstream cooldown.
The 20:35 re-open was the right call; the prior resolutions were the wrong
call (forward-staleness applied to a recurring infra fault, not a recovered
job).

---

**See also:** `no-agent-monitor-exit1-noop-readonly-classification.md` — when an
UNKNOWN job is a bare `Script exited with code 1` with NO stderr and no
`UNHEALTHY` / `Restart FAILED` stdout, it is most likely a no-op, NOT this
upstream-degraded fault. Classify it READ-ONLY (do NOT run the script — it has
queue-enqueue side effects). That file gives the exact `latest_mtime`
comparison procedure to confirm the no-op without mutating state.
