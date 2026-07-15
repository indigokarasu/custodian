---
name: Escalated issue premise can be stale — re-derive the probe and re-check binaries live
date: 2026-07-13
skill: ocas-custodian
---

## Mechanism

A prior scan wrote an issue record asserting facts about current system state
(e.g. "docker binary absent", "port 8080 returns HTTP 000 (not serving)").
Between that scan and the next escalation loop, the environment recovered
(docker installed, container resumed, upstream engine un-suspended). The issue
record is now a STALE FALSE PREMISE.

If the escalation loop trusts the issue text, it concludes "user-gated /
unresolvable in cron" and re-persists it as open — a self-fulfilling staleness
loop. This is a distinct failure mode from "stale `last_error` in jobs.json":
here the issue's OWN BODY asserts wrong facts about the world.

## Technique to break it (verified 2026-07-13, SearXNG case)

1. **Re-check every binary the issue claims is absent.** `which docker` —
   never trust an "absent" claim. Environment state changes; the issue is a
   snapshot from a degraded window.
2. **Read the ACTUAL monitoring/health script the cron job runs** (here
   `<hermes-root>/scripts/searxng_watchdog.py`) to find the real probe TARGET
   and METHOD. The issue author may have transcribed the wrong port. In this
   case the issue said `:8080`; the watchdog probes `:8888` (the container's
   published mapping is `127.0.0.1:8888->8080`). Trust the script, not the issue.
3. **Probe the service exactly as the watchdog does:**
   `curl -s -o /dev/null -w "%{http_code}" --max-time 10 "http://localhost:8888/search?q=test&format=json"`
   and check the JSON result count (`len(results) > 0`).
4. **Run the watchdog script exactly as the cron job invokes it**
   (`python3 <hermes-root>/scripts/searxng_watchdog.py`) and capture the REAL
   exit code. This is ground truth — it bypasses stale `last_status`/`last_error`
   in jobs.json.
5. **If live state is healthy, mark the issue RESOLVED (not user-gated)** and
   leave the monitor running. Do not pause a healthy monitor.

## Outcome (this session)

Issue `oc_searxng_upstream_degraded_20260713` was open + `user_gated=true` +
`escalation_needed=true` since 12:07Z, claiming docker absent + port 8080 dead.
At 19:02Z live verification showed: docker present at `/usr/bin/docker`,
`searxng` container Up (8888->8080), service HTTP 200 with 1 result, watchdog
exit 0. Patched to `status: resolved`, `escalation_needed: false`. 0 open
escalated issues remaining. Residual root cause (intermittent upstream engine
rate-limit, `SearxEngineTooManyRequestsException suspended_time=180`) is
externally driven but auto-recovered by the watchdog.

## General rule

A prior scan's factual claims about environment state EXPIRE. The escalation
loop's "verify live state both directions" must extend to: re-derive the probe
target from SOURCE and re-check claimed-absent binaries LIVE — not just read
jobs.json `last_status`/`last_error`. Before concluding "user-gated /
unresolvable," confirm the issue's factual premise against reality.
