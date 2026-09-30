# Env-sync quiet gate: the completed-turn blind spot

**Fingerprint:** `oc_router_restart_churn_connection_refused`
**First seen:** 2026-09-29 12:00:30 PDT
**Status:** Tier 1 fix applied, pending two live rotations

## The class

`hello-operator-env-sync.sh` restarts `hello-operator.service` whenever the Nous
token rotates (~every 45 min). Cutting the listener kills in-flight streaming
turns, which every Hermes profile observes as:

```
Streaming failed before delivery: Connection error.
openai.APIConnectionError: Connection error
```

The restart is *required* — systemd reads `EnvironmentFile` once at start, so a
new token cannot be picked up without one. The outage window is the only
avoidable part.

## Why the first fix was not the fix

An earlier Tier 1 fix (2026-09-29 16:45Z) added a quiet gate:
`QUIET_AFTER_S=25`, `MAX_WAIT_S=120`, `SAFETY_S=30`. It narrowed the blast
radius and left the blind spot intact.

The gate's `last_chat_epoch()` reads the aiohttp access log, **which is written
at turn completion**. During a long streaming turn that log is silent, so:

```
age = now - last_completion
```

grows past `QUIET_AFTER_S` *mid-turn*, and the gate concludes "quiet" and
restarts underneath the very turn it is waiting for. At 12:00:30 it logged
`quiet 25s; restarting now`; one second later 4 turns of `10khr-grind` aborted
in 6s.

> A quiet access log is ambiguous between "no work" and "work still streaming".
> The gate resolved that ambiguity toward "no work" — the unsafe direction.
> **Raising a threshold does not repair an ambiguous measurement.** Shortening
> the window is not the same as making the reading correct.

## The fix

A turn in flight holds an ESTABLISHED socket to the router, so connection state
is the in-flight signal the access log cannot give:

```bash
inflight_chats() {
  local n
  n=$(ss -tn state established '( sport = :8800 )' 2>/dev/null | tail -n +2 | wc -l)
  echo "${n:-0}"
}
```

Placed before the age-break, so the loop defers when sockets are up while the
access log is quiet. `inflight=0` still restarts — normal rotation unchanged.
`MAX_WAIT_S` still forces the restart regardless, so the fix cannot make things
worse.

## Detection recipe

1. `systemctl show hello-operator -p ActiveEnterTimestamp -p ExecMainStartTimestamp -p NRestarts`
   — a scripted stop has `NRestarts=0` and `ActiveEnterTimestamp == ExecMainStartTimestamp`.
2. `journalctl -t hello-operator-env-sync -n 40` — the gate's own reasoning.
3. `grep -c "unit stopped" /root/.hello-operator/stop-forensics.log` and match the
   stop timestamp against `Streaming failed` lines within ~15s.
4. Confirm the victim job appears in **no** open issue's `affected_job_ids`
   before calling it covered (Step 8f).

## Re-validation

The loop is only reachable when the token actually differs from
`/etc/hello-operator.env`. A `HELLO_OPERATOR_ENVSYNC_DRYRUN=1` run between
rotations exits at the "token did not change" guard and proves nothing about the
loop. Exercise the decision path in a harness that stubs the env write and
`systemctl restart`, replaying the known-bad inputs:

| in-flight sockets | last completion | expected |
|---|---|---|
| 2 | 40s ago | DEFER (pre-fix: "quiet 40s; restarting now") |
| 0 | 40s ago | RESTART (unchanged) |
| 2 | 5s ago | DEFER on the pre-existing age branch (unchanged) |

## Related

- `references/hello-operator-envsync-restart-drops-inflight.md` — the original class
- `references/escalation-loop-pitfalls.md` — Step 8f gap handling
