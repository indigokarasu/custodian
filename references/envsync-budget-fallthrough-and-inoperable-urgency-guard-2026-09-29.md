# Env-sync quiet gate: two defects that survive a "successful" prior fix

**Date:** 2026-09-29. **File:** `/root/.hermes/scripts/hello-operator-env-sync.sh`
**Fingerprint:** `oc_router_restart_churn_connection_refused` (variant `budget_fallthrough`)

## Why this reference exists

A Tier 1 fix for this fingerprint was applied at 19:13Z the same day and its journal
recorded it as revalidated. **The same fingerprint recurred one token rotation (~47 min)
later, on a different job, and the fix was not the cause.** Both defects below are
about a guard that *looks* correct in the diff and is inert in the code.

## Defect 1 — the fall-through path was never guarded

A wait-loop gate was added with an in-flight check **inside the loop body**:

```bash
waited=0
while [ "$waited" -lt "$MAX_WAIT_S" ]; do
  ...
  if [ "${inflight:-0}" -gt 0 ] && [ "$age" -ge "$QUIET_AFTER_S" ]; then
    sleep 10; waited=$((waited+10)); continue    # guard lives here only
  fi
  ...
done
systemctl restart hello-operator                   # <-- no guard, ever
```

When the budget expired the loop fell straight through to the restart. A **correct
gate still produced the exact restart it was written to prevent.** The measured
failure: the gate logged `deferring restart (0s/120)` … `(110s/120)` and then
`nous token rotated; service restarted` at 120s while a 12-minute cron turn was
still streaming — 4 `Streaming failed before delivery: Connection error` in 6s.

**Rule:** when adding a guard to a loop that exits into an action, **audit every exit
path**, not just the loop body. A `break` and a loop-exhaustion are different exits and
both must be guarded. Grep for the action and count its reachable predecessors.

**Fix shape:** after the loop, if work is still in flight, keep waiting up to a cap;
if the cap is reached, **skip the restart entirely** and exit 0. The rotated value is
already persisted to the config file, so a skipped rotation loses nothing — it is
applied next cycle. Skipping is strictly better than cutting live work.

## Defect 2 — the urgency guard measured the wrong token

The script *replaces* a token in `/etc/hello-operator.env`, and the function
`old_token_secs_left()` read that **same file** back to measure remaining life. But
the write happened **earlier in the file** than the call:

```bash
printf '%s\n' "$NEW" > /etc/hello-operator.env     # line 28: new token written
...
left=$(old_token_secs_left)                       # reads the file -> NEW token
```

So the "is the old token about to expire?" check always measured the **new** token and
concluded the old one was long-lived. **The guard could never fire.** Confirmed by
counting its own log line: `old token has Ns left` appeared **0 times** across 3
rotations that day.

**Rule:** a guard that reads state the same operation just mutated is measuring the
result, not the input. **Verify a safety guard by triggering it** — grep for its log
line and check the count is non-zero over a window where it *should* have fired. A
guard with zero lifetime firings is more likely broken than unnecessary.

**Fix shape:** capture the value **before** the mutation
(`OLD_SECS_LEFT_AT_START=$(old_token_secs_left_now)`), and make it overridable
(`${OLD_SECS_LEFT_AT_START:-...}`) so a harness can inject values.

## Defect 3 (introduced by the fix, caught in revalidation) — branch order

Placing the new in-flight branch **before** the existing urgency branch meant an
expiring token deferred behind a live turn — a deadline shadowed by the guard meant to
protect it. The harness case for "urgent token + live turn" **hung instead of
restarting**, which is the tell.

**Rule:** in a gate, a **hard deadline outranks a soft protective condition.**
Evaluate urgency first in *every* path, including post-loop paths. When a revalidation
case hangs rather than fails, suspect your own ordering before blaming the test.

## Sentinel-value trap

`old_token_secs_left` printed `-1` on any parse failure. With `if [ "$left" -lt "$budget" ]`,
`-1` is less than any positive budget — so **an unmeasurable token silently masqueraded
as urgent** and forced an immediate restart. Use `[ "$left" -ge 0 ] && [ "$left" -lt "$budget" ]`
so only a *measured* value can trip urgency.

## Harness notes (so this is re-runnable)

Run the **real script's control flow**, not a paraphrase: copy it, rewrite only the
`/etc` paths and force the "token did not change" early exit open, then stub `ss`,
`journalctl`, `logger`, `systemctl`. Three harness bugs, each of which produced a
confident false result:

1. **sed delimiter clash** aborted the harness silently; every case exited early and
   reported a meaningless "fell through". **Assert the sed actually applied.**
2. **A fixed epoch** in the `journalctl` stub made `age` huge, so every case
   short-circuited into the "quiet" branch and the guarded branch was never reached.
3. **The `ss` stub omitted its header row.** The script does `tail -n +2` to skip ss's
   header — so the single fake socket was discarded and `inflight_chats()` always
   returned 0. Stub defect, not script defect; real `ss` always prints a header.

**A harness that exits 0 while testing nothing is worse than no harness.** Assert on
the case output, not the exit code.

## Timing note

Cases that exercise the cap take ~7 min each (120s budget + 300s cap, 10s polls). Run
in the background. A case that "hangs" may be a real defect, not slowness.
