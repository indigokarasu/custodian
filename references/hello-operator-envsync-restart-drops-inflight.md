# hello-operator env-sync restarts drop in-flight streaming turns (2026-09-29)

Fingerprint: `oc_hello_operator_envsync_restart_drops_inflight`
Fix id: `esc_20260929T0945Z_envsync_quiet_deferral` (Tier 1, applied + revalidated)

## The trap that costs the most time

In `/root/work/HelloOperator/hello_operator/server.py` the class that serves
`/v1/chat/completions` is **named `Router`** (line ~199). So:

- `ps ... | grep router` finds nothing.
- "The gateway never restarted" is **evidence about a different process**.
  The Hermes gateway (`hermes gateway run`, pid observed 3984354) and
  hello-operator (systemd unit, `--rank-free` CLI) are separate services.

An issue titled "router restart churn" can be about either. Check WHICH.

## Identified root cause

`/root/.hermes/scripts/hello-operator-env-sync.sh` line 13 runs
`systemctl restart hello-operator`, driven by
`hello-operator-env-sync.timer` (`OnUnitActiveSec=5min`). It restarts only when
the rendered `/etc/hello-operator.env` differs — i.e. when the Nous token
rotated. Token lifetime is **60 min** (`expires_at - obtained_at` in
`/root/.hermes/shared/nous_auth.json`), so a rotation is noticed within ~45 min.

Result: ~13 restarts/day, each closing the aiohttp listener and aborting every
in-flight stream, which every Hermes profile sees as
`Streaming failed before delivery: Connection error.`

## The restart is required — do not "fix" it by skipping

systemd reads `EnvironmentFile=` once at start and the app expands
`${NOUS_API_KEY}` from its own environment (`config.py:229`
`os.path.expandvars`). A rotated token needs a reload. The outage is
*avoidable*, not the restart.

## How the caller is identified

`/root/.hermes/scripts/ho-stop-forensics.sh` runs as **ExecStopPost**, so
`systemctl`'s caller and its parent chain are still alive at that instant. It
writes to `/root/.hello-operator/stop-forensics.log`. **Read this first** when
a unit restart looks unexplained — it is the only in-band record of the caller.
It captured `hello-operator-env-sync.sh -> systemctl restart hello-operator`.

## The applied fix

Wait for a quiet moment before restarting:

- `QUIET_AFTER_S=25` — no completed chat turn for this long
- `MAX_WAIT_S=120` — total budget; then restart anyway (never worse than before)
- `SAFETY_S=30` — urgency guard: if the OLD token has less life than the
  remaining budget, restart now
- `flock -n 9` on `/run/lock/hello-operator-env-sync.lock` so a waiting run
  cannot overlap the next timer tick
- `HELLO_OPERATOR_ENVSYNC_DRYRUN=1` to exercise the path without restarting

## Two implementation gotchas

1. **`journalctl -o short-unix` emits fractional seconds** (`1790700245.597409`).
   `$(( now - last ))` then fails. Truncate: `cut -d. -f1`. Run the arithmetic
   before deploying — it was caught only because the pre-deploy test executed it.
2. **`cp -p` between the scratch file and the live script can change the mode.**
   The live file was 0755; the copy landed 0644. Re-`chmod 755` and verify with
   `ls -la`. The unit invokes it via `/bin/bash <path>` so a lost +x would not
   break the unit, but the file is no longer directly runnable.

## How to measure "quiet"

Last **completed** `POST /v1/chat/completions` in the aiohttp access log:

```
journalctl -u hello-operator --no-pager -o short-unix -n 4000 \
  | grep 'POST /v1/chat/completions' | tail -1 | cut -d' ' -f1 | cut -d. -f1
```

Empty (unmeasurable) => restart now, never treat as idle.

## Ruled-out suspects (don't re-derive)

- `hello-operator-watchdog.sh` — fires only on two failed `/healthz` probes
  (`logger ... "healthz failed twice"`). 0 such lines on a churn day; the 60s
  timer otherwise deactivates cleanly.
- `hello-operator-rank.service` — `OnCalendar=*-*-* 04:20:00` +
  `RandomizedDelaySec=600`, with `ExecStartPost=try-restart`. Daily only.
- root crontab — no restart entry.
- The Hermes gateway — uninterrupted across every cited window.

## Revalidation that actually proved it

Run the **systemd unit**, not the inner script, while a turn is in flight:

```
systemctl start hello-operator-env-sync.service
journalctl -u hello-operator-env-sync --since "-2min" --no-pager | grep deferring
```

Expected trail: `deferring (0s/120) ... (20s/120) ... quiet 31s; restarting now`.
Then assert `/healthz` 200 and **zero** lost turns in the window. An unchanged
`MainPID` on the next timer run proves the early-exit path (token already synced).
