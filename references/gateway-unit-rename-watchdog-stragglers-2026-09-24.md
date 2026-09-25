# Gateway unit rename -> watchdog stragglers (sweep after unit-rewriting updates)

`hermes update` renamed the gateway user unit on 2026-09-20 04:37:
`hermes-gateway-indigo.service` -> `hermes-gateway.service`. Scripts that
hardcode the old name keep running against the retired name — watchdogs
silently mis-aim (restart/check a unit that no longer exists).

## Symptom (this class)
- `gateway-mem-watchdog.sh` logged `DOWN: state=inactive mainpid=0` +
  `restart command FAILED` every 3 min from 09-20 04:39 to 09-24 17:08
  (~2,100 entries) while the gateway was healthy under the new name.
- `gateway_memory_watchdog.py` (hermes-cron, every 2m) would have no-op'd at
  CRIT: its restart path targeted the retired unit.
Both were FALSE alarms — the gateway itself never went down.

## Fix pattern: resolve the live unit at runtime (never hardcode)
Probe both names, prefer loaded/active; all gateway watchers use this now:
- `gateway_health_check.py` — `_resolve_service()` (best reference impl)
- `gateway-mem-watchdog.sh` — `resolve_svc()`
- `gateway_repair.py` — resolver for SERVICE/DROP_DIR
- `gateway_memory_watchdog.py` — `_resolve_gateway_service()`

## Sweep recipe (run after ANY update that rewrites unit files)
    grep -rln "hermes-gateway-indigo" ~/.hermes/scripts ~/.hermes/profiles/indigo/scripts
Remaining hits are OK only if they are probe-list candidates, comments, or
`.bak` files. Any live hardcoded use = fix with the probe pattern.
Also sweep: system crontab entries, hermes cron `script` jobs, other watchers.

## Verify a watchdog still WORKS after the fix (don't trust 'no error')
- `gateway-mem-watchdog.sh`: `tail ~/.hermes/logs/gateway-mem-watchdog.log`
  -> expect a fresh `RSS <n>MB ok` every ~3 min; run once manually for instant
  evidence. (2026-09-24: confirmed `RSS ok` at 17:09/17:12/17:15 after fix.)
- `gateway_memory_watchdog.py`: run it (rc=0, silent when RSS < CRIT); confirm
  `GATEWAY_SERVICE` resolves to `hermes-gateway` and `get_service_active()`=True.
- Env: hermes-cron scripts inherit `XDG_RUNTIME_DIR` + `DBUS_SESSION_BUS_ADDRESS`
  from the gateway process (verify: `tr '\0' '\n' < /proc/<gateway-pid>/environ`);
  system-crontab entries must export them (the .sh does).

## Related open item
Same rename orphaned `~/.config/systemd/user/hermes-gateway-indigo.service.d/`
(memory/OOM/drain-timeout drop-ins — not applied to the new unit). Flagged for
Mentor/owner in the custodian journal of 2026-09-25.
