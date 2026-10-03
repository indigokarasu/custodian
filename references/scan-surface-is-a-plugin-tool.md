# The Scan Surface Is a Plugin Tool, Not a `hermes` CLI Verb

Extracted verbatim from `SKILL.md` on 2026-10-03 (sha256[:12] = `c3d048024b47`,
2002 chars).

**When to read:** whenever a scan reports that `custodian.scan.light` (or
`custodian.scan.deep`) is "not a hermes command", or when you are tempted to run
`hermes update` because a command appears to be missing. Read this BEFORE
diagnosing, not after — the 2026-10-01 incident cost a half-updated install and
~11 minutes, and none of it was needed for the scan.

---

**⚠️ `custodian.scan.light` IS NOT A `hermes` CLI COMMAND — never diagnose its absence by updating Hermes (measured 2026-10-01):** the plugin exposes scan through a **registered tool** (`custodian_scan`, mode `light`/`deep`) and a **slash command** (`/custodian scan light`), NOT a CLI verb; `hermes custodian.scan.light` returns `'custodian.scan.light' is not a hermes command`. The reflex to run `hermes update` when a CLI command is missing is exactly what a light scan must never do: on 2026-10-01 a light scan hit that dead end, ran `hermes update`, the update pulled 283 commits, hit the foreground timeout mid desktop-build, and **the scan itself SIGTERMed it with `pkill -f 'hermes update'`** — leaving every subsequent `hermes` call printing `a source update is unfinished` plus `did not restart running gateways`. Cost: a half-updated install, a 6-minute repair run, and ~5 minutes of wall clock for a scan that needed none of it. **Rule:** (a) never invoke `hermes update` (or restart any gateway/dashboard) from a light scan — an update is Tier 2+ and user-gated by nature; if the CLI appears broken, that is a **finding to journal**, not a repair to attempt; (b) when a command is unknown, confirm the surface first (`hermes --help`, the plugin's `register()`, or `~/.hermes/plugins/custodian/hermes_custodian_plugin/__init__.py`) before concluding anything; (c) do the scan by reading `jobs.json` + the log set with the scripts in this skill, which needs no CLI at all; (d) if an update IS somehow already running, let it finish — never `pkill` it; a killed updater leaves `.update-incomplete`-class state and an install whose HEAD and checkout disagree. After an update completes, the fleet-restart warning can persist by design (it defers the gateway restart to process exit) — that residual is not a failed fix, and restarting the gateway from a cron run is itself a tracked fault (`oc_needrestart_mass_restart_drops_live_turns_20260930T1401Z`).

Run gates (all apply in cron context):
