# no_agent AGENT_ROOT env-propagation fix pattern

**Symptom:** A `no_agent: true` cron job fails every run with `FileNotFoundError` (or
"Database file does not exist") pointing at a path containing a doubled `home/.hermes`
segment that does not exist on disk, e.g.
`<hermes-home>/home/.hermes/commons/data/ocas-weave/config.json`.

**Root cause:** Many skill scripts compute their data/db paths as:
```python
AGENT_ROOT = Path(os.environ.get("AGENT_ROOT", Path.home() / ".hermes"))
```
In interactive/agent sessions `AGENT_ROOT` is set (or `Path.home()` resolves to `/root`),
so paths land at `<hermes-root>/...`. But the `no_agent` cron wrapper scripts (e.g.
`rr_weave_sync.sh`) typically export only `HERMES_HOME`, never `AGENT_ROOT`. In the
`no_agent` execution context `Path.home()` resolves to a scratch dir (e.g.
`/tmp/tmp.XXXX/.hermes`), producing the bogus `home/.hermes` path. The script can't find
its config/db and exits 1.

This is a SIBLING of (not duplicate of):
- `references/cron-script-path-home-pattern.md` — literal `Path.home() / ".hermes"` in a
  script that should hardcode `<hermes-root>`.
- `references/subdirectory-hints-home-dir-pattern.md` — a specific framework bug
  (`subdirectory_hints.py` raising when `$HOME` unset).
The fix here is at the WRAPPER layer (missing env export), not inside the skill package.

**Fix (Tier 1, no skill-package edit needed):** Add `export AGENT_ROOT=<hermes-home>`
to the failing job's wrapper script, alongside the existing `export HERMES_HOME=...` line.
The skill code already honors the env var, so this is a one-line wrapper edit — Custodian
must NOT edit the skill package itself (see "Never modify files inside skill package
directories"). Re-enable any jobs that were paused for the bug (`enabled: true`,
`paused_at: null` in jobs.json).

**Verify before claiming fixed:** Run a probe with the exact env the wrapper sets:
```bash
export HERMES_HOME=<hermes-home>
export AGENT_ROOT=<hermes-home>
python3 -c "
from pathlib import Path
import os
r=Path(os.environ.get('AGENT_ROOT', Path.home()/'.hermes'))
p=r/'commons/data/<skill>/config.json'
print('AGENT_ROOT =', r)
print('config path =', p)
print('exists:', p.exists())
"
```
Confirm the printed path matches the real on-disk layout
(`<hermes-home>/commons/...`, NO `home/.hermes` segment) and `exists: True`.

**Custodian fingerprints this maps to:** `oc_weave_path_home_resolution_bug`,
`oc_weave_skill_path_bug` (both the same root cause — confirmed 2026-07-13: fixed via
`AGENT_ROOT` export in `rr_weave_sync.sh` + `rr_weave_enrichability.sh`, resumed 3 jobs,
config-load probe returned exit 0).

**Honesty boundary:** This bug is fully auto-fixable. Do NOT conflate it with genuinely
user-gated siblings that share the `FileNotFoundError` surface but need owner's action
(Google Tasks 403 re-auth, Spotify token, SDK validation bugs). Those stay open.
