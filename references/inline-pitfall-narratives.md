# Inline Pitfall Narratives

Extracted from SKILL.md 2026-10-08. Full pitfall catalogue (40+ rules):
`references/measurement-pitfalls.md`.

## Entry Module Probe (2026-10-05)

**NEVER exercise a Hermes entry module (`python -m cron.scheduler`, or any `__main__`) as a
verification probe.** Measured 2026-10-05T04:00Z: a light scan's own diagnostic ran
`<bare store interpreter> -m cron.scheduler --help` with `cwd=/root/.hermes/hermes-agent` but
WITHOUT the PYTHONPATH that `cron/scheduler_worker_env.pin_hermes_tree_on_pythonpath` builds for
a real worker. That pin adds `repo_root` AND the PM-committed generation's `site-packages`
(issues #112729 / #122222); without it the child sees only the bare interpreter's stdlib
site-packages, so `hermes_cli/env_loader.py` -> `import dotenv` raises and the scheduler marks
**every job it cannot dispatch** `state=error` — a **15-job error storm** in the registry the
probe was reading, aimed at a single job. Root cause was absent: the committed site-packages
held dotenv, ruamel.yaml and croniter (279 entries), and the pinned import chain returned rc 0.
14 of 15 cleared on their own next scheduled fire. **Rule:** verify import health by importing
the PINNED chain (repo + committed site-packages on `PYTHONPATH`), which is what actually runs;
never by invoking the entry point. This is the destructive-verification-probe class applied to a
*registry* rather than a journal: the probe's blast radius is every job, not the one under test.

## ImportError During hermes update (2026-10-07)

**An `ImportError` from a repo module during an in-flight `hermes update` is transient, not a
defect — check for the updater before persisting an issue.** Measured 2026-10-07T04:03-04 local:
two `cannot import name 'is_recurring' from 'cron.constants'` errors (jobs `lucid:user-dream`
aece38b3e953 and `custodian:deep` c3cdf6e7d887) landed while a `hermes update` was running
(`cron/constants.py` mtime 04:00:14, `cron/jobs.py` 04:01:08 — the files were rewritten as the
scheduler imported them). 0 occurrences after the update window; a bare re-import of the pinned
chain returned rc 0 and `is_recurring` resolves. **Rule:** before writing an issue for any
import-failure signature, `ps` for an in-flight `hermes update` / `pm repair` and re-run the
import against the on-disk file; if it now resolves AND the last occurrence falls inside the
update window, it is a transient artifact — do NOT write an issue (that would be a false
escalation, the same class as the stale-premise guard).

## Shared Store Interpreter (2026-10-07)

**Read the module that documents the symptom BEFORE fixing it, and never install into a shared
store interpreter as a first move.** The same run installed `ruamel.yaml` into
`/root/.hermes/tools/python-3.14.7+.../` because the registry's traceback said
`ModuleNotFoundError: No module named 'ruamel'`. `cron/scheduler_worker_env.py`'s module
docstring names that EXACT string as issue #122222 and prescribes the pin — a package install
into the bare store interpreter treats the symptom of a missing PYTHONPATH and mutates a shared
runtime that many profiles inherit. Reverted (`pip uninstall`, verified `import ruamel.yaml` -> rc 1).
**Rule:** the fix that makes a traceback go away is not the fix that makes the *cause* go away;
if the failing import happens in a child, find the module that sets the child's environment
before touching site-packages. Corollary: `sys.executable -m pip install` against
`/root/.hermes/tools/python-*` is a Tier 4-adjacent mutation and should be treated as
`user_gated` even though it is reversible.

## Named Driver Verification

**A named driver must exist on THIS host, and repeated identical control-log lines
prove it did not run.** A filed attribution is a CLAIM, and crediting it retires the
search — re-locate growth by differential `du` over a real interval, not by re-reading
the attribution. Full entry, with the measured numbers:
`references/measurement-pitfalls.md`.
