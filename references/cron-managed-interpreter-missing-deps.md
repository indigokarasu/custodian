# Cron `no_agent` scripts run under the MANAGED interpreter, not the venv

**Signature:** a `no_agent: true` cron job fails in under a second with
`ModuleNotFoundError: No module named '<pkg>'`, while the *identical* script
runs perfectly when invoked by hand.

**Why it happens.** Hermes executes `no_agent` script jobs with the bare
managed runtime on PATH:

```
/root/.hermes/tools/python-3.14.7+<stamp>-linux-x64/bin/python3
```

That runtime is a clean pip-only install — **no third-party packages at all**
(not even PyYAML). Interactive shells resolve `python3` to a Hermes venv
(`/root/.hermes/installs/<id>/environments/<hash>/venv/bin/python3`) that *does*
have the deps, and `/usr/bin/python3` often does too. So every manual test
passes and only cron fails. The failure is structurally invisible to
interactive verification.

**How to confirm the mismatch is real (do not assume):**

1. `which -a python3` — note every candidate.
2. Probe each candidate for the missing module; the managed one fails.
3. Reproduce exactly: run the script with the managed interpreter and a
   minimal env, confirm the same `ModuleNotFoundError`.
4. `grep -n "sys.executable\|shutil.which" <hermes-source>` to see which
   interpreter the scheduler actually picks.

**Fix (Tier 1, root-cause):** install the package into the managed runtime
that cron actually uses — do not "fix" it by editing the script to avoid the
import, and do not assume the venv is the one cron sees.

```
<managed-python> -m pip install --quiet <pkg>
```

**Blast-radius audit before/after — do this, it is cheap and it is the
difference between a scoped fix and a guess:** iterate every cron job with a
`script` field, resolve the script to its on-disk path (the same file is often
hard-linked at both `/root/.hermes/scripts/` and
`/root/.hermes/profiles/indigo/scripts/` — same inode, so do not treat them as
two scripts), and collect its top-level `import`/`from` statements. Report
every script importing a non-stdlib module. In the confirmed instance
(2026-09-26) exactly one of ~150 job scripts imported a third-party module, so
the fix was provably scoped to a single job.

**Pitfall:** do not re-run the job interactively as "verification" — it will
pass and prove nothing. Re-validate through the registry
(`hermes cron run <id>`) and confirm the *original* signature is gone. Note
that a single `hermes cron run` on a long enrichment job can exceed a
foreground command timeout; run it backgrounded and poll the registry, and do
not read a still-stale `last_error` as a failed fix.

**Follow-on:** clearing the import error often exposes a *second*, previously
masked failure downstream. Treat that as a new fingerprint, not as the fix
failing. Confirm it (2026-09-26: the next failure was `database is locked` on
a 2.4G sqlite db under a 2-core host), and do not assume the original fix
failed because the job is still red.
