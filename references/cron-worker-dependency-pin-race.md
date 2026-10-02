# The restart-safe external cron worker's dependency pin/lease pair

Observed 2026-10-01 on `custodian:light` (job `31889beac268`), worker pid 3289607, execution `27fc593e4bcb4dc7847275decfd1a3ad`. Single occurrence; classified not-active after re-run. Documented because the mechanism is non-obvious and the fix must respect it.

## The failure

Worker handed off at 09:17:22 local, exited 1 at 09:24:38 without a durable terminal state:

```
runpy.run_module('cron.scheduler', ...)
  cron/__init__.py -> cron/jobs.py:32 -> cron/env_settings.py:15
  -> agent/secret_scope.py:23 -> utils.py:17 -> hermes_yaml.py:10
ModuleNotFoundError: No module named 'ruamel'
```

The import chain dies at the FIRST third-party import, before the worker publishes its ownership ack — so the scheduler records a failed dispatch whose side effects are declared unknown.

## The two legs

| Leg | File | Job |
|---|---|---|
| path pin | `cron/scheduler_worker_env.py::pin_hermes_tree_on_pythonpath` | puts `repo_root` AND the committed generation's `site-packages` on the worker's `PYTHONPATH` |
| generation lease | `cron/worker_bootstrap.py::worker_bootstrap` | gated on env marker `_HERMES_CRON_WORKER_BOOT`; runs `pm.environments.activate_dependencies`, which selects and leases the generation for the process lifetime |

`cron/__init__.py` calls `worker_bootstrap()` before importing `cron.jobs` — `-m cron.scheduler` executes the package first, so the boot happens ahead of the first dependency import.

## The 2x2 reproduction matrix

Run the runtime interpreter with `cwd=repo_root`:

| PYTHONPATH | marker | result |
|---|---|---|
| tree + generation site-packages | set | OK |
| tree + generation site-packages | absent | OK (custodian plugin entry-point warning only) |
| tree only | set | OK — `activate_dependencies` adds the path itself |
| tree only | absent | **`ModuleNotFoundError: No module named 'ruamel'`** ← the observed failure |

So the failure requires BOTH legs absent. Neither alone is sufficient.

## Traps when reproducing

- **Do not add `-I`.** Isolated mode drops the pinned path and fails earlier with `No module named 'cron'` — a different, unobserved failure. The worker is launched as `sys.executable -m cron.scheduler` with no isolation flags. Reproduce with the argv the failing process actually ran.
- **Verify the dependency exists** before suspecting a bad install: `pm.environments.committed_venv(repo_root)` + `site_packages(env)` on the live host returns a venv that DOES contain `ruamel`.
- **Do not "fix" by pip-installing into the runtime.** A pinned path is not a boot: without the lease, the PM collector may delete the generation between the gateway's exit and the worker's next import (#122290 review). The pin restores the path; the lease keeps it.

## Why it stayed Tier 3

The candidate hardening is in framework code and affects all 158 registered jobs:

1. Make `_launch_external_cron_worker` treat a missing committed generation as a dispatch failure with an explicit error, instead of spawning a worker that cannot import its own dependencies.
2. Have `worker_bootstrap` assert its dependency import resolves before the first third-party import in `cron/__init__.py`.

Both require deciding whether the pin or the lease is the invariant, and neither is reversible from an escalation run. Upstream already tracks this pair (#122222 pin, #122290 lease review).

## Promotion trigger

A second occurrence. Promote to an InsightProposal carrying the repro matrix above.

## Related

- `references/cron-worker-death-classification.md` — how to classify the death before doing any of this
- `references/operational-gotchas.md`