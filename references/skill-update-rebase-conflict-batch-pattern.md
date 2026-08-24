# Skill update-wrapper failure patterns (rebase-stuck batch, path mismatch)

Consolidated failure signatures for `:*:update` cron jobs. All three are NOT network/auth issues.

## When to read
- Many `:*:update` jobs fail simultaneously with git rebase/merge/am errors → read all sections.
- A single update job fails with 'Script exited with code 1' after repo sync → read Path Mismatch.

## Rebase-stuck batch pattern (confirmed 2026-07-22)
: When many `:*:update` cron jobs simultaneously fail with `FAIL: ... Could not apply ... git rebase --abort`, the affected skill repos are typically left with stuck `MERGE_MSG` / rebase-merge state. This is NOT a network/auth issue. Fix per repo: `git merge/rebase/am --abort || true`, then `git reset --hard origin/main && git clean -fd`, and verify `HEAD == origin/main`. After repo sync, rerun each affected update wrapper individually. If one wrapper still fails after sync, inspect whether it still calls a legacy helper path before rescheduling. Confirmed 2026-07-22.

## Wrapper path mismatch (confirmed 2026-07-22)
A wrapper may call a legacy hardcoded helper that no longer exists after environment refactors (`python3 ~/.hermes/scripts/skill_update.py ...` → file missing). The modern canonical wrapper is `bash ~/.hermes/profiles/<profile>/scripts/update_skill.sh <skill>`. When an update fail still shows “Script exited with code 1” after repo sync, read the wrapper directly; if it delegates to a missing target, rewrite the wrapper to the canonical helper, then rerun. Confirmed 2026-07-22.

## Merge-conflict batch pattern (confirmed 2026-07-27)
(merge-conflict fix loop, confirmed 2026-07-27): When multiple `:*:update` cron jobs simultaneously fail with `FAIL: ... unresolved conflict` from `git pull --ff-only`, the affected skill repos sit in a diverged state with merge conflict markers in the working tree (UU files on `README.md`, `SKILL.md`, etc.). This is NOT a network/auth issue — the upstream has commits that conflict with local branch state. Fix per repo (all are idempotent — safe to run on all 6 simultaneously):
  1. `cd <repo> && git merge --abort 2>/dev/null; git rebase --abort 2>/dev/null; git am --abort 2>/dev/null; true`
  2. `git reset --hard origin/main` (repos with HEAD at their own "fix: resolve merge-conflict markers" commit — these have local edits that must be discarded for the update to succeed)
  3. `git clean -fd` (remove untracked conflict artifacts)
  4. Verify `HEAD == origin/main` (both should match, e.g. `1e948a2`)
  5. Re-run each update wrapper (`bash ~/.hermes/profiles/<profile>/scripts/update_skill.sh <skill>`) — expect `OK`
  6. After wrapper succeeds, force-flip the registry: `hermes cron run <job_id>` for each previously-failing job — expect `Ran now: succeeded.`
  7. Re-read `jobs.json` to confirm `last_status` flipped from `error` to `ok` and `last_error` cleared.
  **Post-fix verification is mandatory**: a re-run of `update_skill.sh` alone can succeed while `jobs.json` still shows stale `error` — the cron registry only updates on the next scheduled execution unless you force it with `hermes cron run`. See `references/skill-update-rebase-conflict-batch-pattern.md` and `references/hermes-cron-run-verify-recipe.md`. Confirmed 2026-07-27: 6 repos (`ocas-vesper`, `ocas-styx`, `ocas-praxis`, `ocas-forge`, `ocas-sift`, `ocas-custodian`) — all clean after reset, all 6 `hermes cron run` calls returned `succeeded`.

### Escal
