# Backup GitHub LFS Push Failure (GH008)

**Fingerprint:** A backup `git push` cron job (e.g. `Backup Hermes Sessions to GitHub`, `backup_system.sh`) fails with:

```
remote: error: GH008: Your push referenced at least 1 unknown Git LFS object:
remote:     <oid>
remote: Try to push them with 'git lfs push --all'.
```

**Root cause:** A commit was created referencing an LFS object the remote doesn't have — typically because a local LFS object was added/modified but `git lfs push` hadn't synced it yet, or a prior partial push left the remote missing the object. NOT an auth/credential issue, NOT a rebase conflict.

**Fix (Tier 1, deterministic, non-destructive):**
1. `cd <github_repo>` (e.g. `<fs-root>/indigo-repo`).
2. `git lfs push --all origin` — pushes every LFS object the local repo references. This clears the "unknown LFS object" error.
3. `git push origin main` — re-push the backup commit.
4. Verify: `git rev-parse HEAD` == `git rev-parse origin/main`.

**Verified in custodian light scan (2026-07-22):** The 00:17 backup commit failed GH008. After `git lfs push --all origin`, the subsequent `git push` succeeded and `HEAD == origin/main`. The stored `last_error` became stale on the next successful run.

**Gotcha — do NOT "fix" this by rewriting history or `git lfs prune`:** the object is genuinely needed by the pushed commit. `git lfs push --all` is the correct repair. (Contrast with `references/backup-disk-full-symlink-gotcha.md`, which is about disk space, not LFS object sync.)

**When to escalate (Tier 3):** If `git lfs push --all` itself fails — network/auth to the LFS endpoint, or a locally-corrupted object — that is a deeper issue requiring investigation, not the routine Tier-1 fix above.

**RECURRING-ROOT-CAUSE PATTERN (make it stop happening):** If GH008 recurs on every backup run (observed 4× in ~12h on the `Backup Hermes Sessions to GitHub` job: 07-21 12:06 / 18:01 / 20:36, 07-22 00:17Z), the cause is structural, NOT a one-off missing object. `backup_system.sh` copies freshly-churned LFS content (e.g. `chronicle.lbug` / `styx.db` change hourly) into `data/`, commits, then runs `git push origin main` with **no `git lfs push` first**. The newly committed LFS object is never uploaded → GitHub rejects with GH008. The Tier-1 manual `git lfs push --all` only clears the *current* symptom; the next hourly churn recreates it.

**Durable fix (applied 2026-07-22):** in `$HERMES_HOME/../indigo/scripts/backup_system.sh`, add `git lfs push origin main` BEFORE the `git push origin main` line, with a failure branch that skips the commit push if LFS upload fails (so a broken LFS upload never produces a GH008-triggering commit). Repo at `<fs-root>/indigo-repo`. This makes future hourly runs self-heal — no recurrence.

**Verify the fix stuck:** `bash backup_system.sh --json` → expect `status:"ok"`, `errors:0`; then `git lfs push origin main` (exit 0) + `git push origin main` (exit 0, "Everything up-to-date") with NO GH008; confirm `git rev-parse HEAD == git rev-parse origin/main` and `git lfs status` shows empty "Objects to be pushed".

**Related drift (NOT GH008):** `weave.lbug` may be perpetually SKIPPED if its configured source path no longer exists (observed: `commons/db/ocas-weave/weave.lbug` moved to `archive-lbug-20260713/weave.lbug` on 07-13, leaving a stale 07-13 copy in the repo's `data/`). That's a backup-integrity gap for the user to decide on — separate from the LFS push failure.
