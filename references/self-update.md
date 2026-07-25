# Self-Update Procedure (custodian.update)

**⚠️ Critical pitfall:** Git tags with higher version numbers may be OLDER commits on divergent branches. Always compare commit dates, not version strings. The v1.5.1 tag in this repo is a historical commit from 2026-03-31 that predates the v1.3.0+ Hermes adaptations — it contains hardcoded `~/openclaw/` paths and the removed `scripts/custodian.py` (which calls `openclaw` binary). Adopting it would break all Hermes-specific functionality.

**Divergent branch warning:** Topic branches (docs/known-code-fixes, merge/skill-status-diagnostic, etc.) are often **divergent** from HEAD — `git merge-base HEAD origin/<branch>` returns empty. Never `git merge` these branches. Extract content as manual patches instead. See `references/divergent-branch-handling.md` for the full detection and extraction procedure.

## Update steps (Hermes)

1. **Fetch remote state:**
   ```bash
   cd {skill_root} && git fetch origin
   ```

2. **Check for new commits on origin/main:**
   ```bash
   git log HEAD..origin/main --oneline
   ```
   If empty, no new commits to pull. Do NOT stop yet — see step 2.5.

3. **Check GitHub releases** (more reliable than tags):
   ```bash
   gh release list -R indigokarasu/custodian --limit 5
   ```
   The latest release is the canonical version. Tags like v1.5.1 may be historical artifacts on older branches.

4. **⚠️ Check topic branches for unmerged content** — Even if origin/main has no new commits, other remote branches may have valuable changes. After step 2, scan all remote branches:
   ```bash
   git branch -r
   ```
   For each non-main branch, check if it has commits not in HEAD:
   ```bash
   git log HEAD..origin/<branch> --oneline
   ```
   Assess each candidate the same way as step 5 (compatibility check). Merge branches that add documentation or fix patterns without removing Hermes adaptations. Do NOT merge branches with OpenClaw paths, removed sections, or `scripts/custodian.py` re-additions.

5. **⚠️ Stash local changes before merging** — The plugin directory may have local uncommitted changes (classifier.py schema guards, pyproject.toml build-backend fix, hook signature patches). Stash before merging:
   ```bash
   git stash push -m "local fixes: <description>"
   ```
   After merge completes, restore them:
   ```bash
   git stash pop
   ```
   **If `git stash pop` produces merge conflicts** (common when upstream also modified the same files):
   - **Do NOT use `git checkout --theirs` blindly** — this discards stashed changes silently (e.g., version string reverts to old value)
   - Manually edit conflicted files to keep the best of both sides
   - After resolving: `git add <file>` for each resolved file
   - **Verify `__version__`** in `__init__.py` — fix with `sed -i 's/__version__ = "X.Y.Z"/__version__ = "A.B.C"/'` if needed
   - Commit: `git commit -m "merge: apply local fixes on top of upstream vA.B.C"`
   - `git stash pop` auto-drops on clean apply but NOT on conflicts — run `git stash drop` after successful resolution

6. **Assess compatibility before merging** — if there ARE new commits on origin/main or a topic branch:
   - Check `git diff HEAD..origin/main -- SKILL.md` for path references (`~/openclaw/`, `/tmp/openclaw/`, `openclaw cron` commands) that are incompatible with Hermes
   - Check if `scripts/custodian.py` was re-added (it calls `openclaw` binary which doesn't exist on Hermes)
   - Check if `skill.json` was re-added (we removed it in favor of SKILL.md frontmatter)
   - If incompatible: do NOT merge. Document as "update skipped — incompatible upstream changes". Record in `decisions.jsonl`.

7. **If compatible, merge with Hermes patches preserved:**
   ```bash
   git merge --no-edit <branch-to-merge>
   ```
   Then review and restore any Hermes-specific adaptations that were overwritten.

8. **⚠️ Resolve conflicts by keeping both sides** — When topic branches add new sections at the same insertion point, resolve by keeping BOTH sections. Check for conflict markers:
   ```bash
   grep -n '<<<<<<<\\|=======\\|>>>>>>>' SKILL.md
   ```
   Open the file, identify the conflict region, and replace the entire block with both sections concatenated.

   **Reliable conflict resolution pattern (Python regex):**
   ```python
   import re
   with open('SKILL.md', 'r') as f:
       content = f.read()
   # Keep stash side (includes upstream + local additions)
   content = re.sub(
       r'<<<<<<< Updated upstream\n(.*?)\n=======\n(.*?)\n>>>>>>> Stashed changes',
       lambda m: m.group(2),
       content, flags=re.DOTALL
   )
   with open('SKILL.md', 'w') as f:
       f.write(content)
   ```
   **Caveat:** The regex approach works when each conflict region has exactly ONE `<<<<<<<`/`=======`/`>>>>>>>` triplet. If a single conflict region contains MULTIPLE `=======` markers (multi-section conflicts), the regex will match too broadly. In that case, resolve those specific conflicts manually by editing the file directly.

9. **Update SKILL.md version metadata** to reflect the actual installed version.

10. **⚠️ Record the decision using write_file, not shell redirect** — The security scanner blocks shell redirects (`echo >>`) to `{agent_root}/` directory files. Use Python to append to `decisions.jsonl` instead.

11. **Write a report** to `{agent_root}/commons/data/ocas-custodian/reports/YYYY-MM-DD-HHMM.md` using `write_file`.

## Divergent branch handling

Topic branches may be divergent from HEAD (no merge-base). See `references/divergent-branch-handling.md` for detection and safe extraction procedure. Key rule: never `git merge` a divergent branch — extract content as manual patches instead.

## Version compatibility checks

| Check | What to look for | Action if found |
|---|---|---|
| Path references | `~/openclaw/`, `/tmp/openclaw/` instead of `{agent_root}/commons/` | Do NOT merge — incompatible |
| Command references | `openclaw cron`, `openclaw doctor` instead of `hermes cron`, `hermes doctor` | Do NOT merge — incompatible |
| scripts/custodian.py | Present in diff | **Assess content:** Old version (1708 lines, OpenClaw-era backup scripts) = reject. New stub (argparse-based, ~27 lines, no openclaw calls) = safe to keep. Check line count and grep for `openclaw`. |
| skill.json | Present in diff (was deliberately removed) | Reject this file from merge |
| OKR section removed | Missing from SKILL.md | Reject — we need OKRs |
| Initialization section removed | Missing from SKILL.md | Reject — we need init |
| Hermes execution patterns removed | Missing from SKILL.md | Reject — we need these |

## Current version state (plugin)

| Field | Value |
|---|---|
| Branch | `main` |
| Plugin version | `3.0.0` |
| Skill version | `3.0.0+hermes` |
| Last self-update | 2026-06-18 07:13 UTC |
| Upstream HEAD | `49496dd` (feat: cron-health) |
| Local HEAD | `769b709` (merge: local fixes on top of v3.0.0) |
| Known local patches | classifier.py schema guard, pyproject.toml build-backend, hook `**kwargs` defaults |

## Current version state (legacy skill copy)