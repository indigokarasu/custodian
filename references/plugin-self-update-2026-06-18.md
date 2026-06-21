# Plugin Self-Update Procedure (custodian.update)

**For the plugin directory** (`~/.hermes/plugins/custodian/`), not the skill directory.

## Update steps

1. **Check for local modifications first:**
   ```bash
   cd ~/.hermes/plugins/custodian && git status
   ```
   If clean, skip to step 4.

2. **Stash local changes** (plugin dir has local fixes not in upstream — classifier.py schema guards, pyproject.toml build-backend fix, hook signature defaults):
   ```bash
   git stash push -m "local fixes: classifier schema guard, pyproject build-backend, hook defaults"
   ```

3. **Pull upstream:**
   ```bash
   git pull
   ```

4. **If stash exists, pop it:**
   ```bash
   git stash pop
   ```

5. **⚠️ If `git stash pop` produces merge conflicts** (expected when upstream also modified the same files):
   - **Do NOT use `git checkout --theirs` or `--ours` blindly** — this discards one side entirely and silently loses changes
   - Instead, manually edit conflicted files to merge both sides
   - For `__init__.py`: upstream may have already incorporated some changes (e.g., `**kwargs` on hooks). Keep upstream's version, then verify local additions are present
   - For `classifier.py`: upstream likely doesn't have the schema contamination guard — keep the stashed version's additions
   - For `pyproject.toml`: keep the stashed `setuptools.build_meta` fix
   - After resolving: `git add <file>` for each resolved file
   - **Critical:** Check `__version__` in `__init__.py` — conflict resolution often silently keeps the OLD version string. Verify with `grep __version__ hermes_custodian_plugin/__init__.py` and fix if needed:
     ```bash
     sed -i 's/__version__ = "2.0.0"/__version__ = "3.0.0"/' hermes_custodian_plugin/__init__.py
     ```
   - Verify with `python3 -c "import hermes_custodian_plugin; print(hermes_custodian_plugin.__version__)"` (clear `__pycache__` first if needed)
   - Commit: `git commit -m "merge: apply local fixes on top of upstream vX.Y.Z"`
   - Only then: `git stash drop` (if stash still exists — `git stash pop` auto-drops on clean apply, but NOT on conflicts)

6. **Clear stale `.pyc` caches** after update:
   ```bash
   find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null
   ```

7. **Verify the plugin loads:**
   ```bash
   python3 -c "import hermes_custodian_plugin; print('Version:', hermes_custodian_plugin.__version__)"
   ```

## Known local patches (not in upstream as of 2026-06-18)

| File | Local fix | Why needed |
|------|-----------|------------|
| `hermes_custodian_plugin/classifier.py` | Schema contamination guard in `_load()` + `.get("attempts", 0)` in `should_escalate()` | Prevents `KeyError: 'attempts'` crash when raw fix log entries mix into `fix_effectiveness.jsonl` |
| `pyproject.toml` | `build-backend = "setuptools.build_meta"` | Upstream has `setuptools.backends._legacy:_Backend` which may not work in all environments |

## Version state (2026-06-18)

| Field | Value |
|---|---|
| Branch | `main` |
| Plugin version | `3.0.0` |
| Skill version | `3.0.0+hermes` |
| Last self-update | 2026-06-18 07:13 UTC |
| Upstream HEAD | `49496dd` (feat: cron-health) |
| Local HEAD | `769b709` (merge: local fixes on top of v3.0.0) |
