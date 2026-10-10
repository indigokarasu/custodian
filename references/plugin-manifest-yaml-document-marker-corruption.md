# Plugin manifest corrupted by stray YAML document-end markers

**Fingerprint:** `oc_<plugin>_plugin_yaml_fix` (e.g. `oc_reflex_plugin_yaml_fix`).
Code-defect, Tier 1, fixable here.

## Symptom
`hermes_cli.plugins` logs, on EVERY plugin-discovery pass (roughly once a minute
per active session — measured ~60/min here):

```
WARNING hermes_cli.plugins: Failed to parse /root/.hermes/plugins/<name>/plugin.yaml:
  expected '<document start>', but found ('<block mapping start>',)
```

The plugin never loads, so its `provides_hooks` / `provides_tools` are silently
absent. Measured 2026-10-09: `reflex/plugin.yaml` produced 10,291 such lines across
2026-10-07..10-09 before the fix.

## Root cause shape
A manifest that is valid-looking to the eye but carries **standalone `...` lines**
(the YAML document-END marker) interleaved between mapping keys:

```yaml
name: reflex
...
version: 1.0.0
...
```

`...` terminates the document, so the next `key:` is a second document start and
ruamel/PyYAML raises `expected '<document start>'`. A second, independent defect can
coexist: **under-indented plain-scalar continuation lines** (a `description:` whose
continuation is indented only 2 spaces, so it reads as a new mapping key).

Corruption source here was a release-prep commit (`ebb055a`, 2026-10-04) that
reformatted the manifest; the markers persisted through every later commit.

## Detection
```bash
grep -c '^\.\.\.$' /root/.hermes/plugins/<name>/plugin.yaml   # >0 = corrupt
python3 -c "import yaml,sys; yaml.safe_load(open(sys.argv[1]))" <file>  # raises
```
Confirm with the REAL loader, not a bare yaml load:
```python
from hermes_cli.plugins_manifest import parse_manifest_file
m = parse_manifest_file(Path(p), Path(p).parent, "user", "")
# m is None on failure; a PluginManifest on success
```

## Fix
**First check upstream.** The local checkout may simply be behind — `git -C
<plugin_dir> fetch origin && git log HEAD..origin/main` may already show a
`fix(ci): repair malformed ... plugin.yaml` commit. Prefer adopting canonical
upstream over a divergent local repair:
```bash
git fetch origin
git merge origin/main --no-edit -X theirs   # then verify the worktree file == origin/main
git reset --mixed origin/main               # if you made a local commit; NOT reset --hard
```
`git reset --hard` is BLOCKED in cron (destroys uncommitted changes). Use
`git merge` / `git reset --mixed`, and keep a pre-fix byte copy under
`/root/.hermes/cache/scratch/`.

If no upstream fix exists, strip the `...` lines and fold under-indented
continuations into their key, then re-assert the semantic payload is unchanged
(compare `config_schema` keys/values, `provides_hooks`, `provides_tools` against the
last known-good commit — `git show <last_good>:plugin.yaml`).

## Verification (two stages, both required)
1. **Loader:** `parse_manifest_file` returns a PluginManifest for BOTH the root and
   the profile copy. The two paths are usually **hardlinked (same inode)** — verify
   with `ls -i`; a single write fixes both.
2. **Live:** count the warning over a real discovery window. Pre-fix ~60/min; after
   the fix assert ZERO over ≥5 min AND that plugin-discovery lines are still being
   emitted (so the silence is a fix, not a stalled log).

## False-completion trap (why this file exists)
A prior deep scan journalled `oc_reflex_plugin_yaml_fix` with evidence "Removed
erroneous '...' lines ... Verified plugin now loads without warning" — but the file
was **byte-identical to its `.bak`** and warnings kept firing for hours. A claimed
fix with no post-fix signature recheck is a claim, not a resolution. Always
re-grep the original signature AFTER the fix and confirm it is gone; compare the
file's md5/mtime against the pre-fix state before believing a fix landed.
