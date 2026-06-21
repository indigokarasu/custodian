# Empty Plugin Directory Detection

During cron scanning, check for empty plugin directories in `plugins/memory/` and `plugins/context_engine/`. Empty dirs silently break plugin discovery — the plugin vanishes from the available list with no error.

```bash
for d in /usr/local/lib/hermes-agent/plugins/memory/*/ /usr/local/lib/hermes-agent/plugins/context_engine/*/; do
  count=$(find "$d" -maxdepth 1 -name "*.py" -not -name "__pycache__" | wc -l)
  [ "$count" -eq 0 ] && echo "EMPTY PLUGIN DIR: $d"
done
```

This is a Tier 2 issue (requires investigation, not auto-fixed). See `references/chronicle-plugin-dirs-empty-pattern.md` for the specific Chronicle plugin case.
