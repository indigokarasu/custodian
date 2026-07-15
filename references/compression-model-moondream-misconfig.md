# Compression-model misconfiguration (vision model as text compressor)

## Symptom
Gateway log shows, repeatedly, in live agent sessions (e.g. `agent:main:telegram:dm:8`):
```
ValueError: Auxiliary compression model moondream has a context window of 2,048 tokens,
which is below the minimum 64,000 required by Hermes Agent. Choose a co[mpatible model].
```

## Root cause — TWO distinct `compression:` blocks
`config.yaml` has **two** compression config blocks that a naive `grep compression` collides on:

1. **Top-level `compression:`** — the agent's auto/context compression feature.
2. **`auxiliary.compression:`** — a separate auxiliary-client block (often with `provider: nous`, `model: tencent/hy3:free`).

The error is thrown by the **top-level** block reading `compression.model: moondream`.
`moondream` is a *vision* model served via Ollama (`http://localhost:11434/v1`) with only a
2,048-token context — unusable as a text compressor (minimum 64,000). The error text says
"Auxiliary compression model", which makes it look like the `auxiliary.compression:` block, but
that block's `model` is typically correct. **Read the TOP-LEVEL `compression:` block, not
`auxiliary.compression:`.**

## Diagnostic (execute_code is blocked in cron — use terminal + /tmp script)
```python
import os, re
for cfg in ["<hermes-home>/config.yaml", "<hermes-root>/config.yaml"]:
    if not os.path.exists(cfg): continue
    txt = open(cfg).read()
    for ln in txt.splitlines():
        if "moondream" in ln.lower():
            print(cfg, "->", ln.strip())
    # print the TOP-LEVEL compression block (anchored at column 0)
    m = re.search(r"^compression:.*?(?=^\S|\Z)", txt, re.S | re.M)
    if m:
        for ln in m.group(0).splitlines()[:8]:
            print("  TOP>", ln)
```

## Active vs stale — verify BEFORE flagging (do not auto-edit)
This misconfig is frequently **stale**: the running gateway may already use the valid
`auxiliary.compression.model`, and the top-level value is a lingering bad setting. Before
writing an issue or editing `config.yaml`:

1. Find the **last timestamp** of that error signature in the log; compare to the log's last line.
2. Look for a subsequent **successful** operation of the same feature — e.g.
   `Session split detected: … → … (compression)` means compression now works.

```python
import re
LOG = "<hermes-home>/logs/gateway.log"
lines = open(LOG, errors="replace").readlines()
last_ts = None; last_err = None
for ln in lines:
    mm = re.match(r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})", ln)
    if mm: last_ts = mm.group(1)
    if "moondream" in ln.lower() and "context window" in ln.lower():
        last_err = last_ts
print("last moondream err:", last_err, "| log tail:", last_ts)
```
If `last_err` is hours before `log tail` AND a newer `(compression)` success line exists →
**STALE**: note only, do not auto-edit `config.yaml` (guessing a model name risks another
deprecated/broken value; compression is also commonly already degraded by an unrelated billing
401). Flag for user review.

## Related
- `oc_auxiliary_compression_exhaustion` (Tier 2) — OpenRouter credential-pool exhaustion, different root cause.
- `oc_http_404_model_deprecated` — model removed from provider.
- `references/stale-model-error-diagnostic-pattern.md` — distinguishes stale vs active 404 model errors.
