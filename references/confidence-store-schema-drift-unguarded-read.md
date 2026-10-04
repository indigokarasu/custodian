# Confidence-store schema drift crashes the status tool (2026-10-03)

## Symptom

`ERROR tools.registry: Tool custodian_status dispatch error: 'attempts'` — a bare
KeyError with no file, no line, no record identity. Same crash from
`/custodian confidence show`. Both are total failures of those two surfaces,
not degraded output.

## Root cause: an unguarded read beside a guarded one

`hermes_custodian_plugin/classifier.py`:

```python
if rec["attempts"] >= 3 and rec.get("success_rate", 1.0) < 0.2:
```

`success_rate` is fetched with `.get()` **on the same line**; `attempts` is
indexed directly. That asymmetry is the tell — a `.get()` two characters away
from a `[...]` is code where the author already knew the record might be
partial and guarded one field and not the other. Three sites, same defect:
lines 169, 197, 199.

16 of 18 records in `fix_effectiveness.jsonl` had no `attempts` key. They were
written by *scan authors* appending to the store by hand, not by
`ConfidenceModel.record_outcome`, so they never carried the model's schema.
`get_summary()` calls `should_escalate()` for **every** record, so one
malformed row anywhere crashes the whole tool — blast radius is the entire
tier-gating input, not one row.

## Why it stayed hidden for weeks

The first occurrence in the indexed set was 2026-10-02 20:04 local, in
`agent.log.1`. It was invisible to the sanctioned sweep because
`custodian_sweep.py` fingerprints only `timeout`, `unclaimed_removed` and
`database_is_locked` — a plugin dispatch error matches none of them, so it can
only surface through the Step 2 traceback/ERROR-line check. That check is a
*second* probe, and its own class of defect is documented elsewhere; a scan that
skips it sees a clean sweep and a live crash simultaneously.

## Fix, split by envelope

| half | where | disposition |
|---|---|---|
| the data | `commons/data/ocas-custodian/fix_effectiveness.jsonl` | Tier 1 — autonomous |
| the unguarded read | `plugins/custodian/.../classifier.py` | `user_gated` — plugin code, needs gateway restart |

The data repair normalised **all 16** records in one pass (the class, not the
member) and derived `attempts` from the historical `prior_attempts` alias where
present, so recorded history was not silently reset to zero. Backup written and
asserted byte-identical to the pre-fix file first.

Filing only the code half would have been wrong in the other direction: the
tool is broken *now*, and the data half is inside the envelope.

## Rule

**A `.get()` adjacent to a `[...]` in the same expression is a defect report
about the `[...]`.** Sweep for the asymmetry rather than for the crash.

**Writers of a machine-read store own the reader's schema.** Any code path that
appends to `fix_effectiveness.jsonl` without `attempts`/`successes`/`failures`
re-arms this crash, so the file patch is a mitigation, not a resolution — which
is why the issue is filed open rather than resolved, with the residual stated
explicitly in the row.

## Verification pattern used (reusable)

Three arms, because a bare pass proves nothing when the control path can pass
trivially:

1. **Inverted** — unpatched module against a *copy* of the real store: must
   raise.
2. **Repaired** — patched copy against the same copy: must return a summary.
3. **Attribution** — *unpatched* module against the same records with the
   counters synthesised: must pass. If the unpatched code passes once the data
   is fixed, the crash is the record shape, not the probe.

Arm 3 is what distinguishes "the patch is right" from "the fixture was wrong."
Fixture work is throwaway (`cache/scratch/`), never the live store.

## Related

- `references/fix-safety.md` — plugin code outside the autonomous envelope
- `references/measurement-pitfalls.md` rule 6 — uniform zero / grep-the-literal
- `references/parallel-handrolled-log-sweep-false-clean-20261003.md` — why the
  ERROR-line probe is itself error-prone, and why the signature was reachable
