# Running a hand-rolled log sweep BESIDE the sanctioned helper (2026-10-03)

## Symptom

A light scan ran the mandatory helper (`scripts/custodian_sweep.py`, self-test
`"ok": true`) AND, in parallel, a secondary log scan it hand-wrote for the
stale-job check. The secondary probe returned:

- **0** for every one of six fingerprints (`never claimed`, `already running`,
  `database is locked`, `timed out`, `IntegrityError`, `zero tool`)
- **1** in-window line out of 76 files scanned
- newest stamp **1 hour stale** against a live, busy gateway

The helper, same 75-minute window, returned `timeout=6 raw / 2 deduped` and
`unclaimed_removed=46 raw / 23 deduped`. The two disagreed by an order of
magnitude on a system that was demonstrably logging.

## Root cause

Not the bug class the reference library documents (that one compares a *UTC*
baseline to *local* stamps). This one is quieter:

```python
cut = now - timedelta(minutes=75)          # tz-AWARE: 2026-10-03T14:03+00:00
carry = datetime.fromisoformat(...).replace(tzinfo=now.tzinfo)   # 2026-10-03 08:xx-07:00
if carry < cut: continue
```

The cutoff carries `tzinfo`; the parsed log stamp had one bolted on from the
*process's* zone rather than from the log's own domain. Aware-vs-aware compares
silently and wrongly — no `TypeError` to warn you. Result: every line in the
real window sorted as pre-cutoff, so the in-window counter stayed at 0 while the
`newest stamp` line (computed outside the same filter) still printed a plausible
time. That last part is what makes it deceptive: **the newest-stamp field looks
like corroboration but is measured on a different path than the counter it is
supposed to validate.**

The generic symptom is the documented one — a uniform zero across unrelated
fingerprints — and the generic guard applies: *an exactly-uniform zero across
unrelated fingerprints is a filter bug until proven otherwise.*

## Why running both was the actual mistake

The helper exists precisely because five independent runs each hand-rolled this
and each got the clock domain wrong differently. Writing a *second* sweep in the
same run does not add a control — it adds a second unvalidated detector whose
output competes with the sanctioned one, and the reader cannot tell which is
which without re-deriving both.

The hand-rolled probe was justified internally as "I also need stale-job
candidates." It did not: that check reads `next_run_at` / `last_run_at` from
`jobs.json` and needs **no log sweep at all**. The log scan was decoration.

## Rule

**One sweep per run.** If another check appears to need log data, re-derive the
need from scratch before scanning — the usual answer is that it reads the
registry instead.

If a second log probe is genuinely required (e.g. a targeted grep for one named
signature to check issue coverage), then:

1. It is a **targeted grep**, not a census, and the journal must say so — it
   publishes no counts.
2. It asserts its own coverage: files opened > 0, read errors == 0, and
   **newest-stamp age < 1.0h computed on the same filtered path as the match**,
   not on a separate accumulator.
3. Every count it does publish sits **beside** the helper's anchored total for
   the same window. `0` with no anchored total beside it is a false clean.

## Sibling found the same day: resetting `carry` is NOT sufficient

A second probe this day passed rule 2's spirit — `carry = None` was declared
**inside** the per-file loop, as `references/measurement-pitfalls.md` line 40
demands — and still returned a wrong number, in the opposite direction:

| probe | in-window result |
|---|---|
| raw grep (single shared `carry`) | 11 traceback headers |
| collapse pass (`carry` per-file, correct) | **0** traceback events |

Two independent probes on the same 120-minute window, 11 versus 0. Rule 6
applies: name the file, grep the literal string, read the line. The newest
traceback header anywhere in the indexed set was `2026-10-03 05:27:01` — so
**0 in-window was correct** and the raw grep's 11 were the false ones.

**Root cause:** `carry` was correctly per-file, but the **accumulator that
*keys* the events** (`events = {}`) was declared *outside* the file loop, and
each un-stamped continuation line was attributed via `list(events.keys())[-1]`
— i.e. "whatever event was recorded last", which after a file boundary is an
event from a *different file*. The timestamp filter was sound; the event
attribution was not, so raw per-signature counts were cross-file contaminated
even though the collapsed count was right.

**Rule:** every piece of per-file state must be scoped to the file — the
timestamp AND the container that records what the timestamp was attributed to.
Resetting the clock while sharing the ledger reproduces the documented bug with
the guard already in place, which is worse than not guarding at all because it
reads as compliant.

Corollary for the same probe: a *stamped* `ERROR`/`CRITICAL` line sets `carry`
from itself and is therefore immune to this class. When a collapse pass and a
raw grep disagree, count the stamped level-marker lines separately and treat
that as the number you can publish — it is the only one of the three that has a
single, file-independent attribution path.

## Related

- `references/gateway-log-timestamp-range-filtering-pitfall.md` — the
  local-naive-stamps case and per-line extraction
- `references/measurement-pitfalls.md` line 40 — per-file `carry` reset + clock
  guard (the sibling this extends)
- `references/measurement-pitfalls.md` rule 6 — uniform zero is a filter bug
- mandatory-helper rule in the run prompt — anchored control beside every count